""" enrich2.py

Phase 2 enrichment that computes funneling and smurfing/structuring features
directly on the existing `:Transaction` nodes for a single scoring day.

- Accepts a single argument in YYYYMMDD (no dashes). Example: `20250401`
- Uses `tx.amount` for all calculations.
- Updates are direct MATCH + SET on matched transactions (same style as enrich phase 1).
- Idempotent per day: re-running simply overwrites the same properties.

It writes/overwrites the following properties on each `:Transaction` in the day:
    tx.funneling_score_v2                :: float in [0, 1]
    tx.funneling_multiple_senders_v2     :: integer (distinct senders into the recipient that day)
    tx.smurfing_score_v2                 :: float in [0, 1]
    tx.smurfing_small_tx_count_v2        :: integer (count of "small" outgoing txs from the sender that day)
    tx.enriched_v2_at                    :: epoch seconds of when enrichment ran

Why these features?
- Funneling (a.k.a. "funnelling"): many distinct senders push funds into a SINGLE receiver over a short window.
  This often indicates cash-in consolidation before onward movement (layering). We measure the *breadth of
  sources* (distinct senders) and reward cases where the per-sender average amount is small (typical of deposits
  split to stay under reporting thresholds).
- Smurfing/Structuring: one sender splits a larger amount into many *small* transfers to multiple recipients over
  a short period to avoid detection thresholds. We measure the number of small outgoing transactions and the spread
  across distinct recipients, and reward patterns with many small pieces.

How are scores built?
- Both scores are intentionally unitless and normalized to [0, 1] within the day to be comparable across days.
- For funneling, the core signal is the number of distinct senders into a recipient that day, penalized when the
  average contribution per sender is large. Intuition: "many tiny deposits" is more suspicious than "many big deposits".
- For smurfing, the core signal is how many small outgoing transactions a sender makes that day (vs. the total),
  amplified by how many distinct recipients the sender touches and penalized when the average piece size is large.

IMPORTANT implementation notes:
- We do all aggregation in the database (Cypher) and only compute light normalizations in Python.
- We only use "tx.amount" for monetary values. If an amount is missing, it is treated as 0.0.
- We keep write batches reasonably small to reduce heap pressure on Neo4j.

Schema expectations (same as phase 1):
  (:Wallet {address})-[:SENT]->(:Transaction {transaction_id, timestamp, amount})-[:RECEIVED_BY]->(:Wallet {address})

"""

import logging
import math
import sys
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Dict, List

from neo4j import GraphDatabase, Session

# ======== CONFIG ========
NEO4J_URI = "bolt://192.168.0.5:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "PotatoDTND12!"

# A "small" transaction upper bound (domain knob).
# Many jurisdictions use thresholds like 10k (or equivalent).
SMALL_TX_MAX = 10_000.0

# Write batch size for UNWIND writes.
WRITE_BATCH = 1000

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


# ======== HELPERS ========

def parse_yyyymmdd(arg: str) -> datetime:
    """Strictly parse YYYYMMDD and return a UTC-aware datetime at 00:00:00."""
    if len(arg) != 8 or not arg.isdigit():
        raise ValueError(
            f"Expected date as YYYYMMDD (no dashes). Got: {arg!r}"
        )
    dt = datetime.strptime(arg, "%Y%m%d").replace(tzinfo=timezone.utc)
    return dt


@dataclass
class FunnelingGroup:
    recipient: str
    sender_count: int
    total_amount: float
    tx_ids: List[str]


@dataclass
class SmurfingGroup:
    sender: str
    small_tx_count: int
    total_tx: int
    recipient_count: int
    tx_ids: List[str]


# ======== CYTHER READ QUERIES ========

FUNNELING_Q = """
// Aggregate per-recipient for the day.
// We only read what we need and compute large reductions in-db.
MATCH (from:Wallet)-[:SENT]->(tx:Transaction)-[:RECEIVED_BY]->(to:Wallet)
WHERE tx.timestamp >= $start_ts AND tx.timestamp < $end_ts
WITH to.address AS recipient,
     collect(tx) AS txs,
     count(DISTINCT from.address) AS sender_count,
     sum(coalesce(tx.amount, 0.0)) AS total_amount
RETURN recipient,
       sender_count,
       total_amount,
       [t IN txs | t.transaction_id] AS tx_ids
"""

SMURFING_Q = """
// Aggregate per-sender for the day, preserving per-tx amount to count "small" pieces.
MATCH (from:Wallet)-[:SENT]->(tx:Transaction)-[:RECEIVED_BY]->(to:Wallet)
WHERE tx.timestamp >= $start_ts AND tx.timestamp < $end_ts
WITH from.address AS sender,
     collect({txid: tx.transaction_id, amount: coalesce(tx.amount, 0.0)}) AS txs,
     count(DISTINCT to.address) AS recipient_count
RETURN sender,
       size([t IN txs WHERE t.amount <= $small_max]) AS small_tx_count,
       size(txs) AS total_tx,
       recipient_count,
       [t IN txs | t.txid] AS tx_ids
"""


def read_funneling_groups(session: Session, start_ts: int, end_ts: int) -> List[FunnelingGroup]:
    rows = session.run(FUNNELING_Q, start_ts=start_ts, end_ts=end_ts)
    groups: List[FunnelingGroup] = []
    for r in rows:
        groups.append(
            FunnelingGroup(
                recipient=r["recipient"],
                sender_count=int(r["sender_count"] or 0),
                total_amount=float(r["total_amount"] or 0.0),
                tx_ids=list(r["tx_ids"] or []),
            )
        )
    return groups


def read_smurfing_groups(session: Session, start_ts: int, end_ts: int) -> List[SmurfingGroup]:
    rows = session.run(SMURFING_Q, start_ts=start_ts, end_ts=end_ts, small_max=SMALL_TX_MAX)
    groups: List[SmurfingGroup] = []
    for r in rows:
        groups.append(
            SmurfingGroup(
                sender=r["sender"],
                small_tx_count=int(r["small_tx_count"] or 0),
                total_tx=int(r["total_tx"] or 0),
                recipient_count=int(r["recipient_count"] or 0),
                tx_ids=list(r["tx_ids"] or []),
            )
        )
    return groups


# ======== SCORE BUILDERS (WHY / WHAT / WHERE / HOW) ========

def build_funneling_scores(groups: List[FunnelingGroup]) -> Dict[str, Dict]:
    """
    WHY:
      Funneling is most evident when a *single recipient* consolidates deposits from *many distinct senders*.
      We want to flag recipients with high sender cardinality *and* prefer patterns composed of smaller contributions.

    WHAT:
      For each recipient group we compute:
        - distinct sender count
        - average contribution per sender = total_amount / max(1, sender_count)
      Then produce a recipient-level funneling score in [0,1] that increases with sender_count
      and decreases as the average amount grows beyond SMALL_TX_MAX.

    WHERE:
      The score is then written on each Transaction that credits the recipient that day, so later per-tx models
      have direct access to the "recipient context" of funneling risk.

    HOW:
      - Normalize sender_count by the max sender_count observed that day.
      - Compute a "tiny-amount factor" = 1 / (1 + avg_amount / SMALL_TX_MAX). This is near 1 for avg <= threshold
        and decays smoothly for larger values.
      - Score = normalized_sender_count * tiny_amount_factor
    """
    if not groups:
        return {}

    max_senders = max((g.sender_count for g in groups), default=1)
    out: Dict[str, Dict] = {}

    for g in groups:
        if g.sender_count <= 0:
            score = 0.0
            avg_amount = 0.0
        else:
            normalized_sender = (g.sender_count / max_senders) if max_senders > 0 else 0.0
            avg_amount = g.total_amount / max(1, g.sender_count)
            tiny_amount_factor = 1.0 / (1.0 + (avg_amount / SMALL_TX_MAX))
            score = max(0.0, min(1.0, normalized_sender * tiny_amount_factor))

        for txid in g.tx_ids:
            out[txid] = {
                "tx_id": txid,
                "funneling_score_v2": float(score),
                "funneling_multiple_senders_v2": int(g.sender_count),
            }

    return out


def build_smurfing_scores(groups: List[SmurfingGroup]) -> Dict[str, Dict]:
    """
    WHY:
      Smurfing (structuring) happens when a *sender* breaks a larger sum into *many small* transfers across possibly
      many recipients within a short period.

    WHAT:
      For each sender group we compute:
        - how many "small" outgoing txs they produced that day (<= SMALL_TX_MAX)
        - how many total txs (to normalize)
        - how many distinct recipients (wider spread is more suspicious)

    WHERE:
      The score is written on each Transaction sent by that sender that day, so every per-tx record carries the
      "sender-side" smurfing risk signal.

    HOW:
      - normalized_small = small_tx_count / max_small_count_over_day
      - normalized_recipients = recipient_count / max_recipient_count_over_day
      - density = small_tx_count / max(1, total_tx)  (share of small pieces)
      - score_raw = normalized_small * 0.6 + normalized_recipients * 0.4   (blend to balance volume vs. spread)
      - score = score_raw * density
      Finally clamp to [0,1].
    """
    if not groups:
        return {}

    max_small = max((g.small_tx_count for g in groups), default=1)
    max_recips = max((g.recipient_count for g in groups), default=1)

    out: Dict[str, Dict] = {}
    for g in groups:
        normalized_small = (g.small_tx_count / max_small) if max_small > 0 else 0.0
        normalized_recipients = (g.recipient_count / max_recips) if max_recips > 0 else 0.0
        density = (g.small_tx_count / max(1, g.total_tx)) if g.total_tx > 0 else 0.0
        score_raw = (normalized_small * 0.6) + (normalized_recipients * 0.4)
        score = max(0.0, min(1.0, score_raw * density))

        for txid in g.tx_ids:
            out[txid] = {
                "tx_id": txid,
                "smurfing_score_v2": float(score),
                "smurfing_small_tx_count_v2": int(g.small_tx_count),
            }

    return out


# ======== WRITE QUERIES (DIRECT UPDATES) ========

WRITE_FUNNELING = """
UNWIND $rows AS row
MATCH (tx:Transaction {transaction_id: row.tx_id})
SET tx.funneling_score_v2 = row.funneling_score_v2,
    tx.funneling_multiple_senders_v2 = row.funneling_multiple_senders_v2,
    tx.enriched_v2_at = $now_ts
"""

WRITE_SMURFING = """
UNWIND $rows AS row
MATCH (tx:Transaction {transaction_id: row.tx_id})
SET tx.smurfing_score_v2 = row.smurfing_score_v2,
    tx.smurfing_small_tx_count_v2 = row.smurfing_small_tx_count_v2,
    tx.enriched_v2_at = $now_ts
"""


def write_rows(session: Session, query: str, rows: List[Dict], now_ts: int):
    """Chunked UNWIND updates to avoid large transactions."""
    total = len(rows)
    if total == 0:
        return

    for i in range(0, total, WRITE_BATCH):
        chunk = rows[i : i + WRITE_BATCH]
        session.run(query, rows=chunk, now_ts=now_ts)
        logging.info("    wrote %d/%d rows", min(i + WRITE_BATCH, total), total)


# ======== MAIN ========

def run():
    if len(sys.argv) != 2:
        print("Usage: python3 enrich2.py YYYYMMDD", file=sys.stderr)
        sys.exit(2)

    day = parse_yyyymmdd(sys.argv[1])
    day_end = day + timedelta(days=1)
    start_ts = int(day.timestamp())
    end_ts = int(day_end.timestamp())
    now_ts = int(datetime.now(tz=timezone.utc).timestamp())

    logging.info("🔎 Phase2 enrichment for day %s (epoch [%d, %d))", day.strftime("%Y-%m-%d"), start_ts, end_ts)

    with driver.session() as session:
        # Read groups
        funnel_groups = read_funneling_groups(session, start_ts, end_ts)
        smurf_groups = read_smurfing_groups(session, start_ts, end_ts)

        logging.info("  found %d recipient groups (funneling)", len(funnel_groups))
        logging.info("  found %d sender groups (smurfing)", len(smurf_groups))

        # Build per-tx rows
        funnel_map = build_funneling_scores(funnel_groups)
        smurf_map = build_smurfing_scores(smurf_groups)

        # Merge the two maps at the tx level so that a single tx can receive both updates in either order.
        merged: Dict[str, Dict] = {}

        for txid, v in funnel_map.items():
            merged.setdefault(txid, {"tx_id": txid})
            merged[txid].update(v)

        for txid, v in smurf_map.items():
            merged.setdefault(txid, {"tx_id": txid})
            merged[txid].update(v)

        # Partition into two write payloads to keep queries simple & indexes effective
        funnel_rows = [v for v in merged.values() if "funneling_score_v2" in v]
        smurf_rows = [v for v in merged.values() if "smurfing_score_v2" in v]

        logging.info("  preparing to write: %d txs with funneling, %d txs with smurfing", len(funnel_rows), len(smurf_rows))

        # Writes (DIRECT MATCH + SET)
        if funnel_rows:
            logging.info("🧪 writing funneling features...")
            write_rows(session, WRITE_FUNNELING, funnel_rows, now_ts)

        if smurf_rows:
            logging.info("🧪 writing smurfing features...")
            write_rows(session, WRITE_SMURFING, smurf_rows, now_ts)

        logging.info("✅ enrich2 finished; enriched %d transactions", len(merged))


if __name__ == "__main__":
    run()
