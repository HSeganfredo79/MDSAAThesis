#!/bin/bash
# Prefect 3.x Deployment Registration Script with Cron Schedules

: <<'GANTT'
```mermaid
gantt
    title Pipeline Timeline (UTC)

    section Enrichment
    Enrich Flow (D-1)          :done, enrich, 21:00, 1h

    section Scoring
    Score Flow (D-2)           :active, score, 07:00, 1h

    section Drift Monitoring
    Drift Monitor              :drift, drift, 22:00, 1h

    section Training (Weekly)
    Weekly Training (Sun)      :train, train, 23:00, 1h

    section Kafka (Daemon Processes)
    Kafka Producer (manual)    :manual, kafka1, 00:00, 24h
    Kafka Consumer (manual)    :manual, kafka2, 00:00, 24h
    Kafka Streaming Loop (autorun) :active, kafka-loop, 00:00, 24h
```
GANTT

# Dynamic dates
ENRICH_DATE=$(date -d "yesterday" +%Y%m%d)
SCORE_DATE=$(date -d "2 days ago" +%Y%m%d)

# Enrichment flow (runs daily at 9 PM with D-1 txs)
prefect deploy flows/enrich_flow.py:enrich_flow \
  --name enrich-daily \
  --params "{\"date\": \"$ENRICH_DATE\"}" \
  --pool default-agent-pool \
  --cron "0 21 * * *"

# Scoring flow (runs daily at 7 AM with D-2 enriched txs)
prefect deploy flows/score_flow.py:score_flow \
  --name score-daily \
  --params "{\"date\": \"$SCORE_DATE\"}" \
  --pool default-agent-pool \
  --cron "0 7 * * *"

# Weekly training (runs Sunday 8 PM, looks back 7 days for enriched txs)
prefect deploy flows/train_flow.py:train_flow \
  --name train-weekly \
  --pool default-agent-pool \
  --cron "0 23 * * 0"

# Drift monitor (runs daily at 10 PM with enriched REFERENCE data from the last 7 days)
# Fresh data (CURRENT) is the latest enriched 24h tx block
prefect deploy flows/monitor_flow.py:monitor_flow \
  --name drift-monitor \
  --pool default-agent-pool \
  --cron "0 22 * * *"

# Kafka components (manual, long-running — do NOT schedule)
prefect deploy flows/producer_flow.py:producer_flow \
  --name kafka-producer \
  --pool default-agent-pool \

prefect deploy flows/consumer_flow.py:consumer_flow \
  --name kafka-consumer \
  --pool default-agent-pool \

prefect deploy flows/kafka_streaming.py:kafka_streaming_flow \
  --name kafka-loop \
  --pool default-agent-pool \
