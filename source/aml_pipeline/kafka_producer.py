import json
import requests
from kafka import KafkaProducer
from time import sleep
import os

# Configuration
BESU_RPC_URL = "http://192.168.0.3:8545"
KAFKA_BOOTSTRAP_SERVERS = "192.168.0.4:9092"
KAFKA_TOPIC = "usdc-transactions"
STATE_FILE = "last_block.txt" # change to start retrieving newer transactions (last safe block from etherscan.io)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def get_block_timestamp(block_number):
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_getBlockByNumber",
        "params": [block_number, False],
        "id": 1
    }
    try:
        response = requests.post(BESU_RPC_URL, json=payload, timeout=10)
        result = response.json().get("result", {})
        return int(result["timestamp"], 16) if "timestamp" in result else None
    except Exception as e:
        print(f"⚠️ Failed to get block timestamp: {e}")
        return None

def get_latest_block():
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_blockNumber",
        "params": [],
        "id": 1
    }
    try:
        response = requests.post(BESU_RPC_URL, json=payload, timeout=10)
        return int(response.json()["result"], 16)
    except Exception as e:
        print(f"⚠️ Failed to fetch latest block: {e}")
        return None

def fetch_usdc_transactions(start_block, end_block):
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_getLogs",
        "params": [{
            "fromBlock": hex(start_block),
            "toBlock": hex(end_block),
            "address": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48" 
            #Ethereum smart contract address for USDC (USD Coin) on the Ethereum mainnet
        }],
        "id": 1
    }
    try:
        response = requests.post(BESU_RPC_URL, json=payload, timeout=15)
        logs = response.json().get("result", [])
    except Exception as e:
        print(f"⚠️ Error fetching logs: {e}")
        return []

    transactions = []
    for log in logs:
        try:
            topics = log.get("topics", [])
            #if len(topics) < 3 or not log.get("data") or log["data"] == "0x":
            #    continue
            # Only monitor USDC transfer events and not other contract interactions
            if (
                len(topics) < 3 or 
                not log.get("data") or 
                log["data"] == "0x" or 
                topics[0].lower() != "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef" # ERC-20 Transfer event signature
            ):
                continue

            from_address = "0x" + topics[1][-40:]
            to_address = "0x" + topics[2][-40:]
            amount = int(log["data"], 16) / 10**6
            timestamp = get_block_timestamp(log["blockNumber"])
            if timestamp is None:
                continue

            transactions.append({
                "transaction_id": log["transactionHash"],
                "from_address": from_address,
                "to_address": to_address,
                "amount": amount,
                "timestamp": timestamp,
                "block_number": log["blockNumber"]
            })

        except Exception as e:
            print(f"⚠️ Error decoding log entry: {e}")
            continue

    return transactions

def load_last_processed_block():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return int(f.read().strip(), 16)
    else:
        latest = get_latest_block()
        return latest - (30 * 7200) if latest else 0

def save_last_processed_block(block):
    with open(STATE_FILE, "w") as f:
        f.write(hex(block))

# Main Loop
batch_size = 500
current_block = load_last_processed_block()

# Deduplication cache to delete repeated transactions in case of RPC retries or reprocessing of overlapping blocks
recent_tx_hashes = set()
MAX_CACHE_SIZE = 500000  # Adjust based on memory needs

while True:
    latest_block = get_latest_block()
    if latest_block is None or latest_block <= current_block:
        print("🟡 Waiting for new blocks...")
        sleep(10)
        continue

    for block in range(current_block, latest_block + 1, batch_size):
        end_block = min(block + batch_size - 1, latest_block)
        print(f"🔍 Processing block range: {block} to {end_block}")
        txs = fetch_usdc_transactions(block, end_block)
        
        for tx in txs:
            if tx["transaction_id"] in recent_tx_hashes:
                continue  # Skip duplicates

            producer.send(KAFKA_TOPIC,
                          key=tx["transaction_id"].encode("utf-8"), # keying to help deduplication and partition
                          value=tx) 
            
            recent_tx_hashes.add(tx["transaction_id"]) # add to cache
            if len(recent_tx_hashes) > MAX_CACHE_SIZE: # evict old hashes to prevent memory bloat
                recent_tx_hashes.pop()
                
            print(f"✅ Sent: {tx['transaction_id']}")

        current_block = end_block + 1
        save_last_processed_block(current_block)

    sleep(10)
