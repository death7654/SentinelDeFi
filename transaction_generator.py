import json
import random
import time
from datetime import datetime, timezone
import numpy as np
from kafka import KafkaProducer

# Initialize Kafka Producer pointing to localhost:9092
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Simulated active wallet addresses
WALLETS = [
    f"0x{i:040x}" for i in range(1, 11)
]

print("DeFi Transaction Generator Started. Press Ctrl+C to stop.\n")

try:
    while True:
        # 1. Pick a wallet address (cast numpy string to standard str)
        wallet = str(np.random.choice(WALLETS))
        
        # 2. Simulate standard vs anomalous amounts (1% chance of Flash-Loan anomaly)
        # Use np.random.random() instead of np.random()
        is_anomaly = bool(np.random.random() < 0.01)
        
        if is_anomaly:
            # Cast np.float64 to native float for JSON serialization
            amount = float(round(np.random.uniform(1_000_000, 10_000_000), 2))
            gas_fee = float(round(np.random.uniform(1.5, 5.0), 4))
        else:
            amount = float(round(np.random.exponential(scale=250.0), 2))
            gas_fee = float(round(np.random.uniform(0.001, 0.05), 4))

        # 3. Construct JSON Payload (use standard random.getrandbits for 256-bit tx hashes)
        payload = {
            "tx_id": f"0x{random.getrandbits(256):064x}",
            "wallet_address": wallet,
            "amount_usd": max(1.0, amount),
            "gas_fee": gas_fee,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

        # 4. Push event to Kafka
        producer.send('defi-transactions', value=payload)
        print(f"Sent: {payload['wallet_address'][:10]}... | ${payload['amount_usd']:,.2f} | Anomaly: {is_anomaly}")
        
        # Pace the stream (10 transactions per second)
        time.sleep(0.1)

except KeyboardInterrupt:
    print("\nStopping generator...")
    producer.flush()
    producer.close()