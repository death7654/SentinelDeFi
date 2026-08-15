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
WALLETS = [f"0x{i:040x}" for i in range(1, 11)]

# A tight ring of wallets used to simulate circular wash trading —
# funds get passed around this small set instead of a random one so
# the CEP layer's per-wallet windowing actually sees the pattern.
WASH_TRADE_RING = WALLETS[:4]


def make_payload(wallet, amount, gas_fee):
    """Builds a single transaction payload matching the Spark schema
    in streaming_engine.py. Extra fields are NOT added here on purpose —
    from_json() with an explicit StructType would just drop them, so
    keeping the shape exact avoids confusion downstream."""
    return {
        "tx_id": f"0x{random.getrandbits(256):064x}",
        "wallet_address": wallet,
        "amount_usd": max(1.0, float(amount)),
        "gas_fee": float(gas_fee),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def send(payload, tag):
    producer.send('defi-transactions', value=payload)
    print(f"[{tag}] {payload['wallet_address'][:10]}... | ${payload['amount_usd']:,.2f}")


def emit_normal():
    wallet = str(np.random.choice(WALLETS))
    amount = float(round(np.random.exponential(scale=250.0), 2))
    gas_fee = float(round(np.random.uniform(0.001, 0.05), 4))
    send(make_payload(wallet, amount, gas_fee), "normal")
    time.sleep(0.1)


def emit_flash_loan_anomaly():
    """Single oversized transaction — the case already in the original script."""
    wallet = str(np.random.choice(WALLETS))
    amount = float(round(np.random.uniform(1_000_000, 10_000_000), 2))
    gas_fee = float(round(np.random.uniform(1.5, 5.0), 4))
    send(make_payload(wallet, amount, gas_fee), "flash-loan")
    time.sleep(0.1)


def emit_wash_trading_anomaly():
    """Circular wash trading: the same amount gets passed hop-to-hop
    around a small ring of wallets in quick succession, inflating
    volume without real economic activity. This is what the CEP
    layer's velocity check (C_w > 8 per sliding window, per the
    proposal) is meant to catch."""
    hops = random.randint(4, 7)
    amount = float(round(np.random.uniform(5_000, 50_000), 2))
    for i in range(hops):
        wallet = WASH_TRADE_RING[i % len(WASH_TRADE_RING)]
        gas_fee = float(round(np.random.uniform(0.001, 0.01), 4))
        send(make_payload(wallet, amount, gas_fee), "wash-trade")
        time.sleep(0.02)


def emit_bot_burst_anomaly():
    """Frontrunning bot burst: one wallet fires a rapid run of small,
    near-identical transactions with elevated gas fees (bots overpay
    gas to jump the queue), well past the C_w > 8 velocity threshold
    defined in the proposal."""
    wallet = str(np.random.choice(WALLETS))
    burst_size = random.randint(9, 15)
    for _ in range(burst_size):
        amount = float(round(np.random.uniform(10, 100), 2))
        gas_fee = float(round(np.random.uniform(0.05, 0.2), 4))
        send(make_payload(wallet, amount, gas_fee), "bot-burst")
        time.sleep(0.01)


print("DeFi Transaction Generator Started. Press Ctrl+C to stop.\n")

try:
    while True:
        # 1% flash-loan, 0.5% wash-trading, 0.5% bot-burst, rest normal
        r = np.random.random()
        if r < 0.01:
            emit_flash_loan_anomaly()
        elif r < 0.015:
            emit_wash_trading_anomaly()
        elif r < 0.02:
            emit_bot_burst_anomaly()
        else:
            emit_normal()

except KeyboardInterrupt:
    print("\nStopping generator...")
    producer.flush()
    producer.close()