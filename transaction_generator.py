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

# Simulated DeFi protocol contracts (DEX pools, lending pools). Normal
# traffic mostly settles here rather than wallet-to-wallet, which is what
# real DeFi activity looks like and gives the graph two distinct node
# "shapes" (wallets: many small in/out edges; contracts: high in-degree
# hubs) — useful context for PageRank/community detection later.
CONTRACTS = [f"0xC0NTRACT{i:032x}" for i in range(1, 4)]

# A tight ring of wallets used to simulate circular wash trading. Unlike
# the original generator (which only ever set a single `wallet_address`
# per hop with no counterparty), this now actually threads funds
# wallet -> wallet -> wallet -> ... -> back to the start, so the graph
# contains a real directed cycle for Neo4j's cycle-detection query in
# graph_analytics.py to find. Without a `to_wallet` field there was no
# edge at all — CEP could see "one wallet transacting a lot" but nothing
# downstream could ever see "these four wallets are passing money in a
# circle", which is the actual definition of wash trading.
WASH_TRADE_RING = WALLETS[:4]


def make_payload(wallet, to_wallet, amount, gas_fee, true_label):
    """Builds a single transaction payload matching the Spark schema in
    streaming_engine.py. Extra fields are NOT added here on purpose —
    from_json() with an explicit StructType would just drop them, so
    keeping the shape exact avoids confusion downstream.

    `true_label` is the generator's own ground truth ("normal",
    "flash_loan", "wash_trade", "bot_burst") — it costs nothing to keep
    since the generator already knows exactly which case it's emitting,
    and without it there is no way to ever measure whether the CEP rules
    or the Isolation Forest are actually detecting fraud versus just
    firing on something. See evaluate_model.py, which is the entire
    reason this field exists."""
    return {
        "tx_id": f"0x{random.getrandbits(256):064x}",
        "wallet_address": wallet,
        "to_wallet": to_wallet,
        "amount_usd": max(1.0, float(amount)),
        "gas_fee": float(gas_fee),
        "true_label": true_label,
        # timespec="milliseconds": matches Spark's default from_json
        # timestamp pattern (millisecond precision), so event-time
        # parses correctly instead of silently falling back to
        # processing time (see streaming_engine.py's coalesce()).
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
    }


def send(payload, tag):
    producer.send('defi-transactions', value=payload).add_errback(
        lambda exc: print(f"[{tag}] Kafka send failed: {exc}")
    )
    print(
        f"[{tag}] {payload['wallet_address'][:10]}... -> "
        f"{payload['to_wallet'][:10]}... | ${payload['amount_usd']:,.2f}"
    )


def emit_normal():
    """Normal traffic: a wallet interacting with one of the simulated
    protocol contracts (swap, deposit, etc.) — the common case."""
    wallet = str(np.random.choice(WALLETS))
    contract = str(np.random.choice(CONTRACTS))
    amount = float(round(np.random.exponential(scale=250.0), 2))
    gas_fee = float(round(np.random.uniform(0.001, 0.05), 4))
    send(make_payload(wallet, contract, amount, gas_fee, "normal"), "normal")
    time.sleep(0.1)


def emit_flash_loan_anomaly():
    """Single oversized transaction against a lending pool contract."""
    wallet = str(np.random.choice(WALLETS))
    contract = str(np.random.choice(CONTRACTS))
    amount = float(round(np.random.uniform(1_000_000, 10_000_000), 2))
    gas_fee = float(round(np.random.uniform(1.5, 5.0), 4))
    send(make_payload(wallet, contract, amount, gas_fee, "flash_loan"), "flash-loan")
    time.sleep(0.1)


def emit_wash_trading_anomaly():
    """Circular wash trading: the same amount gets passed hop-to-hop
    around a small ring of wallets in quick succession, each hop's
    `to_wallet` being the next wallet in the ring (and the last hop
    closing the loop back to the start) — inflating volume without real
    economic activity, and forming an actual graph cycle. This is what
    the CEP layer's velocity check (C_w > 8 per sliding window) AND
    graph_analytics.py's cycle-detection query are both meant to catch,
    from two different angles (statistical vs. structural)."""
    ring_size = len(WASH_TRADE_RING)
    hops = random.randint(4, 7)
    amount = float(round(np.random.uniform(5_000, 50_000), 2))
    start = random.randrange(ring_size)
    for i in range(hops):
        sender = WASH_TRADE_RING[(start + i) % ring_size]
        receiver = WASH_TRADE_RING[(start + i + 1) % ring_size]
        gas_fee = float(round(np.random.uniform(0.001, 0.01), 4))
        send(make_payload(sender, receiver, amount, gas_fee, "wash_trade"), "wash-trade")
        time.sleep(0.02)


def emit_bot_burst_anomaly():
    """Frontrunning bot burst: one wallet fires a rapid run of small,
    near-identical transactions against a contract with elevated gas
    fees (bots overpay gas to jump the queue), well past the C_w > 8
    velocity threshold."""
    wallet = str(np.random.choice(WALLETS))
    contract = str(np.random.choice(CONTRACTS))
    burst_size = random.randint(9, 15)
    for _ in range(burst_size):
        amount = float(round(np.random.uniform(10, 100), 2))
        gas_fee = float(round(np.random.uniform(0.05, 0.2), 4))
        send(make_payload(wallet, contract, amount, gas_fee, "bot_burst"), "bot-burst")
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
