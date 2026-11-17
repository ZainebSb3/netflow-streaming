"""End-to-end test: needs `make up jobs` running (Kafka on localhost:9092, Postgres on 5433).

Sends a controlled burst for one test IP — 20 events, 5 re-sent duplicates,
3 events 60 s late — and checks that PostgreSQL holds exactly 20 flows for
that IP, in a single window.
"""
import json
import os
import random
import threading
import time
import uuid

import pytest

pytestmark = pytest.mark.e2e

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
PG_DSN = os.getenv("PG_DSN", "postgresql://flink:flink@localhost:5433/netflows")
WINDOW_MS = 10_000


def now_ms() -> int:
    return int(time.time() * 1000)


def event(event_id, src_ip, ts, is_attack=0):
    return {"event_id": event_id, "event_ts": ts, "src_ip": src_ip, "src_port": 40000,
            "dst_ip": "10.250.0.1", "dst_port": 80, "proto": "tcp", "service": "http",
            "duration_s": 0.1, "src_bytes": 100, "dst_bytes": 200, "src_pkts": 1, "dst_pkts": 1,
            "conn_state": "SF", "is_attack": is_attack, "attack_type": "test"}


@pytest.fixture
def producer():
    from confluent_kafka import Producer
    p = Producer({"bootstrap.servers": BOOTSTRAP, "enable.idempotence": True})
    try:
        p.list_topics(timeout=5)
    except Exception:
        pytest.skip(f"Kafka not reachable on {BOOTSTRAP} (run `make up jobs` first)")
    return p


@pytest.fixture
def pg():
    psycopg = pytest.importorskip("psycopg")
    try:
        conn = psycopg.connect(PG_DSN, autocommit=True, connect_timeout=5)
    except psycopg.OperationalError:
        pytest.skip(f"PostgreSQL not reachable ({PG_DSN})")
    yield conn
    conn.close()


def test_duplicates_removed_and_late_events_dropped(producer, pg):
    run = uuid.uuid4().hex[:8]
    test_ip = f"10.250.{random.randint(1, 250)}.{random.randint(1, 250)}"
    send = lambda ev: producer.produce("netflows.raw", key=ev["src_ip"].encode(), value=json.dumps(ev).encode())

    # Heartbeat: steady on-time traffic from another IP so the watermark keeps moving.
    stop = threading.Event()

    def heartbeat():
        i = 0
        while not stop.is_set():
            send(event(f"hb-{run}-{i}", "10.250.255.254", now_ms()))
            producer.poll(0)
            i += 1
            time.sleep(0.2)

    hb = threading.Thread(target=heartbeat, daemon=True)
    hb.start()
    try:
        time.sleep(3)  # watermark is now about (now - 5 s)

        # Put the whole burst in one window, with at least 3 s of margin before it ends.
        ts = now_ms()
        if (ts % WINDOW_MS) > WINDOW_MS - 3000:
            time.sleep((WINDOW_MS - ts % WINDOW_MS) / 1000 + 0.1)
            ts = now_ms()

        for i in range(20):
            send(event(f"e2e-{run}-{i}", test_ip, ts, is_attack=1 if i < 7 else 0))
        for i in range(5):  # same event_id re-sent: must be removed by the dedup job
            send(event(f"e2e-{run}-{i}", test_ip, ts, is_attack=1))
        producer.flush(10)

        time.sleep(1)
        for i in range(3):  # 60 s older than the watermark: must be dropped
            send(event(f"late-{run}-{i}", test_ip, ts - 60_000))
        producer.flush(10)

        # The window closes when the watermark passes its end (window end + 5 s).
        rows = []
        deadline = time.time() + 60
        while time.time() < deadline:
            rows = pg.execute(
                "SELECT window_start, n_flows, n_attack_flows FROM agg_by_src_ip WHERE src_ip = %s",
                (test_ip,)).fetchall()
            if rows:
                break
            time.sleep(1)
        time.sleep(3)  # leave time for a (wrong) row from the late events to show up
        rows = pg.execute(
            "SELECT window_start, n_flows, n_attack_flows FROM agg_by_src_ip WHERE src_ip = %s",
            (test_ip,)).fetchall()
    finally:
        stop.set()
        hb.join()
        producer.flush(10)

    assert len(rows) == 1, f"expected one window for {test_ip}, got {rows}"
    _, n_flows, n_attack = rows[0]
    assert n_flows == 20, "duplicates were counted" if n_flows > 20 else "events were lost"
    assert n_attack == 7
