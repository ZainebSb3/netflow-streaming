"""Replay ToN-IoT network flows into Kafka as a live stream.

Usage (from the repo root):
    python -m replay.producer --csv data/train_test_network.csv --rate 500
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer

from replay.flows import ReplayConfig, ReplayPlanner, check_columns, parse_row


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--csv", required=True, type=Path)
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default="netflows.raw")
    p.add_argument("--rate", type=float, default=500.0, help="records per second, 0 = as fast as possible")
    p.add_argument("--limit", type=int, default=0, help="stop after N records (0 = whole file)")
    p.add_argument("--duration-s", type=float, default=0.0, help="stop after N seconds (0 = no limit)")
    p.add_argument("--loop", action="store_true", help="restart from the top of the file when it ends")
    p.add_argument("--dup-fraction", type=float, default=0.01)
    p.add_argument("--late-fraction", type=float, default=0.005)
    p.add_argument("--late-delay-s", type=float, default=30.0)
    p.add_argument("--jitter-max-s", type=float, default=3.0)
    p.add_argument("--late-warmup-s", type=float, default=10.0)
    p.add_argument("--window-s", type=int, default=10)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--out-dir", type=Path, default=Path("bench/runs"))
    return p.parse_args(argv)


def iter_rows(csv_path: Path, loop: bool):
    """Yield (pass_number, row_number, row) — loops over the file if asked."""
    pass_no = 0
    while True:
        with csv_path.open(newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            check_columns(reader.fieldnames)
            for row_no, row in enumerate(reader):
                yield pass_no, row_no, row
        if not loop:
            return
        pass_no += 1


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    cfg = ReplayConfig(
        dup_fraction=args.dup_fraction, late_fraction=args.late_fraction,
        late_delay_s=args.late_delay_s, jitter_max_s=args.jitter_max_s,
        late_warmup_s=args.late_warmup_s, window_s=args.window_s, seed=args.seed,
    )
    planner = ReplayPlanner(cfg)

    # A fresh run id makes event_ids unique across runs; otherwise a second run
    # would be dropped entirely by the dedup job (its state keeps ids for 1 h).
    run_id = uuid.uuid4().hex[:8]

    producer = Producer({
        "bootstrap.servers": args.bootstrap,
        "enable.idempotence": True,  # broker-side retries never create accidental duplicates
        "acks": "all",
        "linger.ms": 5,
    })
    delivery_errors = 0

    def on_delivery(err, _msg):
        nonlocal delivery_errors
        if err is not None:
            delivery_errors += 1

    interval = 1.0 / args.rate if args.rate > 0 else 0.0
    started_wall = datetime.now(timezone.utc)
    t0 = time.perf_counter()
    records = messages = 0

    try:
        for pass_no, row_no, row in iter_rows(args.csv, args.loop):
            if args.limit and records >= args.limit:
                break
            elapsed = time.perf_counter() - t0
            if args.duration_s and elapsed >= args.duration_s:
                break
            if interval:  # pace against the ideal schedule, not the previous send
                delay = t0 + records * interval - time.perf_counter()
                if delay > 0:
                    time.sleep(delay)

            record = parse_row(row, event_id=f"{run_id}-{pass_no}-{row_no}")
            now_ms = int(time.time() * 1000)
            for _kind, msg in planner.plan(record, now_ms, time.perf_counter() - t0):
                while True:
                    try:
                        producer.produce(
                            args.topic, key=msg["src_ip"].encode(),
                            value=json.dumps(msg).encode(), on_delivery=on_delivery,
                        )
                        break
                    except BufferError:  # local queue full: let librdkafka drain it
                        producer.poll(0.1)
                messages += 1
            producer.poll(0)
            records += 1
    except KeyboardInterrupt:
        print("interrupted, flushing...", file=sys.stderr)

    remaining = producer.flush(30)
    elapsed = time.perf_counter() - t0

    summary = {
        "run_id": run_id,
        "started_at_utc": started_wall.isoformat(timespec="seconds"),
        "csv": str(args.csv),
        "config": vars(cfg),
        "target_rate": args.rate,
        "records_read": records,
        "messages_sent": messages,
        "messages_not_delivered": delivery_errors + remaining,
        "elapsed_s": round(elapsed, 3),
        "send_rate_msg_per_s": round(messages / elapsed, 1) if elapsed else None,
        "injected": dict(planner.stats),
        "expected_on_time_per_window": {str(k): v for k, v in sorted(planner.expected_per_window.items())},
    }
    args.out_dir.mkdir(parents=True, exist_ok=True)
    out_path = args.out_dir / f"run-{run_id}.json"
    out_path.write_text(json.dumps(summary, indent=2))

    print(f"run {run_id}: {records} records, {messages} messages in {elapsed:.1f} s "
          f"({summary['send_rate_msg_per_s']} msg/s), injected={dict(planner.stats)}, "
          f"not delivered={summary['messages_not_delivered']}")
    print(f"summary -> {out_path}")
    return 0 if summary["messages_not_delivered"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
