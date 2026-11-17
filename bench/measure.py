"""Measure a replay run against what Flink wrote to PostgreSQL.

Usage:
    python -m bench.measure bench/runs/run-<id>.json

Reports, for the windows of that run:
- completeness: on-time events expected per window (from the producer) vs n_flows in PostgreSQL;
- latency: time between the end of a window (event time) and its row landing in PostgreSQL;
- throughput: producer send rate, and events counted by Flink per second of event time.
Writes the report to bench/results/run-<id>.md.
"""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import psycopg

PG_DSN = os.getenv("PG_DSN", "postgresql://flink:flink@localhost:5433/netflows")


def to_ts(ms: int) -> datetime:
    # Flink writes window bounds as UTC timestamps without time zone.
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).replace(tzinfo=None)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("summary", type=Path)
    args = ap.parse_args()

    run = json.loads(args.summary.read_text())
    expected = {int(k): v for k, v in run["expected_on_time_per_window"].items()}
    first, last = to_ts(min(expected)), to_ts(max(expected))

    with psycopg.connect(PG_DSN) as conn:
        counted = {
            int(ws.replace(tzinfo=timezone.utc).timestamp() * 1000): n
            for ws, n in conn.execute(
                "SELECT window_start, SUM(n_flows) FROM agg_by_proto "
                "WHERE window_start BETWEEN %s AND %s GROUP BY window_start", (first, last))
        }
        lat = conn.execute(
            "SELECT COUNT(*),"
            " percentile_cont(0.5)  WITHIN GROUP (ORDER BY d),"
            " percentile_cont(0.95) WITHIN GROUP (ORDER BY d),"
            " MAX(d) "
            "FROM (SELECT EXTRACT(EPOCH FROM inserted_at - (window_end AT TIME ZONE 'UTC')) AS d "
            "      FROM agg_by_proto WHERE window_start BETWEEN %s AND %s) t", (first, last)).fetchone()

    closed = sorted(set(expected) & set(counted))
    exact = [w for w in closed if counted[w] == expected[w]]
    not_closed = sorted(set(expected) - set(counted))
    exp_total = sum(expected[w] for w in closed)
    got_total = sum(counted[w] for w in closed)
    span_s = (len(closed) * run["config"]["window_s"]) or 1

    lines = [
        f"# Run {run['run_id']}",
        "",
        f"- Started (UTC): {run['started_at_utc']}",
        f"- Target rate: {run['target_rate']} records/s, records read: {run['records_read']}, "
        f"messages sent: {run['messages_sent']} (not delivered: {run['messages_not_delivered']})",
        f"- Injected: {run['injected']}",
        "",
        "| Metric | Value |",
        "|---|---|",
        f"| Producer send rate | {run['send_rate_msg_per_s']} msg/s |",
        f"| Flink output rate (closed windows) | {got_total / span_s:.1f} events/s of event time |",
        f"| Closed windows matching expected count | {len(exact)} / {len(closed)} |",
        f"| On-time events expected / counted (closed windows) | {exp_total} / {got_total} |",
        f"| Windows not closed yet | {len(not_closed)} |",
    ]
    n, p50, p95, mx = lat
    if n:
        lines += [
            f"| Window-to-PostgreSQL latency p50 | {p50:.2f} s |",
            f"| Window-to-PostgreSQL latency p95 | {p95:.2f} s |",
            f"| Window-to-PostgreSQL latency max | {mx:.2f} s |",
        ]
    lines += ["", "Latency includes the 5 s watermark delay by design (a window can only close "
              "once the watermark passes its end)."]

    report = "\n".join(lines)
    out = Path("bench/results") / f"run-{run['run_id']}.md"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(report + "\n")
    print(report)
    print(f"\nreport -> {out}")


if __name__ == "__main__":
    main()
