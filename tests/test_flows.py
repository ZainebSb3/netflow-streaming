import csv
from pathlib import Path

import pytest

from replay.flows import ReplayConfig, ReplayPlanner, check_columns, parse_row, window_start_ms

FIXTURE = Path(__file__).parent / "fixtures" / "sample_flows.csv"


def load_rows():
    with FIXTURE.open(newline="") as f:
        reader = csv.DictReader(f)
        check_columns(reader.fieldnames)
        return list(reader)


def test_parse_row_casts_types_and_maps_missing_values():
    rows = load_rows()
    rec = parse_row(rows[0], event_id="run-0-0")

    assert rec["event_id"] == "run-0-0"
    assert isinstance(rec["src_port"], int) and isinstance(rec["duration_s"], float)
    assert rec["is_attack"] in (0, 1)
    # ToN-IoT writes "-" for missing values: they must become JSON null, not the string "-".
    missing = parse_row(rows[1], event_id="run-0-1")
    assert missing["service"] is None
    assert missing["duration_s"] is None

    with pytest.raises(ValueError):
        check_columns(["src_ip", "dst_ip"])


def test_planner_is_deterministic_and_counts_only_on_time_events():
    cfg = ReplayConfig(dup_fraction=0.2, late_fraction=0.1, late_delay_s=30,
                       jitter_max_s=3, late_warmup_s=1, window_s=10, seed=123)
    record = parse_row(load_rows()[0], event_id="x")

    def simulate():
        planner = ReplayPlanner(cfg)
        sent = []
        t0 = 1_700_000_000_000
        for i in range(2000):
            now = t0 + i * 10  # 100 records/s of simulated time
            sent += planner.plan({**record, "event_id": f"x-{i}"}, now, (now - t0) / 1000)
        return planner, sent

    p1, sent1 = simulate()
    p2, sent2 = simulate()
    assert sent1 == sent2, "same seed must give the same stream"

    late = [m for kind, m in sent1 if kind == "late"]
    on_time = [m for kind, m in sent1 if kind == "on_time"]
    dups = [m for kind, m in sent1 if kind == "duplicate"]
    assert late and dups
    # No late event during the 1 s warm-up (late event_ts = send time - 30 s).
    assert all(m["event_ts"] >= 1_700_000_000_000 + 1000 - 30_000 for m in late)
    # On-time events are shifted back by at most jitter_max_s (3 s), well under the 5 s watermark delay.
    assert all(m["event_ts"] >= 1_700_000_000_000 - 3000 for m in on_time)
    # A duplicate carries exactly the same event_id and event_ts as its original.
    on_time_by_id = {m["event_id"]: m for m in on_time}
    assert all(on_time_by_id[d["event_id"]] == d for d in dups)
    # Expected counts = on-time events only (duplicates and late events are excluded).
    assert sum(p1.expected_per_window.values()) == len(on_time)
    assert all(ws == window_start_ms(ws, 10) for ws in p1.expected_per_window)
