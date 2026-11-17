"""Pure replay logic: parse ToN-IoT rows and decide what to send.

Nothing in this module talks to Kafka, so every decision (event time,
duplicates, late events) is deterministic for a given seed and unit-testable.
"""
from __future__ import annotations

import random
from collections import Counter
from dataclasses import dataclass, field

# Columns of the ToN-IoT network CSV that the pipeline uses.
REQUIRED_COLUMNS = (
    "src_ip", "src_port", "dst_ip", "dst_port", "proto", "service",
    "duration", "src_bytes", "dst_bytes", "src_pkts", "dst_pkts",
    "conn_state", "label", "type",
)

# ToN-IoT encodes missing values as the literal string "-".
MISSING = "-"


def _to_int(value: str) -> int | None:
    if value in ("", MISSING):
        return None
    return int(float(value))


def _to_float(value: str) -> float | None:
    if value in ("", MISSING):
        return None
    return float(value)


def _to_str(value: str) -> str | None:
    return None if value in ("", MISSING) else value


def check_columns(fieldnames: list[str] | None) -> None:
    missing = [c for c in REQUIRED_COLUMNS if c not in (fieldnames or [])]
    if missing:
        raise ValueError(f"CSV is missing required columns: {missing}")


def parse_row(row: dict[str, str], event_id: str) -> dict:
    """Map one CSV row to the message schema (event_ts is set later)."""
    return {
        "event_id": event_id,
        "src_ip": row["src_ip"],
        "src_port": _to_int(row["src_port"]),
        "dst_ip": row["dst_ip"],
        "dst_port": _to_int(row["dst_port"]),
        "proto": _to_str(row["proto"]),
        "service": _to_str(row["service"]),
        "duration_s": _to_float(row["duration"]),
        "src_bytes": _to_int(row["src_bytes"]),
        "dst_bytes": _to_int(row["dst_bytes"]),
        "src_pkts": _to_int(row["src_pkts"]),
        "dst_pkts": _to_int(row["dst_pkts"]),
        "conn_state": _to_str(row["conn_state"]),
        "is_attack": _to_int(row["label"]),
        "attack_type": _to_str(row["type"]),
    }


def window_start_ms(event_ts_ms: int, window_s: int) -> int:
    """Start of the tumbling window containing event_ts (epoch-aligned, like Flink)."""
    size = window_s * 1000
    return event_ts_ms - (event_ts_ms % size)


@dataclass
class ReplayConfig:
    dup_fraction: float = 0.01      # share of events re-sent with the same event_id
    late_fraction: float = 0.005    # share of events sent with an old event time
    late_delay_s: float = 30.0      # how old a late event is (must exceed the watermark delay)
    jitter_max_s: float = 3.0       # on-time events are shifted back by U(0, jitter) -> out of order
    late_warmup_s: float = 10.0     # no late events before the watermark exists
    window_s: int = 10              # must match the Flink window size
    seed: int = 42


@dataclass
class ReplayPlanner:
    """Decides, for each record, which messages to send and with which event time.

    It also keeps the expected count of on-time events per window, so the
    Flink output can be checked exactly afterwards (see bench/measure.py).
    """
    cfg: ReplayConfig
    expected_per_window: Counter = field(default_factory=Counter)
    stats: Counter = field(default_factory=Counter)

    def __post_init__(self) -> None:
        self._rng = random.Random(self.cfg.seed)

    def plan(self, record: dict, now_ms: int, elapsed_s: float) -> list[tuple[str, dict]]:
        # Always draw the three numbers so the random sequence does not depend on the branch taken.
        r_late, r_jitter, r_dup = self._rng.random(), self._rng.random(), self._rng.random()
        msg = dict(record)

        if elapsed_s >= self.cfg.late_warmup_s and r_late < self.cfg.late_fraction:
            msg["event_ts"] = now_ms - int(self.cfg.late_delay_s * 1000)
            self.stats["late"] += 1
            return [("late", msg)]

        msg["event_ts"] = now_ms - int(r_jitter * self.cfg.jitter_max_s * 1000)
        self.stats["on_time"] += 1
        self.expected_per_window[window_start_ms(msg["event_ts"], self.cfg.window_s)] += 1
        out = [("on_time", msg)]
        if r_dup < self.cfg.dup_fraction:
            self.stats["duplicate"] += 1
            out.append(("duplicate", dict(msg)))
        return out
