# Run d7b24a89

- Started (UTC): 2026-11-17T22:36:24+00:00
- Target rate: 500.0 records/s, records read: 60001, messages sent: 60561 (not delivered: 0)
- Injected: {'on_time': 59706, 'duplicate': 560, 'late': 295}

| Metric | Value |
|---|---|
| Producer send rate | 504.7 msg/s |
| Flink output rate (closed windows) | 482.9 events/s of event time |
| Closed windows matching expected count | 11 / 11 |
| On-time events expected / counted (closed windows) | 53118 / 53118 |
| Windows not closed yet | 2 |
| Window-to-PostgreSQL latency p50 | 6.80 s |
| Window-to-PostgreSQL latency p95 | 8.39 s |
| Window-to-PostgreSQL latency max | 11.76 s |

Latency includes the 5 s watermark delay by design (a window can only close once the watermark passes its end).
