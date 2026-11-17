-- Job 1: remove duplicate deliveries (same event_id) from netflows.raw
-- and write a clean, append-only stream to netflows.dedup.

SET 'pipeline.name' = 'netflows-dedup';
SET 'execution.checkpointing.interval' = '10 s';
-- Dedup keeps one entry per event_id in state; without a TTL that state grows forever.
SET 'table.exec.state.ttl' = '1 h';

CREATE TABLE netflows_raw (
  event_id     STRING,
  event_ts     BIGINT,          -- event time, epoch milliseconds
  src_ip       STRING,
  src_port     INT,
  dst_ip       STRING,
  dst_port     INT,
  proto        STRING,
  service      STRING,
  duration_s   DOUBLE,
  src_bytes    BIGINT,
  dst_bytes    BIGINT,
  src_pkts     BIGINT,
  dst_pkts     BIGINT,
  conn_state   STRING,
  is_attack    INT,
  attack_type  STRING,
  proc_time AS PROCTIME()       -- arrival time in Flink, used to pick the first copy
) WITH (
  'connector' = 'kafka',
  'topic' = 'netflows.raw',
  'properties.bootstrap.servers' = 'kafka:29092',
  'properties.group.id' = 'flink-dedup',
  'scan.startup.mode' = 'group-offsets',
  'properties.auto.offset.reset' = 'earliest',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

CREATE TABLE netflows_dedup (
  event_id     STRING,
  event_ts     BIGINT,
  src_ip       STRING,
  src_port     INT,
  dst_ip       STRING,
  dst_port     INT,
  proto        STRING,
  service      STRING,
  duration_s   DOUBLE,
  src_bytes    BIGINT,
  dst_bytes    BIGINT,
  src_pkts     BIGINT,
  dst_pkts     BIGINT,
  conn_state   STRING,
  is_attack    INT,
  attack_type  STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'netflows.dedup',
  'properties.bootstrap.servers' = 'kafka:29092',
  'key.format' = 'raw',
  'key.fields' = 'src_ip',       -- same partitioning key as the raw topic
  'value.format' = 'json',
  'value.fields-include' = 'ALL',
  'sink.delivery-guarantee' = 'at-least-once'
);

-- Keep the first copy of each event_id. Ordering on processing time makes the
-- result append-only (a kept row is never retracted), so it can go to Kafka.
INSERT INTO netflows_dedup
SELECT event_id, event_ts, src_ip, src_port, dst_ip, dst_port, proto, service,
       duration_s, src_bytes, dst_bytes, src_pkts, dst_pkts, conn_state,
       is_attack, attack_type
FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY proc_time ASC) AS rn
  FROM netflows_raw
)
WHERE rn = 1;
