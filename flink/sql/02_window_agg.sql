-- Job 2: event-time aggregates over 10-second tumbling windows,
-- per source IP and per protocol, written to PostgreSQL.

SET 'pipeline.name' = 'netflows-window-agg';
SET 'execution.checkpointing.interval' = '10 s';
SET 'table.local-time-zone' = 'UTC';
-- A partition with no traffic for 5 s stops holding the watermark back.
SET 'table.exec.source.idle-timeout' = '5 s';

CREATE TABLE netflows (
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
  attack_type  STRING,
  event_time AS TO_TIMESTAMP_LTZ(event_ts, 3),
  -- Accept events up to 5 s out of order; anything older than the watermark is late and dropped.
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'netflows.dedup',
  'properties.bootstrap.servers' = 'kafka:29092',
  'properties.group.id' = 'flink-window-agg',
  'scan.startup.mode' = 'group-offsets',
  'properties.auto.offset.reset' = 'earliest',
  'format' = 'json'
);

CREATE TABLE agg_by_src_ip (
  window_start    TIMESTAMP(3),
  window_end      TIMESTAMP(3),
  src_ip          STRING,
  n_flows         BIGINT,
  n_attack_flows  BIGINT,
  bytes_out       BIGINT,
  bytes_in        BIGINT,
  n_dst_ips       BIGINT,
  n_dst_ports     BIGINT,
  PRIMARY KEY (window_start, src_ip) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://postgres:5432/netflows',
  'table-name' = 'agg_by_src_ip',
  'username' = 'flink',
  'password' = 'flink',
  'sink.buffer-flush.max-rows' = '500',
  'sink.buffer-flush.interval' = '1 s'
);

CREATE TABLE agg_by_proto (
  window_start    TIMESTAMP(3),
  window_end      TIMESTAMP(3),
  proto           STRING,
  n_flows         BIGINT,
  n_attack_flows  BIGINT,
  total_bytes     BIGINT,
  avg_duration_s  DOUBLE,
  PRIMARY KEY (window_start, proto) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://postgres:5432/netflows',
  'table-name' = 'agg_by_proto',
  'username' = 'flink',
  'password' = 'flink',
  'sink.buffer-flush.max-rows' = '500',
  'sink.buffer-flush.interval' = '1 s'
);

-- Both INSERTs run in one Flink job and share a single Kafka source.
EXECUTE STATEMENT SET
BEGIN

INSERT INTO agg_by_src_ip
SELECT window_start, window_end, src_ip,
       COUNT(*)                               AS n_flows,
       SUM(CAST(is_attack AS BIGINT))         AS n_attack_flows,
       SUM(src_bytes)                         AS bytes_out,
       SUM(dst_bytes)                         AS bytes_in,
       COUNT(DISTINCT dst_ip)                 AS n_dst_ips,
       COUNT(DISTINCT dst_port)               AS n_dst_ports
FROM TABLE(TUMBLE(TABLE netflows, DESCRIPTOR(event_time), INTERVAL '10' SECOND))
GROUP BY window_start, window_end, src_ip;

INSERT INTO agg_by_proto
SELECT window_start, window_end, COALESCE(proto, 'unknown') AS proto,
       COUNT(*)                                       AS n_flows,
       SUM(CAST(is_attack AS BIGINT))                 AS n_attack_flows,
       SUM(COALESCE(src_bytes, 0) + COALESCE(dst_bytes, 0)) AS total_bytes,
       AVG(duration_s)                                AS avg_duration_s
FROM TABLE(TUMBLE(TABLE netflows, DESCRIPTOR(event_time), INTERVAL '10' SECOND))
GROUP BY window_start, window_end, COALESCE(proto, 'unknown');

END;
