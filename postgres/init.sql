-- Sink tables for the Flink window aggregates.
-- Primary keys match the Flink sink definitions: the JDBC connector then
-- writes with INSERT ... ON CONFLICT DO UPDATE, so replaying a window is idempotent.
-- inserted_at is filled by PostgreSQL and is only used to measure latency.

CREATE TABLE agg_by_src_ip (
  window_start    TIMESTAMP(3) NOT NULL,
  window_end      TIMESTAMP(3) NOT NULL,
  src_ip          TEXT         NOT NULL,
  n_flows         BIGINT       NOT NULL,
  n_attack_flows  BIGINT,
  bytes_out       BIGINT,
  bytes_in        BIGINT,
  n_dst_ips       BIGINT,
  n_dst_ports     BIGINT,
  inserted_at     TIMESTAMPTZ  NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (window_start, src_ip)
);

CREATE TABLE agg_by_proto (
  window_start    TIMESTAMP(3) NOT NULL,
  window_end      TIMESTAMP(3) NOT NULL,
  proto           TEXT         NOT NULL,
  n_flows         BIGINT       NOT NULL,
  n_attack_flows  BIGINT,
  total_bytes     BIGINT,
  avg_duration_s  DOUBLE PRECISION,
  inserted_at     TIMESTAMPTZ  NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (window_start, proto)
);
