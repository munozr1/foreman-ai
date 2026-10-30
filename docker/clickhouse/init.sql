CREATE DATABASE IF NOT EXISTS foremen;

CREATE TABLE IF NOT EXISTS foremen.mutations
(
    commit_version UInt64,
    key String,
    op_code UInt8,
    value String,
    range_end String,
    kafka_offset Int64,
    batch_index UInt32,
    ingested_at DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree
ORDER BY (key, commit_version, kafka_offset, batch_index);

CREATE TABLE IF NOT EXISTS foremen.version_ends
(
    commit_version UInt64,
    kafka_offset Int64,
    ingested_at DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree
ORDER BY (commit_version, kafka_offset);

CREATE TABLE IF NOT EXISTS foremen.agent_decisions
(
    decision_id UUID,
    run_id String,
    trace_id String,
    agent_id String,
    tool_name String,
    outcome Enum8('committed' = 1, 'blocked_stale' = 2),
    decision_version UInt64,
    decided_at DateTime64(3),
    read_keys Array(String),
    read_versions Array(UInt64),
    CONSTRAINT aligned_reads CHECK length(read_keys) = length(read_versions)
)
ENGINE = MergeTree
ORDER BY (run_id, decided_at);
