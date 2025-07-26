-- Create simplified era completion table
CREATE TABLE IF NOT EXISTS {database}.era_completion (
    network String,
    era_number UInt32,
    status Enum8('processing' = 0, 'completed' = 1, 'failed' = 2),
    slot_start UInt32,
    slot_end UInt32,
    total_records UInt64,
    datasets_processed Array(String),
    processing_started_at DateTime,
    completed_at DateTime DEFAULT now(),
    error_message String DEFAULT '',
    retry_count UInt8 DEFAULT 0,
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY network
ORDER BY (network, era_number);

-- Create era status view for latest state
CREATE VIEW IF NOT EXISTS {database}.era_status AS
SELECT 
    network,
    era_number,
    status,
    slot_start,
    slot_end,
    total_records,
    length(datasets_processed) as dataset_count,
    processing_started_at,
    completed_at,
    retry_count,
    error_message
FROM {database}.era_completion
WHERE (network, era_number, insert_version) IN (
    SELECT network, era_number, max(insert_version)
    FROM {database}.era_completion
    GROUP BY network, era_number
);