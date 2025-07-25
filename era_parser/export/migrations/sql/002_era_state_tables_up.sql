-- Migration 002: Era processing state management tables

CREATE TABLE IF NOT EXISTS {database}.era_processing_state (
    `era_filename` String,              
    `network` String,                   
    `era_number` UInt32,               
    `dataset` String,                   
    `status` String,                    
    `worker_id` String DEFAULT '',     
    `attempt_count` UInt8 DEFAULT 0,   
    `created_at` DateTime DEFAULT now(),
    `completed_at` Nullable(DateTime),
    `rows_inserted` Nullable(UInt64),  
    `file_hash` String DEFAULT '',     
    `error_message` Nullable(String),
    `processing_duration_ms` Nullable(UInt64),
    `insert_version` UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9)),
    
    INDEX idx_status (status) TYPE minmax GRANULARITY 4,
    INDEX idx_network_dataset (network, dataset) TYPE minmax GRANULARITY 4,
    INDEX idx_era_number (era_number) TYPE minmax GRANULARITY 4
) ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY (network, toYYYYMM(created_at))
ORDER BY (era_filename, dataset)
SETTINGS index_granularity = 8192;