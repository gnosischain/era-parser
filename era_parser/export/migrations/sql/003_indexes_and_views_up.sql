-- Migration 003: Essential views for era processing monitoring

CREATE VIEW IF NOT EXISTS {database}.era_processing_progress AS
SELECT 
    network,
    era_filename,
    era_number,
    countIf(status = 'completed') as completed_datasets,
    countIf(status = 'processing') as processing_datasets,
    countIf(status = 'failed') as failed_datasets,
    countIf(status = 'pending') as pending_datasets,
    count(*) as total_datasets,
    sum(rows_inserted) as total_rows_inserted,
    maxIf(completed_at, status = 'completed') as last_completed_at
FROM {database}.era_processing_state
GROUP BY network, era_filename, era_number;

CREATE VIEW IF NOT EXISTS {database}.dataset_processing_progress AS
SELECT
    network,
    dataset,
    countIf(status = 'completed') as completed_eras,
    countIf(status = 'processing') as processing_eras,
    countIf(status = 'failed') as failed_eras,
    countIf(status = 'pending') as pending_eras,
    count(*) as total_eras,
    sum(rows_inserted) as total_rows_inserted,
    maxIf(era_number, status = 'completed') as highest_completed_era
FROM {database}.era_processing_state  
GROUP BY network, dataset;

CREATE VIEW IF NOT EXISTS {database}.era_processing_summary AS
SELECT 
    network,
    count(*) as total_eras_tracked,
    countIf(completed_datasets = total_datasets AND completed_datasets > 0) as fully_completed_eras,
    countIf(processing_datasets > 0) as eras_in_progress,
    countIf(failed_datasets > 0 AND completed_datasets = 0) as fully_failed_eras,
    sum(total_rows_inserted) as total_rows_processed,
    max(era_number) as highest_era_number,
    max(last_completed_at) as last_activity
FROM {database}.era_processing_progress
GROUP BY network;