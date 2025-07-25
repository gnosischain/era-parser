-- Migration 003 Rollback: Drop views

DROP VIEW IF EXISTS {database}.era_processing_summary;
DROP VIEW IF EXISTS {database}.dataset_processing_progress;
DROP VIEW IF EXISTS {database}.era_processing_progress;