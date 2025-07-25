-- Migration 001 Rollback: Drop all initial tables

DROP TABLE IF EXISTS {database}.execution_requests;
DROP TABLE IF EXISTS {database}.blob_commitments;
DROP TABLE IF EXISTS {database}.bls_changes;
DROP TABLE IF EXISTS {database}.attester_slashings;
DROP TABLE IF EXISTS {database}.proposer_slashings;
DROP TABLE IF EXISTS {database}.voluntary_exits;
DROP TABLE IF EXISTS {database}.deposits;
DROP TABLE IF EXISTS {database}.attestations;
DROP TABLE IF EXISTS {database}.withdrawals;
DROP TABLE IF EXISTS {database}.transactions;
DROP TABLE IF EXISTS {database}.execution_payloads;
DROP TABLE IF EXISTS {database}.sync_aggregates;
DROP TABLE IF EXISTS {database}.blocks;