-- ============================================================================
-- Pipeline Execution Drive Table DDL
-- ============================================================================
-- Purpose: Central repository tracking all pipeline executions
-- Database: Snowflake
-- Schema: PIPELINE_TRACKING (or your preferred schema)
-- ============================================================================

CREATE OR REPLACE TABLE pipeline_execution_drive (

    -- ========================================================================
    -- IDENTIFICATION COLUMNS
    -- ========================================================================
    pipeline_id VARCHAR(255) NOT NULL PRIMARY KEY
        COMMENT 'Unique identifier for this pipeline execution (e.g., genetic_tests_20251115_10h_run1)',

    pipeline_name VARCHAR(255) NOT NULL
        COMMENT 'Human-readable name of the pipeline (e.g., genetic_tests, lab_results)',

    -- ========================================================================
    -- EXECUTION STATUS COLUMNS
    -- ========================================================================
    pipeline_status VARCHAR(50) NOT NULL
        COMMENT 'Current status: RUNNING, SUCCESS, FAILED, SKIPPED',

    pipeline_retry_number INTEGER NOT NULL DEFAULT 0
        COMMENT 'Which attempt is this execution (0=first run, 1=first retry, etc.)',

    -- ========================================================================
    -- TIMING COLUMNS
    -- ========================================================================
    pipeline_start_timestamp TIMESTAMP_TZ NOT NULL
        COMMENT 'When the pipeline execution started (UTC)',

    pipeline_end_timestamp TIMESTAMP_TZ
        COMMENT 'When the pipeline execution completed (NULL while running)',

    pipeline_run_duration VARCHAR(50)
        COMMENT 'Human-readable total execution time (e.g., "1h 30m 45s")',

    -- ========================================================================
    -- QUERY WINDOW COLUMNS (Incremental Load Tracking)
    -- ========================================================================
    query_window_start_timestamp TIMESTAMP_TZ NOT NULL
        COMMENT 'Start of the time window being processed',

    query_window_end_timestamp TIMESTAMP_TZ NOT NULL
        COMMENT 'End of the time window being processed',

    target_date DATE NOT NULL
        COMMENT 'Business date for this pipeline execution (derived from query_window_start)',

    query_window_duration VARCHAR(50) NOT NULL
        COMMENT 'Human-readable time window size (e.g., "1h", "24h", "1d")',

    -- ========================================================================
    -- PHASE EXECUTION COLUMNS (VARIANT - Semi-structured JSON)
    -- ========================================================================
    -- Each phase has a VARIANT column storing execution metadata
    -- Structure: {start_timestamp, end_timestamp, actual_run_duration, retry_number,
    --             error_message, status, expected_run_duration, phase_specific_details}

    stale_pipeline_handling_phase VARIANT
        COMMENT 'Execution details for stale pipeline handling phase',

    pre_validation_phase VARIANT
        COMMENT 'Execution details for pre-validation phase',

    source_to_stage_transfer_phase VARIANT
        COMMENT 'Execution details for source to stage transfer phase',

    stage_to_target_transfer_phase VARIANT
        COMMENT 'Execution details for stage to target transfer phase',

    audit_phase VARIANT
        COMMENT 'Execution details for audit phase (includes source_count, stage_count, target_count)',

    stage_cleaning_phase VARIANT
        COMMENT 'Execution details for stage cleaning phase',

    target_cleaning_phase VARIANT
        COMMENT 'Execution details for target cleaning phase (optional)',

    -- ========================================================================
    -- PHASE SUMMARY COLUMNS (Arrays - Denormalized for Quick Queries)
    -- ========================================================================
    phases_completed ARRAY
        COMMENT 'Array of phase names that completed successfully',

    phases_skipped ARRAY
        COMMENT 'Array of phase names that were skipped (already completed in previous run)',

    phase_failed VARCHAR(255)
        COMMENT 'Name of the phase that failed (NULL if all succeeded)',

    phases_pending ARRAY
        COMMENT 'Array of phases that haven''t executed yet (still pending)',

    -- ========================================================================
    -- METADATA COLUMNS
    -- ========================================================================
    created_at TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
        COMMENT 'When this drive table row was created',

    updated_at TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
        COMMENT 'When this row was last updated',

    -- ========================================================================
    -- CONSTRAINTS AND VALIDATIONS
    -- ========================================================================
    CONSTRAINT chk_pipeline_status
        CHECK (pipeline_status IN ('RUNNING', 'SUCCESS', 'FAILED', 'SKIPPED')),

    CONSTRAINT chk_query_window
        CHECK (query_window_end_timestamp > query_window_start_timestamp),

    CONSTRAINT chk_retry_number
        CHECK (pipeline_retry_number >= 0)
);

-- ============================================================================
-- INDEXES FOR PERFORMANCE
-- ============================================================================

-- Index for finding latest run of a specific pipeline
CREATE INDEX idx_pipeline_name_created
    ON pipeline_execution_drive(pipeline_name, created_at DESC);

-- Index for finding failed pipelines
CREATE INDEX idx_pipeline_status
    ON pipeline_execution_drive(pipeline_status);

-- Index for finding pipelines by target date
CREATE INDEX idx_target_date
    ON pipeline_execution_drive(target_date DESC);

-- Index for finding pipelines by query window
CREATE INDEX idx_query_window
    ON pipeline_execution_drive(query_window_start_timestamp, query_window_end_timestamp);

-- ============================================================================
-- COMMENTS ON TABLE
-- ============================================================================

COMMENT ON TABLE pipeline_execution_drive IS
'Central tracking table for all pipeline executions. One row per pipeline execution
(includes original runs and retries). Contains complete execution history with
phase-level details stored in VARIANT columns for flexible querying.';

-- ============================================================================
-- SAMPLE QUERIES
-- ============================================================================

-- Query 1: Find Latest Pipeline Execution
-- SELECT * FROM pipeline_execution_drive
-- WHERE pipeline_name = 'genetic_tests'
-- ORDER BY created_at DESC
-- LIMIT 1;

-- Query 2: Find Failed Pipelines in Last 24 Hours
-- SELECT pipeline_name, phase_failed, created_at
-- FROM pipeline_execution_drive
-- WHERE pipeline_status = 'FAILED'
--   AND created_at > DATEADD(hour, -24, CURRENT_TIMESTAMP());

-- Query 3: Find Which Phase Failed for a Specific Run
-- SELECT pipeline_id, phase_failed,
--        CASE
--            WHEN phase_failed = 'stale_pipeline_handling' THEN stale_pipeline_handling_phase:error_message
--            WHEN phase_failed = 'pre_validation' THEN pre_validation_phase:error_message
--            WHEN phase_failed = 'source_to_stage_transfer' THEN source_to_stage_transfer_phase:error_message
--            WHEN phase_failed = 'stage_to_target_transfer' THEN stage_to_target_transfer_phase:error_message
--            WHEN phase_failed = 'audit' THEN audit_phase:error_message
--            WHEN phase_failed = 'stage_cleaning' THEN stage_cleaning_phase:error_message
--            WHEN phase_failed = 'target_cleaning' THEN target_cleaning_phase:error_message
--        END as error_message
-- FROM pipeline_execution_drive
-- WHERE pipeline_id = 'genetic_tests_20251115_10h_run1';

-- Query 4: Compare Run Duration to Expected Duration
-- SELECT pipeline_id, pipeline_run_duration,
--        stale_pipeline_handling_phase:actual_run_duration as stale_handling_actual,
--        stale_pipeline_handling_phase:expected_run_duration as stale_handling_expected
-- FROM pipeline_execution_drive
-- WHERE pipeline_name = 'genetic_tests'
--   AND pipeline_status = 'SUCCESS'
-- ORDER BY created_at DESC;

-- Query 5: Audit Trail for a Specific Pipeline
-- SELECT pipeline_id, pipeline_status, target_date,
--        phases_completed, phase_failed, pipeline_run_duration
-- FROM pipeline_execution_drive
-- WHERE pipeline_name = 'genetic_tests'
-- ORDER BY target_date DESC;

-- Query 6: Data Freshness Check
-- SELECT pipeline_name,
--        MAX(created_at) as last_successful_run,
--        DATEDIFF(hour, MAX(created_at), CURRENT_TIMESTAMP()) as hours_since_last_run
-- FROM pipeline_execution_drive
-- WHERE pipeline_status = 'SUCCESS'
-- GROUP BY pipeline_name
-- HAVING hours_since_last_run > 24;  -- Alert if > 24 hours

-- ============================================================================
-- END OF DDL
-- ============================================================================
