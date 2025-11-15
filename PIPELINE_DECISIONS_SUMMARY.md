# Robust Data Pipeline - Complete Project Documentation

## Table of Contents
1. [Project Overview](#project-overview)
2. [Architecture & Design](#architecture--design)
3. [Database Schema](#database-schema)
4. [Configuration Management](#configuration-management)
5. [Pipeline Phases](#pipeline-phases)
6. [Execution Flow](#execution-flow)
7. [Error Handling & Retry Strategy](#error-handling--retry-strategy)
8. [User Scripts Interface](#user-scripts-interface)
9. [Framework Components](#framework-components)
10. [Airflow Integration](#airflow-integration)
11. [Monitoring & Operations](#monitoring--operations)
12. [Deployment Guide](#deployment-guide)
13. [Troubleshooting](#troubleshooting)
14. [FAQ](#faq)

---

# Project Overview

## Executive Summary

This is a production-grade, plug-and-play data pipeline framework designed to support incremental data loads from multiple source systems (Elasticsearch, ServiceNow, MySQL, etc.) through a staging layer to a Snowflake target data warehouse. The framework is built on Apache Airflow for orchestration and uses Snowflake as the central tracking and execution platform.

## Key Features

- **Incremental Data Loading**: Process data in configurable time windows (hourly, daily, custom intervals)
- **Plug-and-Play Architecture**: New pipelines can be deployed by providing configuration and phase scripts
- **Automatic Retry & Continuation**: Failed pipelines resume from the failure point on next execution
- **Comprehensive Execution Tracking**: Central drive table tracks all pipeline metadata and phase execution details
- **Flexible Phase Configuration**: Enable/disable phases per pipeline, customize behavior via configuration
- **Bulk Data Validation**: Count-based validation with configurable tolerances at each stage
- **Automatic Cleanup & Archival**: Optional stage and target data cleanup with archive capabilities
- **Audit Trail**: Complete execution history stored in Snowflake for compliance and debugging

## Project Goals

1. Eliminate manual data loading and transformation processes
2. Provide consistent, repeatable pipeline execution across all data sources
3. Enable rapid deployment of new data pipelines (days, not weeks)
4. Maintain comprehensive audit trail for regulatory compliance
5. Ensure data quality through automated validation
6. Minimize storage costs through automatic archival and cleanup
7. Support easy troubleshooting and root cause analysis

## Success Criteria

- Deploy new pipeline in less than 1 day (config + scripts only)
- 99%+ pipeline success rate in production
- All failed pipelines automatically retry and complete on next execution
- End-to-end pipeline execution time < expected_run_duration (90% of runs)
- Zero data loss during transfers (with configured tolerance)
- Complete audit trail for every record processed

---

# Architecture & Design

## High-Level Architecture

```
DATA SOURCES → EXTRACTION LAYER → STAGING LAYER → TARGET WAREHOUSE → ANALYTICS/BI

Components:
- Data Sources: Elasticsearch, ServiceNow, MySQL, APIs, etc.
- Extraction Layer: User-provided scripts, handles connectivity and querying
- Staging Layer: Snowflake (CADS_DB schema), temporary storage for validation
- Target Warehouse: Snowflake (CDW_DB schema), final transformed data
- Orchestration: Apache Airflow, manages phase execution and retries
- Execution Tracking: Drive table in Snowflake, centralized state management
```

## Core Concepts

### Pipeline
A complete end-to-end data flow for one specific data source to one specific target. Example: "genetic_tests" pipeline processes genetic test data from Elasticsearch to Snowflake CDW.

### Pipeline Instance
A specific execution of a pipeline for a particular query window. Example: The genetic_tests pipeline running for query window 2025-11-15 10:00:00 to 2025-11-15 11:00:00.

### Query Window
The time slice of data being processed in a pipeline run. Example: 1 hour of data (10:00 to 11:00), 1 day of data (00:00 to 23:59), etc. Determined by pipeline configuration.

### Phase
One of 6-8 sequential steps in the pipeline. Each phase has specific responsibilities and must complete successfully (or be skipped) before proceeding.

### Drive Table
Central Snowflake table (pipeline_execution_drive) that tracks all pipeline executions and phase-level details. One row per pipeline execution.

### Configuration
JSON file containing all static parameters needed by a pipeline (source system details, target system details, phase behavior, etc.). No hardcoding in scripts or DAGs.

## Design Principles

### Idempotency
All operations are designed to be safely repeatable. Running the same phase twice with the same input should produce the same result. This enables safe retries without data duplication or corruption.

### Separation of Concerns
- **Framework**: Core execution logic, state management, Snowflake operations (framework_scripts/)
- **Configuration**: Static parameters, system details, behavior settings (config.json files)
- **User Scripts**: Business-specific logic for each pipeline phase (user_scripts/)
- **Orchestration**: Task sequencing, error handling, retry logic (Airflow DAGs)

### Fail-Fast Pattern
When any phase fails, the entire pipeline stops immediately. This prevents cascading failures and ensures a clear failure point for debugging.

### Observable State
All state is stored centrally in the drive table. At any point, you can query the table to see exactly which phases completed, which failed, and why.

### Configuration-Driven Behavior
Behavior is controlled via configuration, not code. Pipeline phases can be enabled/disabled, tolerances adjusted, cleanup strategies changed—all without modifying any Python scripts or DAGs.

## Data Flow

### Fresh Pipeline Execution

```
1. DAG Triggered (manual or scheduled)
   ↓
2. Initialize: Create drive table row, set pipeline_status=RUNNING
   ↓
3. Execute Phase 1: Stale Pipeline Handling
   ├─ Detect records stuck "in progress" for too long
   ├─ Quarantine or resolve stale records
   └─ If fails: EXIT DAG, set phase_failed, stop execution
   ↓
4. Execute Phase 2: Pre-Validation
   ├─ Check if this is fresh run or continuation
   ├─ Validate pre-conditions (source accessible, target schema exists, etc.)
   └─ If fails: EXIT DAG
   ↓
5. Execute Phase 3-4: Source → Stage Transfer
   ├─ Source Count: Query source, get baseline count
   ├─ Transfer: Extract and load data to stage table
   ├─ Stage Count: Verify records loaded
   └─ If any fails: EXIT DAG
   ↓
6. Execute Phase 5-6: Stage → Target Transfer
   ├─ Stage to Target: Apply transformations, load to target
   ├─ Target Count: Verify records in target
   └─ If any fails: EXIT DAG
   ↓
7. Execute Phase 7: Audit
   ├─ Compare source_count vs stage_count (apply tolerance)
   ├─ Compare stage_count vs target_count (apply tolerance)
   ├─ If counts out of tolerance: EXIT DAG, phase_failed=audit
   └─ If passes: Log audit details in VARIANT
   ↓
8. Execute Phase 8: Stage Cleaning (if enabled)
   ├─ Archive stage data to S3
   ├─ Delete stage data older than threshold
   ├─ Vacuum stage table
   └─ If fails: EXIT DAG or continue (configurable)
   ↓
9. Execute Phase 9: Target Cleaning (if enabled)
   ├─ Run user-provided cleanup logic
   └─ If fails: EXIT DAG or continue
   ↓
10. Finalize: Set pipeline_end_timestamp, calculate pipeline_run_duration, set pipeline_status=SUCCESS
    ↓
11. DAG completes successfully
```

### Retry Execution (After Previous Failure)

```
1. DAG Triggered (automatic via schedule, or manual retry)
   ↓
2. Initialize: Create NEW drive table row (new pipeline_id), set pipeline_status=RUNNING
   ↓
3. Query previous run from drive table
   ├─ Find phases_completed: [phases that succeeded]
   ├─ Find phase_failed: [phase that failed]
   └─ Find phases_pending: [remaining phases to execute]
   ↓
4. Execute Phase 1: Stale Pipeline Handling
   ├─ Pre-validation will skip this (already completed)
   └─ Skip, mark as SKIPPED
   ↓
5. Execute Phase 2: Pre-Validation
   ├─ Detects previous run data
   ├─ Returns: phases_to_skip = [completed phases from run 1]
   └─ Mark all returned phases as SKIPPED in new run
   ↓
6. SKIP Phases 3-4 (already completed in run 1)
   ├─ Skip source_to_stage_transfer (completed)
   ├─ Skip stage transfer (completed)
   └─ All marked as SKIPPED
   ↓
7. Execute Phase 5-6: Stage → Target Transfer (RETRY)
   ├─ Stage to Target: Attempt transfer again (this previously failed)
   ├─ Target Count: Verify records
   └─ If succeeds: continue to next phase
   ↓
8-11. Execute remaining phases (audit, cleaning, finalize)
    ↓
12. Complete: New drive table row shows successful execution
    ↓
Both drive table rows (run 1 and run 2) visible for audit trail
```

---

# Database Schema

## Drive Table: pipeline_execution_drive

### Purpose
Central repository tracking all pipeline executions. One row per pipeline execution (one row for initial run, one row for each retry).

### Column Categories

#### Identification Columns
- **pipeline_id** (VARCHAR, NOT NULL, PRIMARY KEY)
  - Unique identifier for this pipeline execution
  - Generated at pipeline start
  - Format: Could be {pipeline_name}_{target_date}_{run_number} or UUID
  - Used to link to all phase execution details

- **pipeline_name** (VARCHAR, NOT NULL)
  - Human-readable name of the pipeline
  - Example: "genetic_tests", "lab_results", "customer_data"
  - Used for filtering/grouping in queries

#### Execution Status Columns
- **pipeline_status** (VARCHAR, NOT NULL)
  - Current status of the pipeline execution
  - Values: RUNNING, SUCCESS, FAILED, SKIPPED
  - RUNNING: Pipeline is currently executing phases
  - SUCCESS: All phases completed successfully
  - FAILED: One phase failed, pipeline stopped
  - SKIPPED: All phases were skipped (entire pipeline marked as skipped)

- **pipeline_retry_number** (INTEGER, NOT NULL, DEFAULT 0)
  - Which attempt is this execution
  - 0 = first run, 1 = first retry, 2 = second retry, etc.
  - Helps track retry history

#### Timing Columns
- **pipeline_start_timestamp** (TIMESTAMP_TZ, NOT NULL)
  - When the pipeline execution started
  - Set when drive table row is created
  - Example: 2025-11-15 10:00:00.000 UTC

- **pipeline_end_timestamp** (TIMESTAMP_TZ, NULL initially)
  - When the pipeline execution completed (successfully or with failure)
  - NULL while pipeline is RUNNING
  - Set to current timestamp when pipeline finishes
  - Example: 2025-11-15 10:45:30.123 UTC

- **pipeline_run_duration** (VARCHAR, NULL initially)
  - Human-readable format showing total execution time
  - Calculated as: pipeline_end_timestamp - pipeline_start_timestamp
  - Format: "1h 30m 45s", "45m", "2h", etc.
  - NULL while pipeline is RUNNING
  - Useful for monitoring and SLA tracking

#### Query Window Columns (Incremental Load Tracking)
- **query_window_start_timestamp** (TIMESTAMP_TZ, NOT NULL)
  - Start of the time window being processed
  - Example: 2025-11-15 10:00:00 UTC (for hourly load)
  - Example: 2025-11-15 00:00:00 UTC (for daily load)
  - Defines what data is being extracted from source

- **query_window_end_timestamp** (TIMESTAMP_TZ, NOT NULL)
  - End of the time window being processed
  - Example: 2025-11-15 11:00:00 UTC (for hourly load)
  - Example: 2025-11-16 00:00:00 UTC (for daily load)
  - Defines upper boundary of data extraction

- **target_date** (DATE, NOT NULL)
  - Business date for this pipeline execution
  - Derived as: DATE(query_window_start_timestamp)
  - Example: 2025-11-15
  - Used for partitioning and filtering in queries

- **query_window_duration** (VARCHAR, NOT NULL)
  - Human-readable format showing time window size
  - Calculated as: query_window_end_timestamp - query_window_start_timestamp
  - Format: "1h", "24h", "1d", "30m", etc.
  - Example values: "1h" for hourly pipeline, "1d" for daily pipeline
  - Helps validate correct window size

#### Phase Execution Columns (VARIANT - Semi-structured JSON)

Each phase has a VARIANT column storing execution metadata. All VARIANT columns follow the same structure.

- **stale_pipeline_handling_phase** (VARIANT)
- **pre_validation_phase** (VARIANT)
- **source_to_stage_transfer_phase** (VARIANT)
- **stage_to_target_transfer_phase** (VARIANT)
- **audit_phase** (VARIANT)
- **stage_cleaning_phase** (VARIANT)
- **target_cleaning_phase** (VARIANT)

**VARIANT Content Structure (applicable to all phase columns):**

```
Base Structure:
{
    "start_timestamp": ISO 8601 timestamp when phase started,
    "end_timestamp": ISO 8601 timestamp when phase ended,
    "actual_run_duration": Human-readable duration (e.g., "5m 23s"),
    "retry_number": Integer, which retry attempt this is (0 for first attempt),
    "error_message": String or null, error details if phase failed,
    "status": String, one of [COMPLETED, FAILED, SKIPPED, PENDING],
    "expected_run_duration": Human-readable duration from config,
    "phase_specific_details": Object with phase-specific metadata
}
```

**Phase-Specific Details for Each Phase:**

- **stale_pipeline_handling_phase** additional details:
  - records_resolved: number of stale records handled
  - stale_records_found: count of records that were stuck in progress
  - timeout_minutes: threshold used to identify stale records

- **pre_validation_phase** additional details:
  - is_fresh_run: boolean, true if first execution for this window
  - previous_phases_completed: array of phase names already completed
  - validation_checks: array of validation checks performed and results

- **source_to_stage_transfer_phase** additional details:
  - source_count: number of records at source
  - stage_count: number of records loaded to stage
  - records_failed: count of records that failed during transfer
  - transfer_status: SUCCESS or FAILED

- **stage_to_target_transfer_phase** additional details:
  - stage_count: source count before this phase
  - target_count: records loaded to target
  - records_failed: records that failed
  - transformations_applied: description of transformations

- **audit_phase** additional details:
  - source_count: count at source system
  - stage_count: count at stage
  - target_count: count at target
  - source_to_stage_loss_percent: percentage loss from source to stage
  - source_to_stage_tolerance_percent: configured tolerance threshold
  - stage_to_target_loss_percent: percentage loss from stage to target
  - stage_to_target_tolerance_percent: configured tolerance threshold
  - validation_status: PASSED or FAILED
  - loss_reasons: explanation of why data was lost (validation failures, duplicates, etc.)

- **stage_cleaning_phase** additional details:
  - records_archived: count of records archived
  - records_deleted: count of records deleted
  - archive_location: where data was backed up
  - vacuumed: boolean, whether table was vacuumed

- **target_cleaning_phase** additional details:
  - cleaning_type: type of cleanup performed
  - records_affected: count of records modified
  - transformations: description of transformations applied

#### Phase Summary Columns (Arrays - Denormalized for Quick Queries)

These ARRAY columns provide quick answers without parsing JSON.

- **phases_completed** (ARRAY(VARCHAR))
  - Array of phase names that completed successfully
  - Examples: ["stale_pipeline_handling", "pre_validation", "source_to_stage_transfer"]
  - Used for: Quick query to see which phases succeeded
  - Query example: "WHERE ARRAY_CONTAINS('audit'::VARIANT, phases_completed)" to find runs where audit completed

- **phases_skipped** (ARRAY(VARCHAR))
  - Array of phase names that were skipped
  - Examples: ["source_to_stage_transfer", "stage_to_target_transfer"] (skipped because previous run already completed them)
  - Used for: Understanding retry execution flow
  - Query example: Count how many times each phase was skipped

- **phase_failed** (VARCHAR, NULL if all succeeded)
  - Name of the phase that failed
  - Example: "stage_to_target_transfer"
  - NULL if no phase failed (pipeline succeeded)
  - Used for: Identifying failure point quickly
  - Query example: "WHERE phase_failed IS NOT NULL" to find failed pipelines

- **phases_pending** (ARRAY(VARCHAR))
  - Array of phases that haven't executed yet (still pending)
  - For completed runs: should be empty
  - For running pipelines: shows what's left to execute
  - Used for: Monitoring pipeline progress
  - Query example: Monitor real-time pipeline progress

#### Metadata Columns

- **created_at** (TIMESTAMP_TZ, NOT NULL, DEFAULT CURRENT_TIMESTAMP())
  - When this drive table row was created
  - Set automatically by Snowflake
  - Used for: Audit trail, data retention policies

- **updated_at** (TIMESTAMP_TZ, NOT NULL, DEFAULT CURRENT_TIMESTAMP())
  - When this row was last updated
  - Updated each time a phase completes
  - Used for: Understanding when row was last modified

## Drive Table Queries

### Query 1: Find Latest Pipeline Execution
```
Purpose: Get the most recent run of a specific pipeline
Use case: Dashboard showing current pipeline status
Filter by: pipeline_name, order by created_at DESC, limit 1
```

### Query 2: Find Failed Pipelines in Last 24 Hours
```
Purpose: Identify which pipelines failed recently
Use case: Alerting, incident response
Filter by: pipeline_status = 'FAILED' AND created_at > NOW() - 24 hours
```

### Query 3: Find Which Phase Failed
```
Purpose: For a specific failed pipeline, identify the failure point
Use case: Root cause analysis
Filter by: pipeline_id, get phase_failed column
Join with phase VARIANT to get error details
```

### Query 4: Compare Run Duration to Expected Duration
```
Purpose: Identify slow-running phases
Use case: Performance monitoring, capacity planning
Calculate: Compare actual_run_duration vs expected_run_duration for each phase
Alert if: actual > 2x expected
```

### Query 5: Audit Trail for a Specific Pipeline
```
Purpose: Get complete execution history for one pipeline
Use case: Compliance, debugging
Filter by: pipeline_name, order by target_date DESC
Shows: All executions, retries, phase results, audit details
```

### Query 6: Data Freshness Check
```
Purpose: Find when last successful pipeline run completed
Use case: Monitoring data staleness
Filter by: pipeline_name, pipeline_status = 'SUCCESS', order by created_at DESC
Alert if: Last successful run > X hours ago
```

---

# Configuration Management

## Configuration File (config.json)

### Purpose
All static, deployment-time parameters for a pipeline are defined in a single JSON file. This file is the source of truth for pipeline behavior.

### Location
Located at: `projects/{pipeline_name}/config.json`

Example path: `projects/genetic_tests/config.json`

### Why JSON?
- Human-readable and easy to edit
- Hierarchical structure supports nested settings
- No special tools needed to view/edit
- Can be version controlled
- Can be validated against JSON schema
- Easy to load in Python or other languages

### Top-Level Sections

#### 1. Pipeline Metadata Section
**Purpose:** Information about the pipeline and its ownership

**Contains:**
- pipeline_name: Unique name for this pipeline (used in drive table, logs, monitoring)
- pipeline_description: Human-readable description of what this pipeline does
- owner_name: Name of the person responsible for this pipeline
- owner_email: Email address of primary owner
- notification_emails: List of email addresses to notify on failure
- slack_channel: Slack channel for alerts and notifications (e.g., #data-alerts)

**Example:**
```
Pipeline Name: genetic_tests
Owner: John Doe
Emails: john.doe@company.com, data-team@company.com
Slack: #data-pipeline-alerts

This section allows operations team to quickly identify who to contact about this pipeline.
```

#### 2. DAG Schedule Section
**Purpose:** Determines when Airflow triggers this pipeline

**Contains:**
- frequency: How often to run (hourly, daily, weekly, custom)
- cron_expression: Standard Unix cron expression for timing
- timezone: Timezone for cron interpretation (UTC recommended)
- start_date: When to begin scheduling runs
- catchup: Whether to backfill historical missed runs (true/false)

**Example:**
```
Frequency: hourly
Cron: "0 * * * *" (every hour, at minute 0)
Timezone: UTC
Start Date: 2025-11-15
Catchup: false

This means: Run every hour starting Nov 15. Don't backfill missing runs if Airflow was down.
```

**Common Cron Expressions:**
- "0 * * * *" = Every hour at minute 0 (hourly)
- "0 0 * * *" = Every day at midnight (daily)
- "0 2 * * 0" = Every Monday at 2 AM (weekly)
- "0 8 * * MON-FRI" = Every weekday at 8 AM (business days)
- "*/15 * * * *" = Every 15 minutes

#### 3. Query Window Section
**Purpose:** Defines what time slice of data this pipeline processes

**Contains:**
- type: The window type (hourly, daily, custom)
- duration: How much data to process in each run

**Example:**
```
Type: hourly
Duration: 1h

This means: Each pipeline run processes 1 hour of data.
If run triggers at 2025-11-15 10:00, it processes data from 10:00 to 11:00.
```

**Duration Values:**
- For hourly pipelines: "1h"
- For daily pipelines: "1d" or "24h"
- For 30-minute windows: "30m"
- For 15-minute windows: "15m"

#### 4. Source System Section
**Purpose:** Tells pipeline where to extract data from

**Contains:**
- type: Source system type (elasticsearch, servicenow, mysql, api, etc.)
- endpoint: Connection URL/address for source
- index/table: What to query in the source
- query_field: Timestamp field used to identify data window
- batch_size: How many records to fetch at once
- authentication: Credentials reference (stored separately in secrets)

**Example for Elasticsearch:**
```
Type: elasticsearch
Endpoint: https://elasticsearch.company.com:9200
Index: genetic_tests_v1
Query Field: @timestamp
Batch Size: 5000

This tells the source_count script where to connect and what to query.
```

**Example for MySQL:**
```
Type: mysql
Host: mysql-prod.company.com
Port: 3306
Database: source_db
Table: genetic_tests
Query Field: created_timestamp
Batch Size: 10000
```

#### 5. Stage System Section
**Purpose:** Temporary storage in Snowflake for data validation

**Contains:**
- type: Always "snowflake" for our pipelines
- database: Snowflake database name (usually CADS_DB)
- schema: Snowflake schema name (e.g., stg_genetic_tests)
- table: Table name where stage data goes
- temp_table_prefix: Prefix for temporary tables used during loading

**Purpose of Stage:**
- Temporary landing zone for extracted data
- Allows validation before loading to final target
- Data isolated by query_window for lineage tracing
- Can be archived and deleted after successful target load

#### 6. Target System Section
**Purpose:** Final destination for processed data

**Contains:**
- type: Always "snowflake"
- database: Snowflake database (usually CDW_DB or data warehouse name)
- schema: Target schema (e.g., prod_genetic_tests)
- table: Final table name where data lives

**Note:** This is the source of truth. Data here is permanent. Query_window is maintained here for lineage.

#### 7. Phase Configuration Section
**Purpose:** Behavior settings for each of the 6-8 pipeline phases

**Per-Phase Settings:**
- enabled: true/false, whether this phase should execute
- expected_run_duration: Baseline expected duration (used for monitoring alerts)
- phase-specific parameters

**Stale Pipeline Handling Phase:**
- timeout_minutes: How long a record can be "in progress" before considered stale
  - Example: 120 means records stuck for >2 hours are stale
  - Used to detect stuck/dead pipeline runs and restart data

**Pre-Validation Phase:**
- Just enabled flag
- No additional config (logic is standard)

**Source-to-Stage Transfer & Stage-to-Target Transfer:**
- Just enabled flag and expected_run_duration
- Business logic in user scripts determines actual behavior

**Audit Phase:**
- enabled: true/false
- expected_run_duration: How long audit should take
- count_tolerances: Object containing tolerance percentages
  - source_to_stage_tolerance_percent: Acceptable data loss from source to stage
    - Example: 2.0 means 2% data loss allowed (accounts for validation failures)
  - stage_to_target_tolerance_percent: Acceptable data loss from stage to target
    - Example: 1.0 means 1% data loss allowed (accounts for deduplication)

**Stage Cleaning Phase:**
- enabled: true/false (usually true)
- expected_run_duration: Expected duration of cleanup
- archive_before_delete: true/false, whether to backup before deletion
  - true = Archive to S3 before deleting local data
  - false = Just delete, no backup
- archive_location: S3 path where backups stored
  - Example: "s3://company-archive/stage-backups/"
  - Data partitioned by target_date and pipeline_name
- delete_after_days: How many days to keep stage data before deletion
  - Example: 7 means keep stage data for 7 days, then delete
  - Rationale: Keep for debugging if issues arise, then cleanup

**Target Cleaning Phase:**
- enabled: false by default (only enable if needed)
- expected_run_duration: Expected duration
- description: What cleanup to perform (custom per pipeline)

#### 8. Retry Configuration Section
**Purpose:** How to behave when a phase fails

**Contains:**
- max_retries: Maximum number of retry attempts (typically 3)
- backoff_strategy: Wait strategy between retries
  - exponential: 1s, 2s, 4s, 8s wait between attempts
  - linear: same wait each time (e.g., always 5s)
  - none: no wait between retries (fast but can overwhelm systems)

**Example:**
```
Max Retries: 3
Backoff: exponential

If a phase fails:
Attempt 1: FAILS, wait 1 second
Attempt 2: FAILS, wait 2 seconds
Attempt 3: FAILS, wait 4 seconds
Attempt 4: FAILS, give up

Why exponential? If issue is temporary (server overload, network glitch),
exponential backoff gives system time to recover.
```

#### 9. Monitoring Section
**Purpose:** Alerting thresholds for operational monitoring

**Contains:**
- alert_on_failure: true/false, send alert if pipeline fails
- alert_on_duration_exceed: true/false, send alert if phase takes too long
- duration_multiplier_threshold: Factor to trigger duration alerts
  - Example: 2.0 means alert if actual > 2x expected_run_duration

**Example:**
```
Alert On Failure: true
Alert On Duration: true
Duration Threshold: 2.0

If phase expected 10m but takes 21+ minutes, send alert.
This helps catch performance degradation early.
```

## Configuration Best Practices

### Immutability During Pipeline Run
Once a pipeline run starts, its configuration is frozen. Configuration changes only apply to future runs. This ensures reproducibility—if you change config, you can re-run with new config, but the old run used the old config.

### Validation Before Deployment
Before deploying a new pipeline or changing configuration:
1. Validate config against JSON schema
2. Verify all referenced systems are accessible
3. Test with sample data
4. Dry-run pipeline without committing data

### Version Control
Store config.json files in git with full history. Track changes to understand why each setting is what it is.

### Documentation
Include comments in config explaining:
- Why tolerances are set to specific values
- Why batch_size is set to that value
- Any performance tuning done
- Owner contact info if unusual setup

### Secrets Management
Database credentials, API keys, etc. should NOT be in config.json. Store in:
- HashiCorp Vault
- AWS Secrets Manager
- Airflow Connections/Variables
- Config references like: `"credentials_ref": "snowflake_service_account"`

---

# Pipeline Phases

## Phase Overview

A pipeline consists of 6 mandatory phases plus 2 optional phases. Phases execute sequentially. Each phase has specific responsibilities and expected behavior.

### Execution Sequence

```
1. Stale Pipeline Handling (mandatory) ← First, handle any stuck records
   ↓
2. Pre-Validation (mandatory) ← Validate pre-conditions
   ↓
3. Source Count (data extraction begins)
   ↓
4. Source-to-Stage Transfer (mandatory) ← Extract from source
   ↓
5. Stage Count
   ↓
6. Stage-to-Target Transfer (mandatory) ← Transform and load to target
   ↓
7. Target Count
   ↓
8. Audit (mandatory) ← Validate counts match expectations
   ↓
9. Stage Cleaning (optional, usually enabled) ← Cleanup temporary data
   ↓
10. Target Cleaning (optional, user-requested) ← Arbitrary transformations
    ↓
Complete
```

## Phase 1: Stale Pipeline Handling

### Purpose
Detect and handle records that have been stuck in "in progress" state for too long. Prevents data loss from dead/hung pipeline processes.

### Why It Matters
If a previous pipeline run crashed while processing, it might have left records marked as "in progress". If not cleaned up, those records are never processed. This phase detects and resolves that situation.

### What It Does
1. Queries the stage table for records where status = 'in_progress' AND modified_timestamp < (now - timeout_minutes)
2. These are stale records that have been processing for too long
3. Actions on stale records (user-defined):
   - Mark as 'failed' for manual review
   - Mark as 'pending' to retry processing
   - Archive to dead-letter queue
4. Returns count of stale records resolved

### Configuration Parameters
- enabled: true/false
- timeout_minutes: Minutes to wait before considering a record stale
  - Example: 120 (2 hours) - if in_progress for >2 hours, it's stale
- expected_run_duration: How long this phase should take

### Expected Duration
Usually very quick (seconds to minutes) unless millions of stale records.

### When It Might Fail
- Stage table is corrupt or inaccessible
- Permission issues updating stage table
- Stale records cannot be moved to quarantine location

### Retry Behavior
On next run, pre-validation will skip this phase (already completed in previous run).

---

## Phase 2: Pre-Validation

### Purpose
Validate that all pre-conditions are met and determine whether this is a fresh run or continuation from a previous failure.

### What It Does

#### Check 1: Is This Fresh or Continuation?
- Queries drive table for previous runs of this pipeline with same query_window
- If previous run exists AND phase_failed is not null:
  - This is a continuation run
  - Determine which phases need to be skipped
  - Return phases_to_skip list
- If no previous run:
  - This is fresh run
  - Return phases_to_skip = []

#### Check 2: Validate Prerequisites
- Source system accessible (connection successful)
- Source has data for this query_window
- Stage table schema exists
- Stage table is writable
- Target table schema exists
- Target table structure matches expectations

#### Check 3: Detect Data Window
- If fresh run: use query_window from config (e.g., now - now-1h for hourly)
- If continuation: use same query_window from previous run (ensure consistency)

### Configuration Parameters
- enabled: true/false (always true)
- expected_run_duration: Expected duration

### Expected Duration
Seconds to low minutes (just validation checks).

### When It Might Fail
- Source system unreachable
- Stage table corrupt or missing
- Target table missing
- Query window has no data in source

### Retry Behavior
On retry run, this phase determines what to skip and marks skipped phases in the new drive table row.

---

## Phase 3: Source Count

### Purpose
Query source system and get baseline count of records in the query window.

### What It Does
1. Connect to source system (Elasticsearch, MySQL, ServiceNow, etc.)
2. Query for all records where timestamp >= query_window_start AND timestamp < query_window_end
3. Count total records matching criteria
4. Return count

### Configuration Parameters
- Query Field: Which timestamp field to use for windowing
- Batch Size: How many records to process per batch
- Connection details from source_system section

### Expected Duration
Depends on source system. Usually seconds to minutes.

### When It Might Fail
- Source unreachable
- Query malformed
- Source has no data for window

### Return Value
- count: Total number of records found at source
- Stored in audit_phase VARIANT for later validation

### Example Flows
```
Elasticsearch source:
- Connect to ES cluster
- Query: @timestamp >= 2025-11-15T10:00:00Z AND @timestamp < 2025-11-15T11:00:00Z
- Get count of matching documents
- Return 10,000

MySQL source:
- Connect to MySQL database
- Query: SELECT COUNT(*) FROM genetic_tests WHERE created_timestamp >= '2025-11-15 10:00:00' AND created_timestamp < '2025-11-15 11:00:00'
- Return 10,000
```

---

## Phase 4: Source-to-Stage Transfer

### Purpose
Extract data from source and load into stage table for validation.

### What It Does
1. Connect to source system
2. Execute query to extract records matching query_window
3. Validate each record (schema, required fields, data types)
4. Rejected records logged with rejection reason
5. Valid records loaded to stage table
6. Commit transaction when batch complete
7. Repeat until all records processed

### Validation Rules
- All required fields present
- Data types match schema (dates are dates, numbers are numbers, etc.)
- No null values in NOT NULL fields
- String lengths within limits
- Date/timestamp values within reasonable ranges

### Configuration Parameters
- Batch Size: Number of records per commit
- Stage Table: Target stage table name
- Transformation Rules: Any data cleanup/normalization

### Expected Duration
Depends on data volume and source performance. Usually minutes.

### When It Might Fail
- Source query fails
- Stage table full or quota exceeded
- Network connection dropped
- Data validation rejects too many records (quota threshold exceeded)

### Idempotency
This phase is idempotent. Running it twice loads the same data. If stage already has data for this query_window:
- Option 1: Delete existing data first, then reload
- Option 2: Merge (add new records not already present)
- Handled by configuration/user script logic

### Monitoring Points
- Records extracted vs records accepted
- Rejection rate (if > threshold, might indicate data quality issue)
- Load rate (records per second)

---

## Phase 5: Stage Count

### Purpose
Verify that records loaded to stage match expectations.

### What It Does
1. Query stage table for records with this query_window (target_date and timestamp range)
2. Count total records loaded
3. Compare to source_count from Phase 3
4. Calculate loss percentage: (source_count - stage_count) / source_count * 100
5. Store count in audit_phase VARIANT

### Configuration Parameters
- Stage Table: Which table to query
- Query Window: Time range for this load

### Expected Duration
Seconds (just counting).

### When It Might Fail
- Stage table corrupt or inaccessible
- Records were deleted between phase 4 and now (shouldn't happen)
- Stage schema changed unexpectedly

### Result
- If stage_count < source_count: Some records failed validation (expected)
- If stage_count = source_count: Perfect, all records loaded
- If stage_count > source_count: Unexpected, indicates duplicate insertion (error)

---

## Phase 6: Stage-to-Target Transfer

### Purpose
Apply business transformations and load validated data from stage to target table.

### What It Does
1. Read data from stage table (all records for this query_window)
2. Apply transformations (user-defined in script):
   - Rename columns
   - Change data types
   - Combine/split fields
   - Apply business logic (calculations, lookups, etc.)
   - Deduplicate
   - Filter out unwanted records
3. Load transformed data to target table
4. Commit transaction

### Transformations (Examples)
- Standardize date formats
- Parse nested JSON fields
- Join with lookup tables
- Calculate derived metrics
- Rename columns for target schema
- Aggregate by time period

### Configuration Parameters
- Target Table: Where to load data
- Transformation Rules: Custom transformation logic
- Deduplicate Logic: How to handle duplicates

### Expected Duration
Depends on data volume and transformation complexity. Usually minutes.

### When It Might Fail
- Target table schema mismatch
- Foreign key constraint violation
- Transformation code error
- Target quota exceeded

### Idempotency
Like source→stage, this should be idempotent. Running twice should produce same result. Handled by:
- Delete existing target data for this query_window before reload, OR
- Upsert logic (update if exists, insert if not)

### Monitoring Points
- Records transformed vs records loaded
- Transformation rate (records per second)
- Dropped records (filtered/deduplicated)

---

## Phase 7: Target Count

### Purpose
Verify records loaded to target table.

### What It Does
1. Query target table for records with this query_window
2. Count total records
3. Store count for audit phase comparison

### Expected Duration
Seconds (just counting).

### Result Stored For
Audit phase uses this count to validate stage→target transfer success.

---

## Phase 8: Audit

### Purpose
Validate data integrity across all stages. Ensure data wasn't lost or corrupted during transfer.

### What It Does

#### Count Comparison 1: Source → Stage
1. Get source_count from Phase 3
2. Get stage_count from Phase 5
3. Calculate loss: (source_count - stage_count) / source_count * 100
4. Compare to source_to_stage_tolerance_percent from config
5. If loss <= tolerance: PASS
6. If loss > tolerance: FAIL, set skip_dag_run = True

#### Count Comparison 2: Stage → Target
1. Get stage_count from Phase 5
2. Get target_count from Phase 7
3. Calculate loss: (stage_count - target_count) / stage_count * 100
4. Compare to stage_to_target_tolerance_percent from config
5. If loss <= tolerance: PASS
6. If loss > tolerance: FAIL, set skip_dag_run = True

#### Audit Logging
1. Store all counts, percentages, and tolerances in audit_phase VARIANT
2. Document why data was lost (validation failures, deduplication, filtering, etc.)
3. Visible in drive table for later analysis

### Configuration Parameters
- source_to_stage_tolerance_percent: Maximum acceptable loss from source to stage
  - Example: 2.0 (2% loss allowed)
  - Rationale: Accounts for validation failures, bad data, missing fields
- stage_to_target_tolerance_percent: Maximum acceptable loss from stage to target
  - Example: 1.0 (1% loss allowed)
  - Rationale: Accounts for deduplication, constraint violations, filtering

### Expected Duration
Seconds to minutes (counting and comparison).

### When It Might Fail
- Data loss exceeds configured tolerance
- Count queries fail (table inaccessible)

### Why Tolerances?
- Some data loss is expected and acceptable
- Validation failures are good (catching bad data)
- Deduplications are good (ensuring data quality)
- Tolerances let pipeline succeed with expected loss but fail on unexpected loss

### Example Outcomes

**Scenario 1: Successful Audit**
```
Source: 10,000 records
Stage: 9,800 records (2% loss from validation failures)
Target: 9,800 records (0% loss from stage)
Config tolerances: 2% source→stage, 1% stage→target
Result: PASSED ✓
```

**Scenario 2: Failed Audit - Excessive Loss**
```
Source: 10,000 records
Stage: 8,000 records (20% loss - unexpected!)
Target: N/A (audit failed, didn't get here)
Config tolerance: 2% source→stage
Result: FAILED ✗
Reason: Loss 20% exceeds tolerance 2%
Action: Pipeline stops, phase_failed = 'audit', skip_dag_run = True
```

---

## Phase 9: Stage Cleaning

### Purpose
Clean up temporary stage data after successful transfer to target. Manages storage costs and compliance.

### What It Does

#### If archive_before_delete = true (recommended)
1. Query stage table for records older than delete_after_days
2. Export these records to Snowflake stage (temp file storage)
3. Copy from Snowflake stage to S3 archive location
4. Partition archived data by target_date
5. Compress to Parquet format (efficient storage)
6. Record archive location and date

#### Delete Old Records
1. Delete from stage table WHERE target_date < (current_date - delete_after_days)
2. Commit deletion

#### Table Maintenance
1. Run VACUUM on stage table (removes deleted row versions)
2. Gather table statistics (helps query optimizer)
3. Done

### Configuration Parameters
- enabled: true/false (usually true)
- archive_before_delete: true/false
- archive_location: S3 path (e.g., s3://company-archive/stage-backups/)
- delete_after_days: How long to keep stage data (e.g., 7)

### Expected Duration
Minutes (depends on data volume).

### When It Might Fail
- S3 permissions issue
- Snowflake stage access issue
- Delete operation fails (lock on table)

### Why Archive Before Delete?
- Compliance: Maintain audit trail for regulatory requirements
- Recovery: If something goes wrong, can restore from archive
- Debugging: Historical data available for analysis

### Storage Impact
- Without archive: Stage data gone forever
- With archive: Data moves to cheaper S3 cold storage
- Net result: Stage stays clean, history maintained cheaply

---

## Phase 10: Target Cleaning (Optional)

### Purpose
Optional, user-requested transformations on target data. Only enabled if pipeline needs additional cleanup.

### What It Does
- Runs custom logic provided by pipeline owner
- Examples:
  - Deduplicate records in target
  - Archive old records to history table
  - Aggregate/summarize data
  - Soft-delete inactive records
  - Apply retention policies

### When Needed
- Target table accumulates stale records
- Need to apply transformations that can't be done at load time
- Data quality cleanup needed periodically
- Compliance requirements (right to be forgotten, data deletion, etc.)

### Configuration Parameters
- enabled: true/false (default false)
- transformation_description: What cleanup to perform

### Expected Duration
Depends on custom logic. Could be minutes to hours.

### When It Might Fail
- Custom transformation code errors
- Constraint violations
- Timeout on large operations

### Difference from Stage Cleaning
- Stage Cleaning: Automatic, removes temporary data, archives for compliance
- Target Cleaning: Optional, transforms production data, user-controlled

---

# Execution Flow

## Complete Fresh Execution Example

### Scenario
Pipeline "genetic_tests" is triggered for the first time for query window 2025-11-15 10:00 to 11:00.

### Step-by-Step Execution

#### 1. Initialization
```
Event: Airflow DAG triggered (by scheduler or manual trigger)
Action: Framework creates drive table row
  - pipeline_id: "genetic_tests_20251115_10h_run1"
  - pipeline_name: "genetic_tests"
  - pipeline_status: RUNNING
  - pipeline_start_timestamp: 2025-11-15 10:00:01 UTC
  - query_window_start_timestamp: 2025-11-15 10:00:00 UTC
  - query_window_end_timestamp: 2025-11-15 11:00:00 UTC
  - target_date: 2025-11-15
  - query_window_duration: "1h"
  - phases_pending: [stale_handling, pre_validation, source_count, source_to_stage_transfer, stage_count, stage_to_target_transfer, target_count, audit, stage_cleaning]
  - phases_completed: []
  - phase_failed: NULL
```

#### 2. Phase 1: Stale Pipeline Handling Executes
```
Event: User script called: user_scripts/stale_pipeline_handling.py
Input: config (from config.json), record (drive table row)
Logic:
  - Load config.timeout_minutes = 120 (2 hours)
  - Query stage_genetic_tests WHERE status='in_progress' AND modified_at < (now - 120 min)
  - Find 0 stale records
  - Return: {skip_dag_run: False, error_message: None, records_resolved: 0}
Action: Phase successful
  - Update VARIANT stale_pipeline_handling_phase:
    - status: COMPLETED
    - start_timestamp: 2025-11-15 10:00:01
    - end_timestamp: 2025-11-15 10:00:05
    - actual_run_duration: "4s"
  - Add "stale_pipeline_handling" to phases_completed
  - Remove "stale_pipeline_handling" from phases_pending
  - Continue to next phase
```

#### 3. Phase 2: Pre-Validation Executes
```
Event: User script called: user_scripts/pre_validation.py
Input: config, record
Logic:
  - Check for previous runs with same query_window
    - Query drive table WHERE pipeline_name='genetic_tests' AND target_date='2025-11-15'
    - Find 0 previous runs (this is fresh run)
  - Validate prerequisites:
    - Connect to Elasticsearch: SUCCESS ✓
    - Query genetic_tests index for data in window: Found 10,000 records ✓
    - Connect to CADS_DB.stg_genetic_tests: SUCCESS ✓
    - Connect to CDW_DB.prod_genetic_tests: SUCCESS ✓
  - Return: {skip_dag_run: False, error_message: None, is_fresh_run: True, phases_to_skip: []}
Action: Phase successful
  - Update VARIANT pre_validation_phase: status COMPLETED
  - Add to phases_completed
  - Continue to next phase
```

#### 4. Phase 3: Source Count Executes
```
Event: User script called: user_scripts/source_count.py
Input: config, record
Logic:
  - Connect to Elasticsearch
  - Query: GET genetic_tests_v1/_count?q=@timestamp:[2025-11-15T10:00:00Z TO 2025-11-15T11:00:00Z}
  - Get result: 10,000 documents
  - Return: {skip_dag_run: False, error_message: None, count: 10000}
Action: Phase successful
  - Store count in audit phase VARIANT for later
  - Update phases_completed
  - Continue
```

#### 5. Phase 4: Source-to-Stage Transfer Executes
```
Event: User script called: user_scripts/source_to_stage_transfer.py
Input: config, record
Logic:
  - Connect to Elasticsearch
  - Batch fetch records (5000 per batch)
  - Batch 1: Fetch records 0-4999
    - Validate each: check schema, required fields, data types
    - 4950 records valid, 50 invalid (rejected)
    - Insert valid records to CADS_DB.stg_genetic_tests
  - Batch 2: Fetch records 5000-9999
    - Validate: 4850 valid, 150 invalid (rejected)
    - Insert valid records
  - All records processed
  - Total inserted: 9800 records
  - Return: {skip_dag_run: False, error_message: None, transfer_completed: True}
Action: Phase successful
  - Update VARIANT source_to_stage_transfer_phase
  - Update phases_completed
  - Continue
```

#### 6. Phase 5: Stage Count Executes
```
Event: User script called: user_scripts/stage_count.py
Input: config, record
Logic:
  - Connect to Snowflake CADS_DB
  - Query: SELECT COUNT(*) FROM stg_genetic_tests WHERE target_date = '2025-11-15' AND timestamp >= '2025-11-15T10:00:00Z' AND timestamp < '2025-11-15T11:00:00Z'
  - Get count: 9800
  - Return: {skip_dag_run: False, error_message: None, count: 9800}
Action: Phase successful
  - Store count in audit VARIANT
  - Update phases_completed
  - Continue
```

#### 7. Phase 6: Stage-to-Target Transfer Executes
```
Event: User script called: user_scripts/stage_to_target_transfer.py
Input: config, record
Logic:
  - Connect to CADS_DB.stg_genetic_tests
  - Read all 9800 records for target_date '2025-11-15'
  - Apply transformations:
    - Rename columns to match target schema
    - Parse nested JSON fields
    - Calculate derived metrics
    - Deduplicate (find 5 duplicate records, keep 1 copy each)
    - Result: 9795 transformed records
  - Insert to CDW_DB.prod_genetic_tests
  - Return: {skip_dag_run: False, error_message: None, transfer_completed: True}
Action: Phase successful
  - Update VARIANT stage_to_target_transfer_phase
  - Update phases_completed
  - Continue
```

#### 8. Phase 7: Target Count Executes
```
Event: User script called: user_scripts/target_count.py
Logic:
  - Query: SELECT COUNT(*) FROM CDW_DB.prod_genetic_tests WHERE target_date = '2025-11-15'
  - Get count: 9795
  - Return: {skip_dag_run: False, error_message: None, count: 9795}
Action: Phase successful
  - Store count in audit VARIANT
  - Update phases_completed
```

#### 9. Phase 8: Audit Executes
```
Event: User script called: user_scripts/audit.py
Logic:
  - Get source_count: 10,000
  - Get stage_count: 9,800
  - Get target_count: 9,795
  - Calculate source→stage loss: (10000-9800)/10000 * 100 = 2.0%
  - Load config tolerance: 2.0%
  - Check: 2.0% == 2.0% ✓ PASS
  - Calculate stage→target loss: (9800-9795)/9800 * 100 = 0.05%
  - Load config tolerance: 1.0%
  - Check: 0.05% <= 1.0% ✓ PASS
  - Return: {skip_dag_run: False, error_message: None, audit_passed: True}
Action: Phase successful
  - Update VARIANT audit_phase with counts, losses, tolerances
  - Update phases_completed
  - Continue
```

#### 10. Phase 9: Stage Cleaning Executes
```
Event: User script called: user_scripts/stage_cleaning.py
Logic:
  - Load config: archive_before_delete=true, archive_location="s3://company-archive/", delete_after_days=7
  - Find stage records older than 7 days: 0 records (fresh data)
  - Nothing to delete
  - VACUUM stage table
  - Return: {skip_dag_run: False, error_message: None, cleaning_completed: True}
Action: Phase successful
  - Update VARIANT stage_cleaning_phase
  - Update phases_completed
```

#### 11. Phase 10: Target Cleaning Skipped
```
Event: Check config: target_cleaning.enabled = false
Action: Phase skipped (not enabled)
  - Add to phases_completed but with status SKIPPED
  - Update phases_pending
```

#### 12. Finalization
```
Event: All phases complete
Action: Framework finalizes drive table row
  - pipeline_end_timestamp: 2025-11-15 10:15:30 UTC
  - pipeline_run_duration: "15m 29s"
  - pipeline_status: SUCCESS
  - phases_completed: [all phases]
  - phases_pending: []
  - phase_failed: NULL
Report: Pipeline completed successfully
  - 10,000 records extracted
  - 9,800 loaded to stage (2% validation failure)
  - 9,795 loaded to target (5 duplicates deduplicated)
  - Audit: PASSED ✓
  - Total time: 15 minutes 29 seconds
```

---

## Retry Execution Example

### Scenario
Same pipeline runs again 1 hour later. The stage_to_target_transfer phase failed in the previous run.

### Previous Run Summary (From Drive Table)
```
pipeline_id: "genetic_tests_20251115_10h_run1"
pipeline_status: FAILED
phase_failed: "stage_to_target_transfer"
phases_completed: ["stale_pipeline_handling", "pre_validation", "source_count", "source_to_stage_transfer", "stage_count"]
phases_pending: ["stage_to_target_transfer", "target_count", "audit", "stage_cleaning"]
Error: "Snowflake lock timeout waiting for target table"
```

### Current Retry Run Execution

#### 1. Initialization (New Run)
```
Event: Airflow DAG triggered (scheduled or manual)
Action: Framework creates NEW drive table row for same query_window
  - pipeline_id: "genetic_tests_20251115_10h_run2" (new ID, different run)
  - pipeline_status: RUNNING
  - pipeline_start_timestamp: 2025-11-15 11:05:00 UTC
  - query_window_start_timestamp: 2025-11-15 10:00:00 UTC (SAME as run 1)
  - query_window_end_timestamp: 2025-11-15 11:00:00 UTC (SAME)
  - phases_pending: [all phases initially]
  - phases_completed: []
```

#### 2. Phase 1: Stale Pipeline Handling
```
Event: Execute stale_pipeline_handling.py
Action: Detects no stale records
Result: Completed successfully
Tracking: Added to phases_completed
```

#### 3. Phase 2: Pre-Validation (Key Difference)
```
Event: Execute pre_validation.py
Logic:
  - Query drive table for previous runs of "genetic_tests" with target_date = '2025-11-15'
  - Find run 1: phase_failed = 'stage_to_target_transfer', phases_completed = [5 phases]
  - Detect: This is a CONTINUATION run
  - Return: {phases_to_skip: ["stale_pipeline_handling", "pre_validation", "source_count", "source_to_stage_transfer", "stage_count"]}
Result: Phase successful
Tracking:
  - Mark all returned phases as SKIPPED in current drive table row
  - phases_completed (current run): [stale_pipeline_handling, pre_validation]
  - phases_skipped: [source_count, source_to_stage_transfer, stage_count]
```

#### 4. Phase 3-5: Skipped (Because Pre-Validation Said So)
```
Event: Framework evaluates phases_to_skip list
Action: Skip source_count, source_to_stage_transfer, stage_count
Tracking:
  - Add each to phases_skipped array
  - Update VARIANT for each with status SKIPPED
  - Continue to next phase not in skip list
```

#### 6. Phase 6: Stage-to-Target Transfer (RETRY of Failed Phase)
```
Event: Execute stage_to_target_transfer.py (same code as run 1, but stage data already exists)
Input: config, record (from run 2)
Logic:
  - Check stage table for records with target_date='2025-11-15'
  - Find 9800 records (from run 1, still in stage)
  - Transform: same logic as run 1
  - Attempt insert to target table
  - This time: Snowflake lock released, insert succeeds
  - 9795 records loaded to target
  - Return: {skip_dag_run: False, transfer_completed: True}
Result: Phase successful (retry worked!)
Tracking:
  - Update VARIANT stage_to_target_transfer_phase with COMPLETED status
  - Add to phases_completed
```

#### 7-12. Remaining Phases Execute
```
Similar to fresh execution:
- Phase 7: Target count: 9795 ✓
- Phase 8: Audit: counts match, PASSED ✓
- Phase 9: Stage cleaning: archives and deletes old data ✓
- Phase 10: Target cleaning: skipped (not enabled)
- Finalization: pipeline_status = SUCCESS
```

#### Final State
```
Drive Table Now Contains TWO Rows:
Row 1 (Run 1):
  - pipeline_id: genetic_tests_20251115_10h_run1
  - pipeline_status: FAILED
  - phase_failed: stage_to_target_transfer
  - phases_completed: [5 phases]
  - pipeline_run_duration: "10m 15s"

Row 2 (Run 2):
  - pipeline_id: genetic_tests_20251115_10h_run2
  - pipeline_status: SUCCESS
  - phase_failed: NULL
  - phases_completed: [2 fresh + 5 skipped + 2 new = 9 total]
  - phases_skipped: [5 phases]
  - pipeline_run_duration: "2m 30s"

Complete Audit Trail: Can trace entire journey of 10K records through retries and eventual success.
```

---

# Error Handling & Retry Strategy

## Error Categories

### Category 1: Transient Errors (Temporary, likely to succeed if retried)
- Network connection timeouts (source temporarily unreachable)
- Database locks (waiting for other transaction to complete)
- Rate limiting (API throttling user temporarily)
- Temporary service degradation (server momentarily slow)

**Handling:** Automatic retry with backoff

### Category 2: Permanent Errors (Will fail again even if retried)
- Invalid configuration (wrong connection string)
- Schema mismatch (target table structure changed)
- Permission denied (credentials lack required privileges)
- Data validation failure (data doesn't match schema)

**Handling:** Fail immediately, alert operator, require manual fix

### Category 3: Configuration Errors (User mistake)
- Missing required config field
- Invalid tolerance percentage (> 100%)
- Source system not accessible at deploy time

**Handling:** Validation during deployment prevents execution

## Failure Detection

### How Pipeline Detects Failure

A phase is considered failed when:
1. User script returns `skip_dag_run: True` in output dictionary
2. User script raises an exception/error
3. User script returns error_message that's not None
4. Script execution times out

### Immediate Response

When any of above detected:
1. Current phase status set to FAILED in drive table
2. phase_failed column set to failing phase name
3. skip_dag_run flag causes Airflow to exit DAG
4. All downstream tasks are skipped (won't execute)
5. No data corruption (previous successful phases are complete, this phase is partial/failed)

## Retry Strategy

### Full Pipeline Retry (Not Individual Phase Retry)

When a phase fails:
1. Entire pipeline stops
2. On next execution (next hour, next day, or manual trigger):
   - New pipeline_id generated
   - Pre-validation detects previous failed run
   - Pre-validation identifies which phases completed
   - New run skips completed phases
   - New run starts from failed phase

### Why Not Individual Phase Retry?

**Alternative: Retry failed phase immediately**
- Tempting: Faster, don't waste completed work
- Problem: State management becomes complex
  - What if retry succeeds partially?
  - What if retried phase succeeds but introduces inconsistency?
  - Harder to track which attempt produced final data
  - Easier to debug: one run = one attempt
  
**Chosen: Full pipeline retry on next execution**
- Simpler state management
- Cleaner audit trail
- Easy to debug (each drive table row is complete execution)
- Pre-validation handles continuation logic
- Same data window ensures consistency

### Backoff Strategy

Between retry attempts, wait time increases exponentially to allow system to recover:

```
Attempt 1: FAILS
Wait 1 second (gives system brief moment to recover)
  ↓
Attempt 2: FAILS
Wait 2 seconds (longer wait)
  ↓
Attempt 3: FAILS
Wait 4 seconds (even longer)
  ↓
Attempt 4: FAILS
Max retries reached, give up
```

### Retry Configuration

```
max_retries: 3
backoff_strategy: exponential

This means:
- Try initial execution
- On failure: Retry up to 3 more times (4 total attempts)
- Between attempts: exponential backoff (1s, 2s, 4s)
- After 4 failed attempts: Give up, alert operator
```

### When Retries Happen

**Automatic Retries:**
- Next scheduled DAG run (pipeline runs daily, retries next day)
- Airflow reschedule (if configured to retry same run)

**Manual Retries:**
- Data engineer manually triggers DAG for same query_window
- Can happen immediately after detecting failure
- Can happen after fixing root cause

## Error Propagation

### Scenario: Phase Fails, What Happens?

```
Stage-to-Target Transfer phase fails
  ↓
Phase returns: {skip_dag_run: True, error_message: "Snowflake table lock timeout"}
  ↓
Framework updates drive table:
  - stage_to_target_transfer_phase VARIANT: status = FAILED, error_message = "..."
  - phase_failed = "stage_to_target_transfer"
  - phase_pending = removes "stage_to_target_transfer"
  - pipeline_status = FAILED (no longer RUNNING)
  ↓
Airflow receives skip_dag_run = True
  ↓
Airflow DAG stops (no more tasks execute)
  - Audit phase won't run
  - Cleaning phase won't run
  - Pipeline marked as FAILED
  ↓
Alert sent to team: "Pipeline genetic_tests failed at stage_to_target_transfer"
  ↓
Team investigates: Check logs, identify Snowflake lock issue
  ↓
Team triggers retry: Manually run DAG for same query_window
  ↓
New DAG execution:
  - Pre-validation skips already-completed phases
  - Starts from stage_to_target_transfer
  - Lock is released, phase succeeds
  - Pipeline continues and completes
```

## Idempotent Operations

All phases must be idempotent: running twice produces same result.

### How Each Phase Achieves Idempotency

**Source Count:**
- Just querying source, no side effects
- Running twice returns same count
- Idempotent ✓

**Source-to-Stage Transfer:**
- Option 1: Delete stage data for this query_window first, then load
  - Ensures idempotency: second run produces same state
- Option 2: Check if data already exists, skip if present
  - Avoids re-loading already-loaded data
- Either approach works

**Audit Phase:**
- Just comparing counts, no side effects
- Running twice produces same result
- Idempotent ✓

**Stage Cleaning:**
- Deletes old data (> delete_after_days)
- Running twice: second run finds nothing to delete
- Idempotent ✓

## Failed Phase Investigation

### How to Determine Why Phase Failed

```
Step 1: Check drive table row
SELECT * FROM pipeline_execution_drive 
WHERE pipeline_id = 'genetic_tests_20251115_10h_run1'

Get: phase_failed = 'stage_to_target_transfer'

Step 2: Look at phase VARIANT
SELECT stage_to_target_transfer_phase 
FROM pipeline_execution_drive 
WHERE pipeline_id = 'genetic_tests_20251115_10h_run1'

Get: error_message = "Snowflake table lock timeout waiting for lock on table CDW_DB.prod_genetic_tests (wait time: 30s)"

Step 3: Root Cause
Lock timeout = Another process was holding lock on target table
Could be:
- Another pipeline loading to same table
- Long-running query/rebuild on target table
- Maintenance operation

Step 4: Fix
- Identify conflicting process
- Adjust schedule to avoid conflicts
- Or increase lock timeout threshold

Step 5: Retry
- Trigger new DAG run
- Phase retries successfully
- Data completes loading
```

---

# User Scripts Interface

## Overview

User scripts are the business-logic layer of the pipeline. They implement the actual data extraction, transformation, and validation logic specific to each pipeline.

## Script Organization

All user scripts live in `main_project_folder/user_scripts/` directory:

```
user_scripts/
├── source_count.py           ← Implemented by user for this pipeline
├── stage_count.py
├── target_count.py
├── source_to_stage_transfer.py
├── stage_to_target_transfer.py
├── audit.py
├── stage_cleaning.py
└── target_cleaning.py
```

## Function Signatures

Every user script implements a specific function that the framework calls.

### Count Functions (source_count, stage_count, target_count)

**Purpose:** Query a system and return count of records

**Function Name:** Must be exactly as shown (case-sensitive)

**Parameters:**
- `config` (dict): Configuration loaded from config.json
  - Contains all pipeline settings (source endpoint, target schema, etc.)
  - Immutable for this run
- `record` (dict): Current drive table row as dictionary
  - Contains all columns from drive table
  - Useful for: Getting query_window, target_date, tracking phase progress

**Return Value (Dictionary):**
- `skip_dag_run` (boolean): True means "stop pipeline now"
  - True: Pipeline stops, no downstream phases
  - False: Pipeline continues to next phase
- `error_message` (string or None): Details if something failed
  - None if successful
  - String if error (will be logged and stored in VARIANT)
- `count` (integer or None): Number of records found
  - Integer: The count value
  - None: If unable to determine count (error occurred)

**Example Return Values:**
```
Success:
  {
    'skip_dag_run': False,
    'error_message': None,
    'count': 10000
  }

Failure (error occurred):
  {
    'skip_dag_run': True,
    'error_message': 'Elasticsearch connection timeout after 30s',
    'count': None
  }

Failure (data issue):
  {
    'skip_dag_run': True,
    'error_message': 'No data found in query window 2025-11-15 10:00 to 11:00',
    'count': None
  }
```

### Transfer Functions (source_to_stage_transfer, stage_to_target_transfer)

**Purpose:** Move and transform data between systems

**Parameters:** Same as count functions

**Return Value (Dictionary):**
- `skip_dag_run` (boolean): Stop pipeline if True
- `error_message` (string or None): Error details
- `transfer_completed` (boolean): Did transfer succeed?
  - True: Data successfully moved
  - False: Transfer incomplete (error occurred)

**Example Return Values:**
```
Success:
  {
    'skip_dag_run': False,
    'error_message': None,
    'transfer_completed': True
  }

Failure:
  {
    'skip_dag_run': True,
    'error_message': 'Snowflake table lock timeout',
    'transfer_completed': False
  }
```

### Audit Function

**Purpose:** Validate that data wasn't lost between stages

**Parameters:** Same as others

**Return Value (Dictionary):**
- `skip_dag_run` (boolean)
- `error_message` (string or None)
- `audit_passed` (boolean): Did audit validation pass?
  - True: Counts acceptable within tolerance
  - False: Counts show unexpected data loss

### Cleaning Functions (stage_cleaning, target_cleaning)

**Purpose:** Clean up temporary or old data

**Parameters:** Same

**Return Value (Dictionary):**
- `skip_dag_run` (boolean)
- `error_message` (string or None)
- `cleaning_completed` (boolean): Did cleanup succeed?

## Accessing Configuration

**From config.json:**
```
{
  "source_system": {
    "type": "elasticsearch",
    "endpoint": "https://es.company.com:9200",
    "index": "genetic_tests_v1"
  }
}
```

**In your script:**
```
config = {...}  # Passed to function

# Access nested values:
source_type = config['source_system']['type']
es_endpoint = config['source_system']['endpoint']
es_index = config['source_system']['index']
```

## Accessing Query Window

**From record (drive table row):**
```
record = {...}  # Passed to function

# Query window info:
window_start = record['query_window_start_timestamp']
window_end = record['query_window_end_timestamp']
target_date = record['target_date']

# Use in queries:
# "SELECT * FROM table WHERE timestamp >= {window_start} AND timestamp < {window_end}"
```

## Handling Errors

### Option 1: Return skip_dag_run = True
```
If something goes wrong:
  return {
    'skip_dag_run': True,
    'error_message': str(e),
    'count': None
  }

Pipeline stops, can be retried later.
```

### Option 2: Raise Exception
```
Alternatively, let exception bubble up:
  raise Exception("Something went wrong")

Framework catches exception, logs it, and stops pipeline.
```

### Best Practice: Return Error
- Return with skip_dag_run = True for expected errors (connection timeout, no data)
- Raise exception for unexpected errors (code bugs)
- Always provide descriptive error message

## Logging

**From user scripts:**
```
import logging

logger = logging.getLogger(__name__)

def source_count(config, record):
    logger.info(f"Querying source for window {record['query_window_start_timestamp']} to {record['query_window_end_timestamp']}")
    
    try:
        count = query_source(...)
        logger.info(f"Found {count} records")
        return {'skip_dag_run': False, 'error_message': None, 'count': count}
    except Exception as e:
        logger.error(f"Error querying source: {e}")
        return {'skip_dag_run': True, 'error_message': str(e), 'count': None}
```

**Logs are:**
- Written to Airflow task logs
- Accessible in Airflow UI
- Also stored in drive table VARIANT error_message field

## Code Reusability

All user scripts can import from common locations:

```
from user_scripts import common_utilities
from framework_scripts import snowflake_operations
from config_handler_scripts import config_loader

# Use shared functions
connection = snowflake_operations.get_snowflake_connection(config)
es_client = common_utilities.get_elasticsearch_client(config)
```

## Example: Full source_count Implementation

### For Elasticsearch

```
Logic:
1. Get Elasticsearch endpoint from config
2. Get index name from config
3. Get query window from record
4. Query ES: count documents with timestamp in window
5. Return count or error

Error Cases:
- ES not reachable: skip_dag_run = True
- Invalid query: skip_dag_run = True
- No data in window: Could be OK (return count=0) or error (return skip_dag_run=True) depending on business rules
```

### For MySQL

```
Logic:
1. Get MySQL host/port/database from config
2. Get table name from config
3. Get query window from record
4. Connect to MySQL
5. Execute: SELECT COUNT(*) FROM table WHERE created_at >= window_start AND created_at < window_end
6. Return count
7. Close connection

Error Cases:
- MySQL not reachable: skip_dag_run = True
- Table doesn't exist: skip_dag_run = True
- Authentication failed: skip_dag_run = True
```

## Testing User Scripts

Before deploying to production:

1. **Unit Test:** Test with mock config and data
   - Ensure function returns correct dictionary structure
   - Test error cases

2. **Integration Test:** Test with real systems
   - Can connect to source system?
   - Can connect to Snowflake?
   - Do queries work with real data?

3. **Dry Run:** Run pipeline with test query window
   - Use past date/time
   - Verify all phases complete
   - Verify data loaded correctly

---

# Framework Components

## Framework_scripts Directory

Contains reusable, non-business-specific code.

### Component 1: Phase Executor

**Purpose:** Execute user scripts and handle return values

**Responsibilities:**
- Import user script dynamically
- Call user script function
- Handle return values
- Check skip_dag_run flag
- Log execution details
- Update drive table

**Usage by Airflow DAG:**
```
For each phase:
  1. Call phase_executor.execute_phase(phase_name, config, record)
  2. Get result back
  3. If result['skip_dag_run'] = True: Stop DAG
  4. Otherwise: Continue to next phase
```

### Component 2: Snowflake Operations

**Purpose:** Common Snowflake operations

**Functions Provided:**
- `get_connection()`: Get Snowflake connection from config
- `initialize_pipeline_run()`: Create new drive table row
- `update_phase_variant()`: Update VARIANT column for a phase
- `update_phase_arrays()`: Update phases_completed/skipped arrays
- `finalize_pipeline_run()`: Set end_timestamp, calculate duration, set final status
- `query_previous_runs()`: Find earlier runs for continuation logic
- `get_count()`: Generic query to count records
- `record_to_dict()`: Convert Snowflake row to Python dict

### Component 3: Duration Utils

**Purpose:** Human-readable duration formatting

**Functions Provided:**
- `calculate_duration()`: Get duration between two timestamps
- `format_duration()`: Convert duration to human-readable string (e.g., "1h 30m 45s")
- `parse_duration()`: Convert string back to seconds (for calculations)

### Component 4: Error Handling

**Purpose:** Consistent error handling patterns

**Functions Provided:**
- `handle_phase_error()`: Process and log phase errors
- `create_error_message()`: Format error details
- `determine_skip_dag()`: Should pipeline stop?
- `log_to_variant()`: Store error in VARIANT

---

# Airflow Integration

## DAG Structure

Each pipeline has one Airflow DAG that orchestrates the phases.

### DAG File Location
`projects/{pipeline_name}/main_dag.py`

### DAG Components

#### DAG Definition
```
Pipeline ID: dag_id = f"pipeline_{pipeline_name}"
Schedule: Based on config cron_expression
Owner: From config pipeline_metadata.owner_name
Tags: ["data_pipeline", pipeline_name]
```

#### Airflow Tasks

One task per phase:

```
Task 1: initialize_pipeline
  └─ Create drive table row
  └─ Set pipeline_status = RUNNING

Task 2: stale_pipeline_handling
  └─ Call framework phase executor
  └─ Execute user_scripts/stale_pipeline_handling.py
  └─ Check skip_dag_run
  └─ Update drive table

Task 3: pre_validation
  └─ Call phase executor
  └─ Execute user_scripts/pre_validation.py
  └─ Get phases_to_skip list
  └─ Update drive table

Task 4: source_count
  └─ Call phase executor
  └─ Execute user_scripts/source_count.py
  └─ Store count in audit VARIANT

... (more count and transfer tasks)

Task N: finalize_pipeline
  └─ Set pipeline_end_timestamp
  └─ Calculate pipeline_run_duration
  └─ Set pipeline_status = SUCCESS or FAILED
  └─ Update drive table

Task N+1: notify_result
  └─ Send email/Slack with results
```

#### Task Dependencies

```
initialize_pipeline
        ↓
[stale_handling, pre_validation] (can run together or sequentially)
        ↓
[source_count, source_to_stage_transfer, stage_count] (sequential: count→transfer→count)
        ↓
[stage_to_target_transfer, target_count] (sequential)
        ↓
audit
        ↓
[stage_cleaning, target_cleaning] (optional, can skip)
        ↓
finalize_pipeline
        ↓
notify_result
```

## Error Handling in Airflow

### When Phase Fails (skip_dag_run = True)

```
Phase task executes
  ↓
User script returns skip_dag_run = True
  ↓
Framework updates drive table (phase_failed = this phase)
  ↓
Framework returns skip_dag_run = True to task
  ↓
Airflow task catches skip_dag_run and stops DAG execution
  ↓
Downstream tasks are skipped (not executed)
  ↓
DAG finishes with status FAILED
  ↓
Notification sent: "Pipeline failed at [phase_name]"
```

### When Phase Raises Exception

```
Phase task executes
  ↓
User script raises exception
  ↓
Framework catches exception
  ↓
Framework logs error
  ↓
Framework updates drive table
  ↓
Airflow task fails (not skipped)
  ↓
Airflow retry logic may kick in (if configured)
  ↓
Or DAG stops
```

## Monitoring & Visibility

### Real-time Monitoring
- Airflow UI shows DAG progress
- Each task shows start time, duration, status
- Logs accessible from task details

### Post-Execution Reporting
- Query drive table to see execution details
- VARIANT columns contain phase-level info
- phases_completed array shows which phases ran
- phase_failed shows failure point

### Alerting
- If phase fails: send email/Slack alert
- If duration exceeds threshold: send alert
- If success: optional summary email

---

# Monitoring & Operations

## Key Metrics to Track

### Metric 1: Pipeline Success Rate
**Definition:** Percentage of pipeline runs that complete successfully

**Calculation:** (Successful runs / Total runs) * 100

**Tracking:** Query drive table WHERE pipeline_status = 'SUCCESS' vs total

**Target:** > 95%

**Alert Threshold:** If drops below 90%

### Metric 2: Phase Failure Rate
**Definition:** Which phases fail most frequently

**Tracking:** For each phase, count how many times it failed (status = FAILED in VARIANT)

**Purpose:** Identify problem areas
- If source_to_stage always fails: source system issue
- If audit always fails: data quality issue
- If stage_cleaning fails: storage/permission issue

### Metric 3: Phase Duration vs Expected Duration
**Definition:** Compare actual execution time to baseline

**Calculation:** actual_run_duration / expected_run_duration

**Alert:** If actual > 2x expected
- Could indicate: slow network, large data volume, resource constraints

### Metric 4: Count Mismatch Percentage
**Definition:** How much data is lost between stages

**Calculation:** (source_count - stage_count) / source_count * 100

**Track:** Compare to tolerance_percent from config
- If within tolerance: OK
- If exceeding tolerance: audit fails

### Metric 5: Data Freshness
**Definition:** How old is the latest data in target?

**Calculation:** Current time - max(created_timestamp in target table)

**Alert:** If > X hours, data is stale
- Could indicate: pipeline stopped, source system down, or just scheduled for daily

### Metric 6: Retry Success Rate
**Definition:** Of pipelines that failed, what percentage succeeded on retry?

**Tracking:** Find all runs with phase_failed != NULL, then find retry runs that succeeded

**Goal:** High retry success rate indicates transient errors (good), not permanent issues

## Operational Dashboards

### Dashboard 1: Pipeline Health (Daily View)
**What to Show:**
- List of all pipelines
- Status of most recent run (SUCCESS, FAILED, RUNNING)
- Time of last successful run
- Alert indicators (red for failed, yellow for warnings)

**Purpose:** Operations team quick check

### Dashboard 2: Failure Analysis (Last 7 Days)
**What to Show:**
- Failed pipelines (which pipelines failed)
- Failure point (which phase failed most)
- Error messages (what went wrong)
- Retry results (did retries fix it)

**Purpose:** Identify trends, common issues

### Dashboard 3: Performance (Last 7 Days)
**What to Show:**
- Average phase duration vs expected
- Slowest phases
- Pipelines that exceed SLA
- Data freshness (time since last successful run)

**Purpose:** Capacity planning, identify bottlenecks

### Dashboard 4: Data Quality (Last 7 Days)
**What to Show:**
- Count mismatches (data loss by phase)
- Audit pass/fail rate
- Records rejected during validation

**Purpose:** Data quality monitoring

## Operational Tasks

### Daily Tasks
- Check health dashboard for any failures
- If failed pipelines: Check phase_failed reason
- Coordinate with teams if specific systems down

### Weekly Tasks
- Review failure trends
- Review performance metrics
- Assess if tolerances need adjustment

### Monthly Tasks
- Capacity planning: Data growth trends
- Review archive storage usage
- Update runbooks if procedures changed

---

# Deployment Guide

## Pre-Deployment Checklist

### 1. Configuration Validation
- [ ] config.json created and valid JSON
- [ ] All required fields present
- [ ] Source system endpoint accessible
- [ ] Target Snowflake database/schema exist
- [ ] Tolerances are reasonable (0-100%)

### 2. User Scripts Validation
- [ ] All 6+ required scripts created
- [ ] Function signatures match exactly
- [ ] Scripts test with mock data
- [ ] Error handling implemented
- [ ] No hardcoded credentials

### 3. Permissions Verification
- [ ] Service account has read access to source
- [ ] Service account has write access to stage
- [ ] Service account has read/write access to target
- [ ] Service account can read/write drive table

### 4. Testing
- [ ] Dry-run with past data (small window)
- [ ] Verify data loaded correctly
- [ ] Verify counts and audit pass
- [ ] Verify cleaning works (if enabled)

## Deployment Steps

### Step 1: Create Project Folder
```
Create: main_project_folder/projects/pipeline_name/
Create: main_project_folder/projects/pipeline_name/config.json
Create: main_project_folder/projects/pipeline_name/main_dag.py
```

### Step 2: Deploy Configuration
- Copy config.json to correct location
- Store secrets separately (AWS Secrets Manager, Vault)
- Version control config.json (track changes)

### Step 3: Deploy Scripts
- Copy user scripts to main_project_folder/user_scripts/
- Or reference from projects/pipeline_name/user_scripts/ if customized
- Ensure all required scripts present

### Step 4: Deploy DAG to Airflow
- Copy main_dag.py to Airflow DAGs folder
- Airflow auto-discovers DAG
- DAG appears in Airflow UI within 60 seconds

### Step 5: Enable Scheduling
- In Airflow UI: Toggle DAG to enabled
- Schedule starts as per cron_expression
- Or manually trigger first run

### Step 6: Verify First Run
- Monitor Airflow UI
- Check task progress
- Verify all phases complete
- Query drive table to see execution record
- Query target table to see data

## Rollback Procedure

### If Issues After Deployment

**Option 1: Pause Pipeline**
- Disable DAG in Airflow (stop new runs)
- Already running instances complete or fail
- No new executions start

**Option 2: Revert Configuration**
- Update config.json to previous version
- Existing data stays (no delete)
- Future runs use new config

**Option 3: Revert Code**
- Update user scripts to previous version
- Future runs use new scripts

**Option 4: Delete/Recreate**
- Remove DAG from Airflow
- Clean up stage data (optional)
- Redeploy with fixes

---

# Troubleshooting

## Common Issues & Solutions

### Issue 1: Pipeline Runs But Phase Fails

**Symptom:** Pipeline shows FAILED, phase_failed = [phase_name]

**Investigation:**
1. Check drive table VARIANT: Look at error_message
2. Check Airflow logs: Detailed error trace
3. Check source system: Is it accessible/healthy?

**Solution:** Based on error_message
- If connection error: Check source system status
- If data validation error: Check data quality
- If Snowflake lock: Wait or reschedule conflicting process

### Issue 2: Audit Fails (Count Mismatch)

**Symptom:** pipeline_status = FAILED, phase_failed = audit

**Investigation:**
1. Check audit VARIANT: source_count, stage_count, loss_percent
2. Loss > tolerance? Yes = audit correctly failed
3. Why is loss > tolerance? 

**Solution:**
- If loss is legitimate (validation failures): Increase tolerance in config
- If loss is unexpected: Debug the transfer phase (why records failed)
- If tolerance is too strict: Adjust based on data patterns

### Issue 3: Data Appears Twice

**Symptom:** Query target table and find duplicates

**Root Cause:** Retry run loaded same query_window data again

**Solution:**
- source_to_stage_transfer should delete existing data before reload
- Or use UPSERT logic (update if exists)
- Verify idempotency in transfer script

### Issue 4: Query Window Has No Data

**Symptom:** source_count returns 0

**Investigation:**
- Query source directly: Any data in this window?
- Check query_window_start/end: Correct range?
- Check query_field: Correct timestamp field?

**Solution:**
- If no data expected: Return count=0 (might be OK)
- If data expected: Check source data ingestion
- Check query_field is populated (not NULL)

### Issue 5: Snowflake Permission Denied

**Symptom:** phase fails with "Permission denied" error

**Investigation:**
- Check IAM/role: Service account permissions
- Check table grants: Role has rights to table
- Check database grants: Role has database access

**Solution:**
- Grant permissions: GRANT READ ON DATABASE CDW_DB TO ROLE service_role
- Grant table permissions: GRANT ALL ON TABLE prod_genetic_tests TO ROLE service_role

### Issue 6: Stage Data Not Deleted

**Symptom:** stage_cleaning runs but data still in stage

**Root Cause:** delete_after_days threshold not reached yet

**Investigation:**
- Check: delete_after_days = 7 (only delete data > 7 days old)
- Check: target_date of stage data (is it old enough?)

**Solution:**
- Lower delete_after_days in config (e.g., 3 days instead of 7)
- Or manually clean old data: DELETE FROM stage WHERE target_date < DATE_SUB(TODAY(), INTERVAL 3 DAY)

---

# FAQ

## Q1: How Long Does a Pipeline Take?
**A:** Depends on data volume and transformation complexity. Typically 10-60 minutes. actual_run_duration stored in drive table for each run.

## Q2: Can Two Pipelines Run Simultaneously?
**A:** Yes, they have different pipeline_ids and operate independently. However, if loading to same target table, Snowflake locks might slow one down.

## Q3: What If Source System Is Down?
**A:** source_count phase fails with connection error. Pipeline stops. Manual retry when source is back up.

## Q4: How Often Is Data Backed Up?
**A:** Stage data archived before deletion (if archive_before_delete=true). Keep in S3 for compliance period. Target data is permanent (source of truth).

## Q5: Can I Change Query Window After Deployment?
**A:** Changing query_window in config affects future runs only. Already-processed query windows won't be re-processed.

## Q6: What If I Need to Reprocess Historical Data?
**A:** Manually trigger DAG for past query_window. Airflow allows running DAGs with past execution_date. New drive table row created.

## Q7: Where Are Errors Logged?
**A:** Three places:
1. Airflow task logs (most detailed)
2. Drive table VARIANT error_message (structured)
3. Application logs (if logging configured)

## Q8: How Do I Monitor Real-Time Progress?
**A:** Watch Airflow UI while DAG runs. See task status, duration. Or query drive table WHERE pipeline_status = 'RUNNING'.

## Q9: Can I Manually Intervene in Running Pipeline?
**A:** Not recommended. Let pipeline complete/fail. Then inspect drive table to understand what happened.

## Q10: What If Target Table Schema Changes?
**A:** stage_to_target_transfer would fail (schema mismatch). Update target schema or update transformation script to match new schema. Deploy updated script.

## Q11: How Do I Know Which Phases Were Skipped?
**A:** Query drive table: phases_skipped array shows which phases were skipped (during retry, skipped the already-completed phases).

## Q12: What's the Difference Between SKIPPED and PENDING?
**A:** SKIPPED = Phase was marked as not-needed (already completed in previous run or explicitly disabled). PENDING = Phase hasn't executed yet in current run.

## Q13: Can I Run Phase Independently?
**A:** Not recommended. Phases depend on each other (count phase depends on previous transfer phase data). Run entire pipeline.

## Q14: What If Tolerance Percentage Should Vary by Time?
**A:** Not supported. Use one tolerance per phase per pipeline. If needs vary: create separate pipelines with different configs.

## Q15: Where Is Most Storage Used?
**A:** Stage table (if not cleaned) and archive S3. Monitor storage: Stage should stay small (cleaned frequently), archive grows slowly (compliance period).

---

## Document Version: 1.0
## Last Updated: 2025-11-15
## Status: Complete - Ready for Implementation
