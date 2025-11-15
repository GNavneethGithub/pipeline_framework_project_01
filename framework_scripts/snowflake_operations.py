"""
Snowflake Operations Module
===========================
Provides common Snowflake operations for the pipeline framework.

Functions:
    - get_connection: Get Snowflake connection from config
    - initialize_pipeline_run: Create new drive table row
    - update_phase_variant: Update VARIANT column for a phase
    - update_phase_arrays: Update phases_completed/skipped arrays
    - finalize_pipeline_run: Set end_timestamp and final status
    - query_previous_runs: Find earlier runs for continuation logic
    - get_count: Generic query to count records
    - record_to_dict: Convert Snowflake row to Python dict
"""

import json
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
import snowflake.connector
from snowflake.connector import DictCursor

from framework_scripts.duration_utils import calculate_duration, format_duration


logger = logging.getLogger(__name__)


class SnowflakeConnection:
    """Context manager for Snowflake connections."""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Snowflake connection.

        Args:
            config: Pipeline configuration containing Snowflake credentials
        """
        self.config = config
        self.conn = None

    def __enter__(self):
        """Open connection."""
        snowflake_config = self.config.get('snowflake_connection', {})

        self.conn = snowflake.connector.connect(
            user=snowflake_config.get('user'),
            password=snowflake_config.get('password'),
            account=snowflake_config.get('account'),
            warehouse=snowflake_config.get('warehouse'),
            database=snowflake_config.get('database'),
            schema=snowflake_config.get('schema'),
            role=snowflake_config.get('role')
        )

        logger.info(f"Snowflake connection established to {snowflake_config.get('account')}")
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close connection."""
        if self.conn:
            self.conn.close()
            logger.info("Snowflake connection closed")


def get_connection(config: Dict[str, Any]) -> snowflake.connector.SnowflakeConnection:
    """
    Get Snowflake connection from configuration.

    Args:
        config: Pipeline configuration

    Returns:
        SnowflakeConnection: Active Snowflake connection

    Example:
        >>> with SnowflakeConnection(config) as conn:
        >>>     cursor = conn.cursor()
        >>>     cursor.execute("SELECT 1")
    """
    snowflake_config = config.get('snowflake_connection', {})

    conn = snowflake.connector.connect(
        user=snowflake_config.get('user'),
        password=snowflake_config.get('password'),
        account=snowflake_config.get('account'),
        warehouse=snowflake_config.get('warehouse'),
        database=snowflake_config.get('database'),
        schema=snowflake_config.get('schema'),
        role=snowflake_config.get('role')
    )

    return conn


def initialize_pipeline_run(
    config: Dict[str, Any],
    pipeline_id: str,
    query_window_start: datetime,
    query_window_end: datetime,
    retry_number: int = 0
) -> Dict[str, Any]:
    """
    Create new drive table row for pipeline execution.

    Args:
        config: Pipeline configuration
        pipeline_id: Unique identifier for this run
        query_window_start: Start of query window
        query_window_end: End of query window
        retry_number: Retry attempt number (0 for first run)

    Returns:
        dict: Created drive table record

    Example:
        >>> record = initialize_pipeline_run(
        >>>     config,
        >>>     "genetic_tests_20251115_10h_run1",
        >>>     datetime(2025, 11, 15, 10, 0, 0),
        >>>     datetime(2025, 11, 15, 11, 0, 0)
        >>> )
    """
    pipeline_name = config.get('pipeline_metadata', {}).get('pipeline_name')
    target_date = query_window_start.date()
    query_window_duration = format_duration(calculate_duration(query_window_start, query_window_end))

    # Get all phases from config
    phases = config.get('phases', {})
    all_phases = [
        'stale_pipeline_handling',
        'pre_validation',
        'source_to_stage_transfer',
        'stage_to_target_transfer',
        'audit',
        'stage_cleaning',
        'target_cleaning'
    ]

    # Filter to only enabled phases
    enabled_phases = [phase for phase in all_phases if phases.get(phase, {}).get('enabled', True)]

    insert_sql = """
        INSERT INTO pipeline_execution_drive (
            pipeline_id,
            pipeline_name,
            pipeline_status,
            pipeline_retry_number,
            pipeline_start_timestamp,
            query_window_start_timestamp,
            query_window_end_timestamp,
            target_date,
            query_window_duration,
            phases_pending,
            phases_completed,
            phases_skipped,
            created_at,
            updated_at
        ) VALUES (
            %(pipeline_id)s,
            %(pipeline_name)s,
            'RUNNING',
            %(retry_number)s,
            CURRENT_TIMESTAMP(),
            %(query_window_start)s,
            %(query_window_end)s,
            %(target_date)s,
            %(query_window_duration)s,
            PARSE_JSON(%(phases_pending)s),
            PARSE_JSON('[]'),
            PARSE_JSON('[]'),
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP()
        )
    """

    with SnowflakeConnection(config) as conn:
        cursor = conn.cursor()

        cursor.execute(insert_sql, {
            'pipeline_id': pipeline_id,
            'pipeline_name': pipeline_name,
            'retry_number': retry_number,
            'query_window_start': query_window_start,
            'query_window_end': query_window_end,
            'target_date': target_date,
            'query_window_duration': query_window_duration,
            'phases_pending': json.dumps(enabled_phases)
        })

        conn.commit()

        logger.info(f"Pipeline run initialized: {pipeline_id}")

    # Return the created record
    return get_pipeline_run(config, pipeline_id)


def update_phase_variant(
    config: Dict[str, Any],
    pipeline_id: str,
    phase_name: str,
    phase_data: Dict[str, Any]
) -> None:
    """
    Update VARIANT column for a specific phase.

    Args:
        config: Pipeline configuration
        pipeline_id: Pipeline execution ID
        phase_name: Name of phase to update
        phase_data: Phase execution data to store

    Example:
        >>> update_phase_variant(
        >>>     config,
        >>>     "genetic_tests_20251115_10h_run1",
        >>>     "source_to_stage_transfer",
        >>>     {
        >>>         "status": "COMPLETED",
        >>>         "start_timestamp": "2025-11-15T10:05:00Z",
        >>>         "end_timestamp": "2025-11-15T10:10:00Z",
        >>>         "actual_run_duration": "5m"
        >>>     }
        >>> )
    """
    # Convert phase_name to column name
    phase_column = f"{phase_name}_phase"

    update_sql = f"""
        UPDATE pipeline_execution_drive
        SET {phase_column} = PARSE_JSON(%(phase_data)s),
            updated_at = CURRENT_TIMESTAMP()
        WHERE pipeline_id = %(pipeline_id)s
    """

    with SnowflakeConnection(config) as conn:
        cursor = conn.cursor()

        cursor.execute(update_sql, {
            'pipeline_id': pipeline_id,
            'phase_data': json.dumps(phase_data)
        })

        conn.commit()

        logger.info(f"Phase variant updated: {pipeline_id} - {phase_name}")


def update_phase_arrays(
    config: Dict[str, Any],
    pipeline_id: str,
    phase_name: str,
    status: str
) -> None:
    """
    Update phases_completed/skipped/pending arrays.

    Args:
        config: Pipeline configuration
        pipeline_id: Pipeline execution ID
        phase_name: Name of phase
        status: Phase status (COMPLETED, SKIPPED, FAILED)

    Example:
        >>> update_phase_arrays(config, pipeline_id, "pre_validation", "COMPLETED")
    """
    if status == 'COMPLETED':
        update_sql = """
            UPDATE pipeline_execution_drive
            SET phases_completed = ARRAY_APPEND(phases_completed, %(phase_name)s),
                phases_pending = ARRAY_REMOVE(phases_pending, %(phase_name)s),
                updated_at = CURRENT_TIMESTAMP()
            WHERE pipeline_id = %(pipeline_id)s
        """
    elif status == 'SKIPPED':
        update_sql = """
            UPDATE pipeline_execution_drive
            SET phases_skipped = ARRAY_APPEND(phases_skipped, %(phase_name)s),
                phases_pending = ARRAY_REMOVE(phases_pending, %(phase_name)s),
                updated_at = CURRENT_TIMESTAMP()
            WHERE pipeline_id = %(pipeline_id)s
        """
    elif status == 'FAILED':
        update_sql = """
            UPDATE pipeline_execution_drive
            SET phase_failed = %(phase_name)s,
                phases_pending = ARRAY_REMOVE(phases_pending, %(phase_name)s),
                updated_at = CURRENT_TIMESTAMP()
            WHERE pipeline_id = %(pipeline_id)s
        """
    else:
        logger.warning(f"Unknown status: {status}")
        return

    with SnowflakeConnection(config) as conn:
        cursor = conn.cursor()

        cursor.execute(update_sql, {
            'pipeline_id': pipeline_id,
            'phase_name': phase_name
        })

        conn.commit()

        logger.info(f"Phase arrays updated: {pipeline_id} - {phase_name} - {status}")


def finalize_pipeline_run(
    config: Dict[str, Any],
    pipeline_id: str,
    status: str
) -> None:
    """
    Set pipeline_end_timestamp, calculate duration, set final status.

    Args:
        config: Pipeline configuration
        pipeline_id: Pipeline execution ID
        status: Final status (SUCCESS or FAILED)

    Example:
        >>> finalize_pipeline_run(config, pipeline_id, "SUCCESS")
    """
    # Get the pipeline run to calculate duration
    record = get_pipeline_run(config, pipeline_id)

    if record:
        start_time = record['pipeline_start_timestamp']
        end_time = datetime.now()
        duration = format_duration(calculate_duration(start_time, end_time))
    else:
        duration = "Unknown"

    update_sql = """
        UPDATE pipeline_execution_drive
        SET pipeline_end_timestamp = CURRENT_TIMESTAMP(),
            pipeline_run_duration = %(duration)s,
            pipeline_status = %(status)s,
            updated_at = CURRENT_TIMESTAMP()
        WHERE pipeline_id = %(pipeline_id)s
    """

    with SnowflakeConnection(config) as conn:
        cursor = conn.cursor()

        cursor.execute(update_sql, {
            'pipeline_id': pipeline_id,
            'duration': duration,
            'status': status
        })

        conn.commit()

        logger.info(f"Pipeline finalized: {pipeline_id} - {status} - Duration: {duration}")


def query_previous_runs(
    config: Dict[str, Any],
    pipeline_name: str,
    target_date: str
) -> List[Dict[str, Any]]:
    """
    Find previous runs for continuation logic.

    Args:
        config: Pipeline configuration
        pipeline_name: Name of pipeline
        target_date: Target date to search for

    Returns:
        list: Previous pipeline runs for this pipeline and date

    Example:
        >>> previous_runs = query_previous_runs(config, "genetic_tests", "2025-11-15")
    """
    query_sql = """
        SELECT *
        FROM pipeline_execution_drive
        WHERE pipeline_name = %(pipeline_name)s
          AND target_date = %(target_date)s
        ORDER BY created_at DESC
    """

    with SnowflakeConnection(config) as conn:
        cursor = conn.cursor(DictCursor)

        cursor.execute(query_sql, {
            'pipeline_name': pipeline_name,
            'target_date': target_date
        })

        results = cursor.fetchall()

        logger.info(f"Found {len(results)} previous runs for {pipeline_name} on {target_date}")

        return results


def get_pipeline_run(
    config: Dict[str, Any],
    pipeline_id: str
) -> Optional[Dict[str, Any]]:
    """
    Get pipeline run record by ID.

    Args:
        config: Pipeline configuration
        pipeline_id: Pipeline execution ID

    Returns:
        dict: Pipeline run record or None if not found
    """
    query_sql = """
        SELECT *
        FROM pipeline_execution_drive
        WHERE pipeline_id = %(pipeline_id)s
    """

    with SnowflakeConnection(config) as conn:
        cursor = conn.cursor(DictCursor)

        cursor.execute(query_sql, {'pipeline_id': pipeline_id})

        result = cursor.fetchone()

        return result


def get_count(
    config: Dict[str, Any],
    table: str,
    where_clause: str = ""
) -> int:
    """
    Generic count query for validation.

    Args:
        config: Pipeline configuration
        table: Table to count from
        where_clause: Optional WHERE clause (without WHERE keyword)

    Returns:
        int: Record count

    Example:
        >>> count = get_count(
        >>>     config,
        >>>     "CADS_DB.stg_genetic_tests",
        >>>     "target_date = '2025-11-15'"
        >>> )
    """
    if where_clause:
        query_sql = f"SELECT COUNT(*) as cnt FROM {table} WHERE {where_clause}"
    else:
        query_sql = f"SELECT COUNT(*) as cnt FROM {table}"

    with SnowflakeConnection(config) as conn:
        cursor = conn.cursor()

        cursor.execute(query_sql)

        result = cursor.fetchone()

        count = result[0] if result else 0

        logger.info(f"Count query: {table} - {count} records")

        return count


def record_to_dict(record: Any) -> Dict[str, Any]:
    """
    Convert Snowflake row to Python dict.

    Args:
        record: Snowflake row object

    Returns:
        dict: Record as dictionary
    """
    if isinstance(record, dict):
        return record

    # If using DictCursor, already a dict
    return dict(record)


# Example usage
if __name__ == "__main__":
    # This is example usage - requires actual Snowflake credentials
    sample_config = {
        'pipeline_metadata': {
            'pipeline_name': 'test_pipeline'
        },
        'snowflake_connection': {
            'user': 'your_user',
            'password': 'your_password',
            'account': 'your_account',
            'warehouse': 'your_warehouse',
            'database': 'your_database',
            'schema': 'your_schema',
            'role': 'your_role'
        },
        'phases': {
            'stale_pipeline_handling': {'enabled': True},
            'pre_validation': {'enabled': True},
            'source_to_stage_transfer': {'enabled': True},
            'stage_to_target_transfer': {'enabled': True},
            'audit': {'enabled': True},
            'stage_cleaning': {'enabled': True},
            'target_cleaning': {'enabled': False}
        }
    }

    print("Snowflake operations module loaded successfully")
    print("Example functions available:")
    print("  - initialize_pipeline_run()")
    print("  - update_phase_variant()")
    print("  - update_phase_arrays()")
    print("  - finalize_pipeline_run()")
    print("  - query_previous_runs()")
    print("  - get_count()")
