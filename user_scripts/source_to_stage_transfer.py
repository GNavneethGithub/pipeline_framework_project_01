"""
Source to Stage Transfer Phase User Script
===========================================
This script extracts data from source system and loads into stage table.

Function Signature:
    source_to_stage_transfer(config: Dict, record: Dict) -> Dict

Return Values:
    {
        'skip_dag_run': bool,
        'error_message': str or None,
        'transfer_completed': bool,
        'source_count': int,
        'stage_count': int,
        'records_failed': int
    }
"""

import logging
from typing import Dict, Any
from datetime import datetime

from framework_scripts import snowflake_operations as sf_ops


logger = logging.getLogger(__name__)


def source_to_stage_transfer(config: Dict[str, Any], record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract data from source and load to stage table.

    Args:
        config: Pipeline configuration
        record: Current drive table row

    Returns:
        dict: Transfer result
    """
    logger.info("Starting source to stage transfer")

    try:
        # Step 1: Get configuration
        source_config = config.get('source_system', {})
        stage_config = config.get('stage_system', {})

        source_type = source_config.get('type')
        batch_size = source_config.get('batch_size', 5000)

        query_window_start = record.get('query_window_start_timestamp')
        query_window_end = record.get('query_window_end_timestamp')
        target_date = record.get('target_date')

        logger.info(f"Source type: {source_type}")
        logger.info(f"Query window: {query_window_start} to {query_window_end}")
        logger.info(f"Batch size: {batch_size}")

        # Step 2: Count source records
        logger.info("Counting source records...")
        source_count = count_source_records(config, query_window_start, query_window_end)
        logger.info(f"Source count: {source_count}")

        if source_count == 0:
            logger.warning("No records found in source for this query window")
            # This might be OK depending on use case
            # Returning success with 0 records

        # Step 3: Clear stage table for this query window (idempotency)
        logger.info("Clearing stage table for this query window...")
        clear_stage_table(config, target_date)

        # Step 4: Extract and load data
        logger.info("Extracting and loading data...")
        records_loaded, records_failed = extract_and_load(
            config,
            query_window_start,
            query_window_end,
            target_date,
            batch_size
        )

        logger.info(f"Records loaded to stage: {records_loaded}")
        logger.info(f"Records failed validation: {records_failed}")

        # Step 5: Verify stage count
        stage_count = count_stage_records(config, target_date)
        logger.info(f"Stage count verified: {stage_count}")

        # Check if counts match
        if stage_count != records_loaded:
            logger.warning(
                f"Stage count ({stage_count}) doesn't match records loaded ({records_loaded})"
            )

        # Success
        logger.info("Transfer completed successfully")

        return {
            'skip_dag_run': False,
            'error_message': None,
            'transfer_completed': True,
            'source_count': source_count,
            'stage_count': stage_count,
            'records_failed': records_failed,
            'transfer_status': 'SUCCESS'
        }

    except Exception as e:
        logger.error(f"Source to stage transfer failed: {e}")
        return {
            'skip_dag_run': True,
            'error_message': str(e),
            'transfer_completed': False,
            'source_count': 0,
            'stage_count': 0,
            'records_failed': 0,
            'transfer_status': 'FAILED'
        }


def count_source_records(
    config: Dict[str, Any],
    window_start: datetime,
    window_end: datetime
) -> int:
    """
    Count records in source system for query window.

    Args:
        config: Pipeline configuration
        window_start: Query window start
        window_end: Query window end

    Returns:
        int: Record count
    """
    source_config = config.get('source_system', {})
    source_type = source_config.get('type')

    # TODO: Implement based on source_type
    # For Elasticsearch:
    #   - Connect to ES cluster
    #   - Query with timestamp filter
    #   - Return count

    # For MySQL:
    #   - Connect to MySQL
    #   - SELECT COUNT(*) WHERE timestamp >= start AND timestamp < end
    #   - Return count

    # For now, return mock count
    logger.info(f"Counting records in {source_type} source...")

    # REPLACE THIS with actual implementation
    return 10000  # Mock count


def clear_stage_table(config: Dict[str, Any], target_date: str) -> None:
    """
    Clear stage table for this query window to ensure idempotency.

    Args:
        config: Pipeline configuration
        target_date: Target date to clear
    """
    stage_config = config.get('stage_system', {})
    stage_database = stage_config.get('database')
    stage_schema = stage_config.get('schema')
    stage_table = stage_config.get('table')

    full_table_name = f"{stage_database}.{stage_schema}.{stage_table}"

    delete_sql = f"""
        DELETE FROM {full_table_name}
        WHERE target_date = %(target_date)s
    """

    with sf_ops.SnowflakeConnection(config) as conn:
        cursor = conn.cursor()
        cursor.execute(delete_sql, {'target_date': target_date})
        conn.commit()

        rows_deleted = cursor.rowcount
        logger.info(f"Deleted {rows_deleted} existing records from stage table")


def extract_and_load(
    config: Dict[str, Any],
    window_start: datetime,
    window_end: datetime,
    target_date: str,
    batch_size: int
) -> tuple:
    """
    Extract data from source and load to stage.

    Args:
        config: Pipeline configuration
        window_start: Query window start
        window_end: Query window end
        target_date: Target date
        batch_size: Records per batch

    Returns:
        tuple: (records_loaded, records_failed)
    """
    source_config = config.get('source_system', {})
    source_type = source_config.get('type')

    # TODO: Implement based on source_type

    # For Elasticsearch:
    #   1. Connect to ES
    #   2. Query with scroll API
    #   3. For each batch:
    #      - Validate records
    #      - Insert to stage table
    #   4. Return counts

    # For MySQL:
    #   1. Connect to MySQL
    #   2. Query with LIMIT/OFFSET
    #   3. For each batch:
    #      - Validate records
    #      - Insert to stage table
    #   4. Return counts

    # REPLACE THIS with actual implementation
    logger.info(f"Extracting data from {source_type}...")

    records_loaded = 9800  # Mock: 200 failed validation
    records_failed = 200

    return records_loaded, records_failed


def count_stage_records(config: Dict[str, Any], target_date: str) -> int:
    """
    Count records in stage table for target date.

    Args:
        config: Pipeline configuration
        target_date: Target date

    Returns:
        int: Record count
    """
    stage_config = config.get('stage_system', {})
    stage_database = stage_config.get('database')
    stage_schema = stage_config.get('schema')
    stage_table = stage_config.get('table')

    full_table_name = f"{stage_database}.{stage_schema}.{stage_table}"

    return sf_ops.get_count(
        config,
        full_table_name,
        f"target_date = '{target_date}'"
    )


# Example usage
if __name__ == "__main__":
    print("Source to Stage Transfer script loaded")
    print("This script extracts data from source and loads to stage table")
    print("\nTODO: Implement source-specific extraction logic")
    print("  - For Elasticsearch: Use elasticsearch-py library")
    print("  - For MySQL: Use mysql-connector-python")
    print("  - For APIs: Use requests library")
