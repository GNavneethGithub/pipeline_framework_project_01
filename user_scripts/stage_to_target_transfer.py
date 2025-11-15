"""
Stage to Target Transfer Phase User Script
==========================================
This script applies transformations and loads data from stage to target.

Function Signature:
    stage_to_target_transfer(config: Dict, record: Dict) -> Dict

Return Values:
    {
        'skip_dag_run': bool,
        'error_message': str or None,
        'transfer_completed': bool,
        'stage_count': int,
        'target_count': int,
        'transformations_applied': str
    }
"""

import logging
from typing import Dict, Any

from framework_scripts import snowflake_operations as sf_ops


logger = logging.getLogger(__name__)


def stage_to_target_transfer(config: Dict[str, Any], record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform and load data from stage to target.

    Args:
        config: Pipeline configuration
        record: Current drive table row

    Returns:
        dict: Transfer result
    """
    logger.info("Starting stage to target transfer")

    try:
        target_date = record.get('target_date')

        # Step 1: Count stage records
        stage_count = count_stage_records(config, target_date)
        logger.info(f"Stage count: {stage_count}")

        if stage_count == 0:
            logger.warning("No records in stage table to transfer")

        # Step 2: Clear target for this query window (idempotency)
        logger.info("Clearing target table for this query window...")
        clear_target_table(config, target_date)

        # Step 3: Transform and load
        logger.info("Transforming and loading data to target...")
        target_count = transform_and_load(config, target_date)

        logger.info(f"Target count: {target_count}")

        # Success
        logger.info("Stage to target transfer completed successfully")

        return {
            'skip_dag_run': False,
            'error_message': None,
            'transfer_completed': True,
            'stage_count': stage_count,
            'target_count': target_count,
            'transformations_applied': 'Standard transformations (rename columns, deduplicate)'
        }

    except Exception as e:
        logger.error(f"Stage to target transfer failed: {e}")
        return {
            'skip_dag_run': True,
            'error_message': str(e),
            'transfer_completed': False,
            'stage_count': 0,
            'target_count': 0
        }


def count_stage_records(config: Dict[str, Any], target_date: str) -> int:
    """Count records in stage table."""
    stage_config = config.get('stage_system', {})
    full_table_name = f"{stage_config['database']}.{stage_config['schema']}.{stage_config['table']}"

    return sf_ops.get_count(config, full_table_name, f"target_date = '{target_date}'")


def clear_target_table(config: Dict[str, Any], target_date: str) -> None:
    """Clear target table for this query window (idempotency)."""
    target_config = config.get('target_system', {})
    full_table_name = f"{target_config['database']}.{target_config['schema']}.{target_config['table']}"

    delete_sql = f"DELETE FROM {full_table_name} WHERE target_date = %(target_date)s"

    with sf_ops.SnowflakeConnection(config) as conn:
        cursor = conn.cursor()
        cursor.execute(delete_sql, {'target_date': target_date})
        conn.commit()

        logger.info(f"Deleted {cursor.rowcount} existing records from target table")


def transform_and_load(config: Dict[str, Any], target_date: str) -> int:
    """
    Transform stage data and load to target.

    TODO: Implement your business-specific transformations here.

    Example transformations:
    - Rename columns
    - Parse nested JSON
    - Calculate derived metrics
    - Deduplicate
    - Join with lookup tables
    """
    stage_config = config.get('stage_system', {})
    target_config = config.get('target_system', {})

    stage_table = f"{stage_config['database']}.{stage_config['schema']}.{stage_config['table']}"
    target_table = f"{target_config['database']}.{target_config['schema']}.{target_config['table']}"

    # Example transformation SQL (customize for your needs)
    insert_sql = f"""
        INSERT INTO {target_table}
        SELECT
            -- Add your column transformations here
            *
        FROM {stage_table}
        WHERE target_date = %(target_date)s
    """

    with sf_ops.SnowflakeConnection(config) as conn:
        cursor = conn.cursor()
        cursor.execute(insert_sql, {'target_date': target_date})
        conn.commit()

        rows_inserted = cursor.rowcount
        logger.info(f"Inserted {rows_inserted} records to target table")

        return rows_inserted


if __name__ == "__main__":
    print("Stage to Target Transfer script loaded")
    print("TODO: Implement your business-specific transformations")
