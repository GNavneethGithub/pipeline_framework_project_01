"""
Stale Pipeline Handling Phase User Script
==========================================
Detects and handles records stuck in 'in progress' state for too long.

Function Signature:
    stale_pipeline_handling(config: Dict, record: Dict) -> Dict

Return Values:
    {
        'skip_dag_run': bool,
        'error_message': str or None,
        'records_resolved': int,
        'stale_records_found': int
    }
"""

import logging
from typing import Dict, Any

from framework_scripts import snowflake_operations as sf_ops


logger = logging.getLogger(__name__)


def stale_pipeline_handling(config: Dict[str, Any], record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Detect and handle stale records in stage table.

    Args:
        config: Pipeline configuration
        record: Current drive table row

    Returns:
        dict: Stale handling result
    """
    logger.info("Starting stale pipeline handling")

    try:
        # Get timeout configuration
        phase_config = config.get('phases', {}).get('stale_pipeline_handling', {})
        timeout_minutes = phase_config.get('timeout_minutes', 120)

        logger.info(f"Timeout threshold: {timeout_minutes} minutes")

        # Find stale records
        stale_count = find_stale_records(config, timeout_minutes)
        logger.info(f"Found {stale_count} stale records")

        if stale_count > 0:
            # Resolve stale records
            resolved_count = resolve_stale_records(config, timeout_minutes)
            logger.info(f"Resolved {resolved_count} stale records")
        else:
            resolved_count = 0
            logger.info("No stale records to resolve")

        return {
            'skip_dag_run': False,
            'error_message': None,
            'records_resolved': resolved_count,
            'stale_records_found': stale_count,
            'timeout_minutes': timeout_minutes
        }

    except Exception as e:
        logger.error(f"Stale pipeline handling failed: {e}")
        return {
            'skip_dag_run': True,
            'error_message': str(e),
            'records_resolved': 0,
            'stale_records_found': 0
        }


def find_stale_records(config: Dict[str, Any], timeout_minutes: int) -> int:
    """Find records stuck in progress for too long."""
    stage_config = config.get('stage_system', {})
    full_table_name = f"{stage_config['database']}.{stage_config['schema']}.{stage_config['table']}"

    # Query for stale records (customize based on your table schema)
    count_sql = f"""
        SELECT COUNT(*)
        FROM {full_table_name}
        WHERE status = 'in_progress'
          AND modified_timestamp < DATEADD(minute, -{timeout_minutes}, CURRENT_TIMESTAMP())
    """

    with sf_ops.SnowflakeConnection(config) as conn:
        cursor = conn.cursor()

        try:
            cursor.execute(count_sql)
            result = cursor.fetchone()
            return result[0] if result else 0
        except Exception as e:
            # If table doesn't have status column, that's OK
            logger.info(f"No status tracking in stage table: {e}")
            return 0


def resolve_stale_records(config: Dict[str, Any], timeout_minutes: int) -> int:
    """Resolve stale records by marking them as failed or pending."""
    stage_config = config.get('stage_system', {})
    full_table_name = f"{stage_config['database']}.{stage_config['schema']}.{stage_config['table']}"

    # Update stale records (customize based on your needs)
    update_sql = f"""
        UPDATE {full_table_name}
        SET status = 'pending',
            modified_timestamp = CURRENT_TIMESTAMP()
        WHERE status = 'in_progress'
          AND modified_timestamp < DATEADD(minute, -{timeout_minutes}, CURRENT_TIMESTAMP())
    """

    with sf_ops.SnowflakeConnection(config) as conn:
        cursor = conn.cursor()

        try:
            cursor.execute(update_sql)
            conn.commit()
            return cursor.rowcount
        except Exception as e:
            logger.info(f"No status tracking in stage table: {e}")
            return 0


if __name__ == "__main__":
    print("Stale Pipeline Handling script loaded")
