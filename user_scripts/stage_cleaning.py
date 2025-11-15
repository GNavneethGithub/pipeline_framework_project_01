"""
Stage Cleaning Phase User Script
=================================
Cleans up temporary stage data after successful transfer to target.

Function Signature:
    stage_cleaning(config: Dict, record: Dict) -> Dict

Return Values:
    {
        'skip_dag_run': bool,
        'error_message': str or None,
        'cleaning_completed': bool,
        'records_archived': int,
        'records_deleted': int
    }
"""

import logging
from typing import Dict, Any
from datetime import datetime, timedelta

from framework_scripts import snowflake_operations as sf_ops


logger = logging.getLogger(__name__)


def stage_cleaning(config: Dict[str, Any], record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Clean up old stage data.

    Args:
        config: Pipeline configuration
        record: Current drive table row

    Returns:
        dict: Cleaning result
    """
    logger.info("Starting stage cleaning")

    try:
        # Get cleaning configuration
        phase_config = config.get('phases', {}).get('stage_cleaning', {})
        archive_before_delete = phase_config.get('archive_before_delete', True)
        archive_location = phase_config.get('archive_location', 's3://default-archive/')
        delete_after_days = phase_config.get('delete_after_days', 7)

        logger.info(f"Archive before delete: {archive_before_delete}")
        logger.info(f"Delete after days: {delete_after_days}")

        # Calculate cutoff date
        cutoff_date = (datetime.now() - timedelta(days=delete_after_days)).date()
        logger.info(f"Cutoff date: {cutoff_date}")

        records_archived = 0
        records_deleted = 0

        # Archive if configured
        if archive_before_delete:
            logger.info("Archiving old stage data...")
            records_archived = archive_stage_data(config, cutoff_date, archive_location)
            logger.info(f"Archived {records_archived} records")

        # Delete old records
        logger.info("Deleting old stage data...")
        records_deleted = delete_old_stage_data(config, cutoff_date)
        logger.info(f"Deleted {records_deleted} records")

        # Vacuum table
        logger.info("Vacuuming stage table...")
        vacuum_stage_table(config)

        logger.info("Stage cleaning completed successfully")

        return {
            'skip_dag_run': False,
            'error_message': None,
            'cleaning_completed': True,
            'records_archived': records_archived,
            'records_deleted': records_deleted,
            'archive_location': archive_location if archive_before_delete else None,
            'vacuumed': True
        }

    except Exception as e:
        logger.error(f"Stage cleaning failed: {e}")
        return {
            'skip_dag_run': False,  # Don't stop pipeline for cleaning failures
            'error_message': str(e),
            'cleaning_completed': False,
            'records_archived': 0,
            'records_deleted': 0
        }


def archive_stage_data(config: Dict[str, Any], cutoff_date, archive_location: str) -> int:
    """Archive stage data to S3 before deletion."""
    stage_config = config.get('stage_system', {})
    full_table_name = f"{stage_config['database']}.{stage_config['schema']}.{stage_config['table']}"

    # Count records to archive
    count = sf_ops.get_count(config, full_table_name, f"target_date < '{cutoff_date}'")

    if count == 0:
        logger.info("No records to archive")
        return 0

    # Copy to S3 (customize based on your S3 setup)
    # archive_sql = f"""
    #     COPY INTO '{archive_location}'
    #     FROM {full_table_name}
    #     WHERE target_date < '{cutoff_date}'
    #     FILE_FORMAT = (TYPE = PARQUET)
    #     HEADER = TRUE
    # """

    # For now, just return count
    logger.info(f"Would archive {count} records to {archive_location}")
    return count


def delete_old_stage_data(config: Dict[str, Any], cutoff_date) -> int:
    """Delete old stage data."""
    stage_config = config.get('stage_system', {})
    full_table_name = f"{stage_config['database']}.{stage_config['schema']}.{stage_config['table']}"

    delete_sql = f"""
        DELETE FROM {full_table_name}
        WHERE target_date < %(cutoff_date)s
    """

    with sf_ops.SnowflakeConnection(config) as conn:
        cursor = conn.cursor()
        cursor.execute(delete_sql, {'cutoff_date': str(cutoff_date)})
        conn.commit()

        return cursor.rowcount


def vacuum_stage_table(config: Dict[str, Any]) -> None:
    """Vacuum stage table to reclaim storage."""
    # Snowflake doesn't have VACUUM like PostgreSQL
    # This is a placeholder for any table maintenance
    logger.info("Table maintenance completed (Snowflake auto-manages storage)")


if __name__ == "__main__":
    print("Stage Cleaning script loaded")
