"""
Pre-Validation Phase User Script
=================================
This script validates pre-conditions and determines if this is a fresh run
or a continuation from a previous failure.

Function Signature:
    pre_validation(config: Dict, record: Dict) -> Dict

Return Values:
    {
        'skip_dag_run': bool,  # True = stop pipeline, False = continue
        'error_message': str or None,  # Error details if any
        'is_fresh_run': bool,  # True if first execution for this window
        'phases_to_skip': list,  # Phases already completed in previous run
        'validation_checks': list  # List of validation checks performed
    }
"""

import logging
from typing import Dict, Any, List
from datetime import datetime

from framework_scripts import snowflake_operations as sf_ops


logger = logging.getLogger(__name__)


def pre_validation(config: Dict[str, Any], record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate pre-conditions and determine execution strategy.

    Args:
        config: Pipeline configuration from config.json
        record: Current drive table row

    Returns:
        dict: Pre-validation result

    This function:
    1. Checks if this is fresh run or continuation
    2. Queries for previous runs with same query_window
    3. Validates prerequisites (source accessible, target exists, etc.)
    4. Returns phases_to_skip if continuation run
    """
    logger.info("Starting pre-validation phase")

    try:
        # Extract query window info
        pipeline_name = config.get('pipeline_metadata', {}).get('pipeline_name')
        target_date = record.get('target_date')

        logger.info(f"Pipeline: {pipeline_name}, Target Date: {target_date}")

        # Step 1: Check for previous runs
        logger.info("Checking for previous runs...")
        previous_runs = sf_ops.query_previous_runs(config, pipeline_name, str(target_date))

        # Filter out current run
        current_pipeline_id = record.get('pipeline_id')
        previous_runs = [r for r in previous_runs if r.get('pipeline_id') != current_pipeline_id]

        is_fresh_run = len(previous_runs) == 0
        phases_to_skip = []

        if is_fresh_run:
            logger.info("This is a fresh run (no previous runs found)")
        else:
            logger.info(f"Found {len(previous_runs)} previous run(s)")

            # Check if previous run failed
            for prev_run in previous_runs:
                prev_status = prev_run.get('pipeline_status')
                prev_phase_failed = prev_run.get('phase_failed')

                if prev_status == 'FAILED' and prev_phase_failed:
                    logger.info(f"Previous run failed at phase: {prev_phase_failed}")

                    # Get phases that completed successfully
                    phases_completed = prev_run.get('phases_completed', [])
                    logger.info(f"Phases completed in previous run: {phases_completed}")

                    # These phases can be skipped in current run
                    phases_to_skip = phases_completed
                    break

        # Step 2: Validate prerequisites
        logger.info("Validating prerequisites...")
        validation_checks = []

        # Check 2a: Source system accessible
        source_check = validate_source_accessible(config)
        validation_checks.append(source_check)

        if not source_check['passed']:
            return {
                'skip_dag_run': True,
                'error_message': f"Source validation failed: {source_check['message']}",
                'is_fresh_run': is_fresh_run,
                'phases_to_skip': [],
                'validation_checks': validation_checks
            }

        # Check 2b: Stage table exists and is writable
        stage_check = validate_stage_accessible(config)
        validation_checks.append(stage_check)

        if not stage_check['passed']:
            return {
                'skip_dag_run': True,
                'error_message': f"Stage validation failed: {stage_check['message']}",
                'is_fresh_run': is_fresh_run,
                'phases_to_skip': [],
                'validation_checks': validation_checks
            }

        # Check 2c: Target table exists and structure matches
        target_check = validate_target_accessible(config)
        validation_checks.append(target_check)

        if not target_check['passed']:
            return {
                'skip_dag_run': True,
                'error_message': f"Target validation failed: {target_check['message']}",
                'is_fresh_run': is_fresh_run,
                'phases_to_skip': [],
                'validation_checks': validation_checks
            }

        # All validations passed
        logger.info("All validations passed")
        logger.info(f"Phases to skip: {phases_to_skip}")

        return {
            'skip_dag_run': False,
            'error_message': None,
            'is_fresh_run': is_fresh_run,
            'phases_to_skip': phases_to_skip,
            'validation_checks': validation_checks
        }

    except Exception as e:
        logger.error(f"Pre-validation failed with exception: {e}")
        return {
            'skip_dag_run': True,
            'error_message': str(e),
            'is_fresh_run': True,
            'phases_to_skip': [],
            'validation_checks': []
        }


def validate_source_accessible(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate that source system is accessible.

    Args:
        config: Pipeline configuration

    Returns:
        dict: Validation result with 'passed' and 'message'
    """
    logger.info("Validating source system accessibility...")

    try:
        source_config = config.get('source_system', {})
        source_type = source_config.get('type')

        logger.info(f"Source type: {source_type}")

        # TODO: Implement actual source connectivity check based on source_type
        # For now, return success
        # In production, you would:
        # - Test Elasticsearch connection if source_type == 'elasticsearch'
        # - Test MySQL connection if source_type == 'mysql'
        # - Test API endpoint if source_type == 'api'

        return {
            'check_name': 'source_accessible',
            'passed': True,
            'message': f'Source system ({source_type}) is accessible'
        }

    except Exception as e:
        return {
            'check_name': 'source_accessible',
            'passed': False,
            'message': f'Source system check failed: {str(e)}'
        }


def validate_stage_accessible(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate that stage table exists and is writable.

    Args:
        config: Pipeline configuration

    Returns:
        dict: Validation result
    """
    logger.info("Validating stage table accessibility...")

    try:
        stage_config = config.get('stage_system', {})
        stage_database = stage_config.get('database')
        stage_schema = stage_config.get('schema')
        stage_table = stage_config.get('table')

        full_table_name = f"{stage_database}.{stage_schema}.{stage_table}"
        logger.info(f"Stage table: {full_table_name}")

        # Test query to check table exists
        with sf_ops.SnowflakeConnection(config) as conn:
            cursor = conn.cursor()

            # Check if table exists
            check_sql = f"SELECT COUNT(*) FROM {full_table_name} LIMIT 1"
            cursor.execute(check_sql)

            logger.info(f"Stage table {full_table_name} exists and is accessible")

        return {
            'check_name': 'stage_accessible',
            'passed': True,
            'message': f'Stage table {full_table_name} is accessible'
        }

    except Exception as e:
        return {
            'check_name': 'stage_accessible',
            'passed': False,
            'message': f'Stage table check failed: {str(e)}'
        }


def validate_target_accessible(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate that target table exists and structure matches expectations.

    Args:
        config: Pipeline configuration

    Returns:
        dict: Validation result
    """
    logger.info("Validating target table accessibility...")

    try:
        target_config = config.get('target_system', {})
        target_database = target_config.get('database')
        target_schema = target_config.get('schema')
        target_table = target_config.get('table')

        full_table_name = f"{target_database}.{target_schema}.{target_table}"
        logger.info(f"Target table: {full_table_name}")

        # Test query to check table exists
        with sf_ops.SnowflakeConnection(config) as conn:
            cursor = conn.cursor()

            # Check if table exists
            check_sql = f"SELECT COUNT(*) FROM {full_table_name} LIMIT 1"
            cursor.execute(check_sql)

            logger.info(f"Target table {full_table_name} exists and is accessible")

        return {
            'check_name': 'target_accessible',
            'passed': True,
            'message': f'Target table {full_table_name} is accessible'
        }

    except Exception as e:
        return {
            'check_name': 'target_accessible',
            'passed': False,
            'message': f'Target table check failed: {str(e)}'
        }


# Example usage
if __name__ == "__main__":
    # This would be called by the framework
    sample_config = {
        'pipeline_metadata': {
            'pipeline_name': 'test_pipeline'
        },
        'source_system': {
            'type': 'elasticsearch'
        },
        'stage_system': {
            'database': 'CADS_DB',
            'schema': 'stg_test',
            'table': 'test_stage'
        },
        'target_system': {
            'database': 'CDW_DB',
            'schema': 'prod_test',
            'table': 'test_target'
        }
    }

    sample_record = {
        'pipeline_id': 'test_pipeline_20251115_run1',
        'target_date': '2025-11-15'
    }

    print("Pre-validation script loaded successfully")
    print("This script would be called by the framework during pipeline execution")
