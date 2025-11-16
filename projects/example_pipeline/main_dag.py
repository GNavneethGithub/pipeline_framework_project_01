"""
Airflow DAG for Example Pipeline
=================================
This DAG orchestrates the execution of the example_pipeline data pipeline.

Pipeline: example_pipeline
Owner: Data Engineering Team
Schedule: Hourly (0 * * * *)
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from framework_scripts import (
    phase_executor,
    snowflake_operations as sf_ops,
    connectivity_checker,
    alerting,
    query_window_calculator as qw_calc
)
from config_handler_scripts import config_loader


logger = logging.getLogger(__name__)


# ============================================================================
# CONFIGURATION
# ============================================================================

# Load pipeline configuration
CONFIG_PATH = Path(__file__).parent / 'config.json'
config = config_loader.load_config(str(CONFIG_PATH))

# Extract DAG configuration
dag_config = config.get('dag_schedule', {})
pipeline_name = config.get('pipeline_metadata', {}).get('pipeline_name')

# DAG default arguments
default_args = {
    'owner': config.get('pipeline_metadata', {}).get('owner_name', 'Data Team'),
    'depends_on_past': False,
    'email': config.get('pipeline_metadata', {}).get('notification_emails', []),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': config.get('retry_configuration', {}).get('max_retries', 3),
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime.strptime(dag_config.get('start_date', '2025-11-15'), '%Y-%m-%d')
}


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_pipeline_id(execution_date: datetime) -> str:
    """
    Generate unique pipeline ID for this execution.

    Format: {pipeline_name}_{YYYYMMDD}_{HH}h_run{N}

    Args:
        execution_date: Airflow execution date

    Returns:
        str: Pipeline ID
    """
    date_str = execution_date.strftime('%Y%m%d')
    hour_str = execution_date.strftime('%H')

    # Check for previous runs to determine run number
    target_date = execution_date.date()
    previous_runs = sf_ops.query_previous_runs(config, pipeline_name, str(target_date))

    run_number = len(previous_runs) + 1

    return f"{pipeline_name}_{date_str}_{hour_str}h_run{run_number}"


def get_query_window(execution_date: datetime) -> tuple:
    """
    Calculate query window based on execution date and config.

    This function now uses the query_window_calculator module to:
    - Calculate windows with granularity and x_days_back constraints
    - Validate against acceptable_data_fetch boundaries
    - Ensure proper continuity by rounding to granularity

    Args:
        execution_date: Airflow execution date

    Returns:
        tuple: (window_start, window_end)
    """
    # Use the new query window calculator
    window_start, window_end = qw_calc.calculate_query_window(
        config=config,
        current_time=execution_date
    )

    return window_start, window_end


# ============================================================================
# TASK FUNCTIONS
# ============================================================================

def check_connectivity(**context):
    """
    Check connectivity to all required systems before starting the pipeline.

    This task checks connectivity to:
    - Snowflake
    - Source system
    - Stage area
    - Target system

    If any connectivity check fails, the DAG will be stopped.
    """
    logger.info("Checking connectivity to all required systems...")

    # Run connectivity checks
    result = connectivity_checker.check_all_connections(config)

    # Check if we should skip the DAG run
    if result.get('skip_dag_run', False):
        error_message = result.get('error_message', 'Connectivity check failed')
        logger.error(f"Connectivity check failed. Stopping DAG.")
        logger.error(error_message)

        # Send alert
        alerting.alerting_func(config, error_message)

        # Raise exception to stop DAG (Airflow will show this error)
        raise Exception(f"Connectivity check failed:\n{error_message}")

    logger.info("All connectivity checks passed. Proceeding with pipeline.")
    return result


def calculate_and_validate_query_window(**context):
    """
    Calculate query window and detect gaps.

    This task:
    1. Calculates the next query window based on config constraints
    2. Detects gaps between last completed run and current window
    3. Sends alerts and creates drive table entries for gaps
    4. Stores query window in XCom for downstream tasks

    If gaps are detected, they are logged and recorded but do not stop the DAG.
    """
    execution_date = context['execution_date']

    logger.info("=" * 80)
    logger.info("Calculating query window and detecting gaps...")
    logger.info("=" * 80)

    # Calculate query window
    try:
        window_start, window_end = get_query_window(execution_date)

        logger.info(f"Query window calculated:")
        logger.info(f"  Start: {window_start}")
        logger.info(f"  End:   {window_end}")

        # Store in XCom for downstream tasks
        context['task_instance'].xcom_push(key='query_window_start', value=window_start.isoformat())
        context['task_instance'].xcom_push(key='query_window_end', value=window_end.isoformat())

    except Exception as e:
        error_message = f"Failed to calculate query window: {str(e)}"
        logger.error(error_message)
        alerting.alerting_func(config, error_message)
        raise Exception(error_message)

    # Detect and handle gaps
    try:
        query_window_config = config.get('query_window', {})
        granularity_str = query_window_config.get('granularity', '1h')
        granularity = qw_calc.parse_time_duration(granularity_str)

        gap_result = qw_calc.handle_gaps(
            config=config,
            next_window_start=window_start,
            granularity=granularity
        )

        if gap_result['gap_detected']:
            logger.warning(f"Gap detected! {gap_result['gap_count']} intervals missing")
            logger.warning(f"Created {len(gap_result['drive_table_entries_created'])} drive table entries for gaps")
        else:
            logger.info("No gaps detected. Pipeline continuity maintained.")

        # Store gap result in XCom
        context['task_instance'].xcom_push(key='gap_result', value=gap_result)

    except Exception as e:
        # Log but don't fail the DAG - gap detection is informational
        logger.error(f"Gap detection failed: {str(e)}")
        logger.warning("Continuing with pipeline execution despite gap detection failure")

    logger.info("=" * 80)

    return {
        'query_window_start': window_start,
        'query_window_end': window_end,
        'gap_result': gap_result if 'gap_result' in locals() else None
    }


def initialize_pipeline(**context):
    """Initialize pipeline execution in drive table."""
    execution_date = context['execution_date']

    logger.info(f"Initializing pipeline: {pipeline_name}")
    logger.info(f"Execution date: {execution_date}")

    # Generate pipeline ID
    pipeline_id = get_pipeline_id(execution_date)
    logger.info(f"Pipeline ID: {pipeline_id}")

    # Get query window from XCom (calculated in previous task)
    window_start_str = context['task_instance'].xcom_pull(
        task_ids='calculate_query_window',
        key='query_window_start'
    )
    window_end_str = context['task_instance'].xcom_pull(
        task_ids='calculate_query_window',
        key='query_window_end'
    )

    # Convert back to datetime
    from datetime import datetime
    window_start = datetime.fromisoformat(window_start_str)
    window_end = datetime.fromisoformat(window_end_str)

    logger.info(f"Query window: {window_start} to {window_end}")

    # Initialize in drive table
    record = sf_ops.initialize_pipeline_run(
        config=config,
        pipeline_id=pipeline_id,
        query_window_start=window_start,
        query_window_end=window_end,
        retry_number=0  # TODO: Detect retry number
    )

    logger.info(f"Pipeline initialized successfully")

    # Store pipeline_id in XCom for downstream tasks
    context['task_instance'].xcom_push(key='pipeline_id', value=pipeline_id)

    return pipeline_id


def execute_phase(phase_name: str, **context):
    """
    Execute a pipeline phase.

    Args:
        phase_name: Name of phase to execute
        **context: Airflow context
    """
    # Get pipeline_id from XCom
    pipeline_id = context['task_instance'].xcom_pull(
        task_ids='initialize_pipeline',
        key='pipeline_id'
    )

    logger.info(f"Executing phase: {phase_name}")
    logger.info(f"Pipeline ID: {pipeline_id}")

    # Create phase executor
    executor = phase_executor.PhaseExecutor(config, pipeline_id)

    # Execute phase
    result = executor.execute_phase(phase_name)

    # Check if pipeline should stop
    if result.get('skip_dag_run', False):
        logger.error(f"Phase {phase_name} failed, stopping pipeline")
        logger.error(f"Error: {result.get('error_message')}")

        # Finalize pipeline as FAILED
        sf_ops.finalize_pipeline_run(config, pipeline_id, 'FAILED')

        raise Exception(f"Phase {phase_name} failed: {result.get('error_message')}")

    logger.info(f"Phase {phase_name} completed successfully")

    return result


def finalize_pipeline(**context):
    """Finalize pipeline execution."""
    # Get pipeline_id from XCom
    pipeline_id = context['task_instance'].xcom_pull(
        task_ids='initialize_pipeline',
        key='pipeline_id'
    )

    logger.info(f"Finalizing pipeline: {pipeline_id}")

    # Finalize as SUCCESS (if we got here, all phases passed)
    sf_ops.finalize_pipeline_run(config, pipeline_id, 'SUCCESS')

    logger.info(f"Pipeline {pipeline_id} completed successfully")


# ============================================================================
# DAG DEFINITION
# ============================================================================

# Get failure callback function
failure_callback = alerting.get_alerting_callback(config)

with DAG(
    dag_id=f'pipeline_{pipeline_name}',
    default_args=default_args,
    description=config.get('pipeline_metadata', {}).get('pipeline_description'),
    schedule_interval=dag_config.get('cron_expression', '0 * * * *'),
    catchup=dag_config.get('catchup', False),
    tags=['data_pipeline', pipeline_name]
) as dag:

    # Check connectivity to all required systems
    connectivity_check_task = PythonOperator(
        task_id='check_connectivity',
        python_callable=check_connectivity,
        provide_context=True,
        on_failure_callback=failure_callback
    )

    # Calculate query window and detect gaps
    calculate_query_window_task = PythonOperator(
        task_id='calculate_query_window',
        python_callable=calculate_and_validate_query_window,
        provide_context=True,
        on_failure_callback=failure_callback
    )

    # Initialize pipeline
    init_task = PythonOperator(
        task_id='initialize_pipeline',
        python_callable=initialize_pipeline,
        provide_context=True,
        on_failure_callback=failure_callback
    )

    # Phase 1: Stale Pipeline Handling
    stale_handling_task = PythonOperator(
        task_id='stale_pipeline_handling',
        python_callable=execute_phase,
        op_kwargs={'phase_name': 'stale_pipeline_handling'},
        provide_context=True,
        on_failure_callback=failure_callback
    )

    # Phase 2: Pre-Validation
    pre_validation_task = PythonOperator(
        task_id='pre_validation',
        python_callable=execute_phase,
        op_kwargs={'phase_name': 'pre_validation'},
        provide_context=True,
        on_failure_callback=failure_callback
    )

    # Phase 3-4: Source to Stage Transfer
    source_to_stage_task = PythonOperator(
        task_id='source_to_stage_transfer',
        python_callable=execute_phase,
        op_kwargs={'phase_name': 'source_to_stage_transfer'},
        provide_context=True,
        on_failure_callback=failure_callback
    )

    # Phase 5-6: Stage to Target Transfer
    stage_to_target_task = PythonOperator(
        task_id='stage_to_target_transfer',
        python_callable=execute_phase,
        op_kwargs={'phase_name': 'stage_to_target_transfer'},
        provide_context=True,
        on_failure_callback=failure_callback
    )

    # Phase 7: Audit
    audit_task = PythonOperator(
        task_id='audit',
        python_callable=execute_phase,
        op_kwargs={'phase_name': 'audit'},
        provide_context=True,
        on_failure_callback=failure_callback
    )

    # Phase 8: Stage Cleaning
    stage_cleaning_task = PythonOperator(
        task_id='stage_cleaning',
        python_callable=execute_phase,
        op_kwargs={'phase_name': 'stage_cleaning'},
        provide_context=True,
        on_failure_callback=failure_callback
    )

    # Phase 9: Target Cleaning (optional)
    target_cleaning_task = PythonOperator(
        task_id='target_cleaning',
        python_callable=execute_phase,
        op_kwargs={'phase_name': 'target_cleaning'},
        provide_context=True,
        on_failure_callback=failure_callback
    )

    # Finalize
    finalize_task = PythonOperator(
        task_id='finalize_pipeline',
        python_callable=finalize_pipeline,
        provide_context=True,
        on_failure_callback=failure_callback
    )

    # Define task dependencies
    # First check connectivity, then calculate query window and detect gaps, then initialize, then run phases
    connectivity_check_task >> calculate_query_window_task >> init_task >> stale_handling_task >> pre_validation_task
    pre_validation_task >> source_to_stage_task >> stage_to_target_task
    stage_to_target_task >> audit_task
    audit_task >> stage_cleaning_task >> target_cleaning_task
    target_cleaning_task >> finalize_task


# ============================================================================
# END OF DAG
# ============================================================================

if __name__ == "__main__":
    print(f"DAG ID: pipeline_{pipeline_name}")
    print(f"Schedule: {dag_config.get('cron_expression')}")
    print(f"Owner: {config.get('pipeline_metadata', {}).get('owner_name')}")
    print(f"Tasks: {len(dag.tasks)}")
