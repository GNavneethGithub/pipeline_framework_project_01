"""
Framework Scripts Package
=========================
Core framework components for the data pipeline framework.

Modules:
    - duration_utils: Duration calculation and formatting
    - snowflake_operations: Snowflake database operations
    - error_handling: Error handling and classification
    - phase_executor: Phase execution orchestration
"""

from framework_scripts.duration_utils import (
    calculate_duration,
    format_duration,
    parse_duration,
    get_duration_seconds,
    is_duration_exceeded
)

from framework_scripts.snowflake_operations import (
    SnowflakeConnection,
    get_connection,
    initialize_pipeline_run,
    update_phase_variant,
    update_phase_arrays,
    finalize_pipeline_run,
    query_previous_runs,
    get_pipeline_run,
    get_count,
    record_to_dict
)

from framework_scripts.error_handling import (
    PipelineError,
    TransientError,
    PermanentError,
    ConfigurationError,
    handle_phase_error,
    create_error_message,
    determine_skip_dag,
    classify_error,
    log_to_variant,
    retry_with_backoff,
    create_phase_result
)

from framework_scripts.phase_executor import (
    PhaseExecutor,
    execute_pipeline
)

__all__ = [
    # duration_utils
    'calculate_duration',
    'format_duration',
    'parse_duration',
    'get_duration_seconds',
    'is_duration_exceeded',

    # snowflake_operations
    'SnowflakeConnection',
    'get_connection',
    'initialize_pipeline_run',
    'update_phase_variant',
    'update_phase_arrays',
    'finalize_pipeline_run',
    'query_previous_runs',
    'get_pipeline_run',
    'get_count',
    'record_to_dict',

    # error_handling
    'PipelineError',
    'TransientError',
    'PermanentError',
    'ConfigurationError',
    'handle_phase_error',
    'create_error_message',
    'determine_skip_dag',
    'classify_error',
    'log_to_variant',
    'retry_with_backoff',
    'create_phase_result',

    # phase_executor
    'PhaseExecutor',
    'execute_pipeline',
]

__version__ = '1.0.0'
