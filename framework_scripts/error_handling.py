"""
Error Handling Module
=====================
Provides consistent error handling patterns for pipeline framework.

Functions:
    - handle_phase_error: Process and log phase errors
    - create_error_message: Format error details
    - determine_skip_dag: Should pipeline stop?
    - log_to_variant: Store error in VARIANT
    - classify_error: Classify error as transient or permanent
"""

import logging
import traceback
import sys
from typing import Dict, Any, Optional, Tuple
from datetime import datetime


logger = logging.getLogger(__name__)


class PipelineError(Exception):
    """Base exception for pipeline errors."""
    pass


class TransientError(PipelineError):
    """Transient error that may succeed on retry."""
    pass


class PermanentError(PipelineError):
    """Permanent error that won't succeed on retry."""
    pass


class ConfigurationError(PipelineError):
    """Configuration error."""
    pass


def handle_phase_error(
    phase_name: str,
    error: Exception,
    config: Dict[str, Any],
    record: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Process and log phase errors.

    Args:
        phase_name: Name of the phase that failed
        error: Exception that occurred
        config: Pipeline configuration
        record: Drive table record

    Returns:
        dict: Standardized error response

    Example:
        >>> try:
        >>>     # Phase logic
        >>>     pass
        >>> except Exception as e:
        >>>     return handle_phase_error("source_count", e, config, record)
    """
    # Log the error
    logger.error(f"Phase {phase_name} failed: {str(error)}")
    logger.error(traceback.format_exc())

    # Classify error
    error_type = classify_error(error)

    # Create error message
    error_message = create_error_message(error, phase_name, error_type)

    # Determine if pipeline should stop
    skip_dag = determine_skip_dag(error, error_type)

    # Create response
    response = {
        'skip_dag_run': skip_dag,
        'error_message': error_message,
        'error_type': error_type,
        'phase_name': phase_name,
        'timestamp': datetime.now().isoformat()
    }

    # Add phase-specific fields based on phase type
    if 'count' in phase_name:
        response['count'] = None
    elif 'transfer' in phase_name:
        response['transfer_completed'] = False
    elif phase_name == 'audit':
        response['audit_passed'] = False
    elif 'cleaning' in phase_name:
        response['cleaning_completed'] = False

    return response


def create_error_message(
    error: Exception,
    phase_name: str,
    error_type: str
) -> str:
    """
    Format error details into user-friendly message.

    Args:
        error: Exception that occurred
        phase_name: Name of phase
        error_type: Error classification

    Returns:
        str: Formatted error message

    Example:
        >>> error_msg = create_error_message(
        >>>     ConnectionError("Timeout"),
        >>>     "source_count",
        >>>     "transient"
        >>> )
    """
    error_class = error.__class__.__name__
    error_str = str(error)

    message = f"[{error_type.upper()}] {phase_name} failed: {error_class}: {error_str}"

    # Add recommendations based on error type
    if error_type == 'transient':
        message += " | Recommendation: Retry the pipeline execution"
    elif error_type == 'permanent':
        message += " | Recommendation: Fix the underlying issue before retrying"
    elif error_type == 'configuration':
        message += " | Recommendation: Check pipeline configuration and credentials"

    return message


def determine_skip_dag(
    error: Exception,
    error_type: str
) -> bool:
    """
    Determine if pipeline should stop.

    Args:
        error: Exception that occurred
        error_type: Error classification

    Returns:
        bool: True if pipeline should stop, False to continue

    Example:
        >>> skip = determine_skip_dag(ConnectionError("Timeout"), "transient")
        >>> # Returns True - stop pipeline for retry
    """
    # Always stop on errors
    # Pipeline will retry on next execution
    return True


def classify_error(error: Exception) -> str:
    """
    Classify error as transient, permanent, or configuration.

    Args:
        error: Exception to classify

    Returns:
        str: Error classification (transient, permanent, configuration)

    Example:
        >>> classify_error(ConnectionError("Timeout"))
        'transient'
        >>> classify_error(ValueError("Invalid config"))
        'permanent'
    """
    error_class = error.__class__.__name__
    error_str = str(error).lower()

    # Transient errors (likely to succeed on retry)
    transient_indicators = [
        'timeout',
        'connection refused',
        'connection reset',
        'network',
        'temporary',
        'lock',
        'busy',
        'throttle',
        'rate limit',
        'unavailable'
    ]

    # Configuration errors
    config_indicators = [
        'authentication',
        'permission',
        'denied',
        'unauthorized',
        'credentials',
        'invalid password',
        'access denied'
    ]

    # Check for transient errors
    for indicator in transient_indicators:
        if indicator in error_str:
            return 'transient'

    # Check for configuration errors
    for indicator in config_indicators:
        if indicator in error_str:
            return 'configuration'

    # Check specific exception types
    if isinstance(error, TransientError):
        return 'transient'
    elif isinstance(error, PermanentError):
        return 'permanent'
    elif isinstance(error, ConfigurationError):
        return 'configuration'
    elif error_class in ['ConnectionError', 'TimeoutError', 'OSError']:
        return 'transient'
    elif error_class in ['ValueError', 'TypeError', 'KeyError', 'AttributeError']:
        return 'permanent'

    # Default to permanent for unknown errors
    return 'permanent'


def log_to_variant(
    phase_data: Dict[str, Any],
    error: Optional[Exception] = None
) -> Dict[str, Any]:
    """
    Prepare phase data for storage in VARIANT column.

    Args:
        phase_data: Base phase execution data
        error: Optional error to include

    Returns:
        dict: Complete phase data for VARIANT storage

    Example:
        >>> variant_data = log_to_variant(
        >>>     {
        >>>         "status": "FAILED",
        >>>         "start_timestamp": "2025-11-15T10:00:00Z",
        >>>         "end_timestamp": "2025-11-15T10:05:00Z"
        >>>     },
        >>>     ConnectionError("Timeout")
        >>> )
    """
    result = phase_data.copy()

    if error:
        error_type = classify_error(error)
        result['error_message'] = str(error)
        result['error_type'] = error_type
        result['error_class'] = error.__class__.__name__
        result['traceback'] = traceback.format_exc()

    return result


def retry_with_backoff(
    func,
    max_retries: int = 3,
    backoff_strategy: str = 'exponential',
    *args,
    **kwargs
) -> Any:
    """
    Retry a function with exponential backoff.

    Args:
        func: Function to retry
        max_retries: Maximum number of retry attempts
        backoff_strategy: 'exponential', 'linear', or 'none'
        *args: Function arguments
        **kwargs: Function keyword arguments

    Returns:
        Result of successful function call

    Raises:
        Exception: If all retries exhausted

    Example:
        >>> result = retry_with_backoff(
        >>>     query_source,
        >>>     max_retries=3,
        >>>     backoff_strategy='exponential',
        >>>     query_params={'window': '1h'}
        >>> )
    """
    import time

    last_error = None

    for attempt in range(max_retries + 1):
        try:
            result = func(*args, **kwargs)
            if attempt > 0:
                logger.info(f"Function succeeded on attempt {attempt + 1}")
            return result

        except Exception as e:
            last_error = e
            error_type = classify_error(e)

            # Don't retry permanent errors
            if error_type == 'permanent':
                logger.error(f"Permanent error detected, not retrying: {e}")
                raise

            if attempt < max_retries:
                # Calculate wait time
                if backoff_strategy == 'exponential':
                    wait_time = 2 ** attempt
                elif backoff_strategy == 'linear':
                    wait_time = 2
                else:  # none
                    wait_time = 0

                logger.warning(
                    f"Attempt {attempt + 1} failed: {e}. "
                    f"Retrying in {wait_time}s... "
                    f"({max_retries - attempt} retries remaining)"
                )

                if wait_time > 0:
                    time.sleep(wait_time)
            else:
                logger.error(f"All {max_retries + 1} attempts failed")
                raise

    # Should never reach here, but just in case
    if last_error:
        raise last_error


def create_phase_result(
    phase_name: str,
    status: str,
    start_timestamp: datetime,
    end_timestamp: datetime,
    expected_duration: Optional[str] = None,
    error_message: Optional[str] = None,
    **additional_data
) -> Dict[str, Any]:
    """
    Create standardized phase result dictionary.

    Args:
        phase_name: Name of the phase
        status: Phase status (COMPLETED, FAILED, SKIPPED)
        start_timestamp: When phase started
        end_timestamp: When phase ended
        expected_duration: Expected duration from config
        error_message: Error message if failed
        **additional_data: Phase-specific additional data

    Returns:
        dict: Standardized phase result

    Example:
        >>> result = create_phase_result(
        >>>     "source_count",
        >>>     "COMPLETED",
        >>>     datetime(2025, 11, 15, 10, 0, 0),
        >>>     datetime(2025, 11, 15, 10, 1, 30),
        >>>     expected_duration="2m",
        >>>     count=10000
        >>> )
    """
    from framework_scripts.duration_utils import calculate_duration, format_duration

    actual_duration = format_duration(calculate_duration(start_timestamp, end_timestamp))

    result = {
        'phase_name': phase_name,
        'status': status,
        'start_timestamp': start_timestamp.isoformat(),
        'end_timestamp': end_timestamp.isoformat(),
        'actual_run_duration': actual_duration,
        'expected_run_duration': expected_duration,
        'error_message': error_message
    }

    # Add additional phase-specific data
    result.update(additional_data)

    return result


# Example usage
if __name__ == "__main__":
    # Test error classification
    test_errors = [
        (ConnectionError("Connection timeout"), "transient"),
        (ValueError("Invalid value"), "permanent"),
        (PermissionError("Access denied"), "configuration"),
        (TimeoutError("Request timeout"), "transient"),
    ]

    print("Error Classification Tests:")
    for error, expected in test_errors:
        result = classify_error(error)
        status = "✓" if result == expected else "✗"
        print(f"{status} {error.__class__.__name__}: '{str(error)}' -> {result} (expected: {expected})")

    # Test error message creation
    print("\nError Message Tests:")
    for error, _ in test_errors:
        msg = create_error_message(error, "test_phase", classify_error(error))
        print(f"  {msg}")

    print("\nError handling module loaded successfully")
