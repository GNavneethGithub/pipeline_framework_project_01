"""
Query Window Calculator Module
===============================
Calculates query windows with granularity, x_days_back constraints,
boundary validation, and gap detection.

Functions:
    - parse_time_duration: Parse duration strings (e.g., "1h", "2d", "30s", "1month")
    - round_to_granularity: Round timestamp to nearest granularity multiple
    - calculate_query_window: Calculate next query window with constraints
    - detect_gaps: Find gaps between last completed run and current window
    - create_gap_intervals: Generate list of gap intervals
    - handle_gaps: Detect gaps, alert, and create drive table entries
"""

import re
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from zoneinfo import ZoneInfo


logger = logging.getLogger(__name__)


def parse_time_duration(duration_str: str) -> timedelta:
    """
    Parse time duration string to timedelta.

    Supports formats:
    - Seconds: "30s", "45sec", "100millisec"
    - Minutes: "30m", "45min"
    - Hours: "1h", "2hour"
    - Days: "1d", "7day"
    - Weeks: "1w", "2week"
    - Months: "1month" (approximated as 30 days)

    Args:
        duration_str: Duration string (e.g., "1h", "30m", "2d", "1month")

    Returns:
        timedelta: Parsed duration

    Raises:
        ValueError: If duration string is invalid

    Example:
        >>> parse_time_duration("1h")
        timedelta(hours=1)
        >>> parse_time_duration("30m")
        timedelta(minutes=30)
        >>> parse_time_duration("2d")
        timedelta(days=2)
        >>> parse_time_duration("1month")
        timedelta(days=30)
    """
    if not duration_str or not isinstance(duration_str, str):
        raise ValueError(f"Duration string cannot be empty or non-string: {duration_str}")

    duration_str = duration_str.strip().lower()

    # Pattern to match number and unit
    # Examples: "30s", "1h", "2d", "1month", "45millisec"
    pattern = r'^(\d+(?:\.\d+)?)\s*(millisec|ms|sec|s|min|m|hour|h|day|d|week|w|month)s?$'
    match = re.match(pattern, duration_str)

    if not match:
        raise ValueError(
            f"Invalid duration format: '{duration_str}'. "
            f"Expected format: <number><unit> (e.g., '1h', '30m', '2d', '1month')"
        )

    value = float(match.group(1))
    unit = match.group(2)

    # Convert to timedelta
    if unit in ('millisec', 'ms'):
        return timedelta(milliseconds=value)
    elif unit in ('sec', 's'):
        return timedelta(seconds=value)
    elif unit in ('min', 'm'):
        return timedelta(minutes=value)
    elif unit in ('hour', 'h'):
        return timedelta(hours=value)
    elif unit in ('day', 'd'):
        return timedelta(days=value)
    elif unit in ('week', 'w'):
        return timedelta(weeks=value)
    elif unit == 'month':
        # Approximate month as 30 days
        return timedelta(days=value * 30)
    else:
        raise ValueError(f"Unknown duration unit: {unit}")


def round_to_granularity(
    timestamp: datetime,
    granularity: timedelta,
    direction: str = 'down'
) -> datetime:
    """
    Round timestamp to nearest granularity multiple.

    This ensures continuity by aligning timestamps to granularity boundaries.

    Args:
        timestamp: Timestamp to round
        granularity: Granularity interval
        direction: 'down' (floor), 'up' (ceil), or 'nearest'

    Returns:
        datetime: Rounded timestamp

    Example:
        >>> dt = datetime(2025, 11, 16, 10, 37, 42)
        >>> round_to_granularity(dt, timedelta(hours=1), 'down')
        datetime(2025, 11, 16, 10, 0, 0)
        >>> round_to_granularity(dt, timedelta(minutes=15), 'down')
        datetime(2025, 11, 16, 10, 30, 0)
    """
    if not granularity or granularity.total_seconds() <= 0:
        raise ValueError("Granularity must be positive")

    # Get granularity in seconds
    granularity_seconds = granularity.total_seconds()

    # Convert timestamp to seconds since epoch (use UTC)
    if timestamp.tzinfo is None:
        # Assume UTC if no timezone
        timestamp = timestamp.replace(tzinfo=ZoneInfo('UTC'))

    epoch = datetime(1970, 1, 1, tzinfo=ZoneInfo('UTC'))
    seconds_since_epoch = (timestamp - epoch).total_seconds()

    # Round to granularity
    if direction == 'down':
        rounded_seconds = (seconds_since_epoch // granularity_seconds) * granularity_seconds
    elif direction == 'up':
        rounded_seconds = ((seconds_since_epoch + granularity_seconds - 1) // granularity_seconds) * granularity_seconds
    elif direction == 'nearest':
        rounded_seconds = round(seconds_since_epoch / granularity_seconds) * granularity_seconds
    else:
        raise ValueError(f"Invalid direction: {direction}. Must be 'down', 'up', or 'nearest'")

    # Convert back to datetime
    rounded_timestamp = epoch + timedelta(seconds=rounded_seconds)

    # Preserve original timezone if it was set
    if timestamp.tzinfo:
        rounded_timestamp = rounded_timestamp.astimezone(timestamp.tzinfo)

    return rounded_timestamp


def calculate_query_window(
    config: dict,
    current_time: Optional[datetime] = None
) -> Tuple[datetime, datetime]:
    """
    Calculate query window based on config constraints.

    This function:
    1. Rounds current_time to granularity multiple
    2. Calculates query_window_start based on last completed run
    3. Validates against x_days_back constraint
    4. Validates against acceptable_data_fetch boundaries
    5. Returns (window_start, window_end)

    Args:
        config: Pipeline configuration containing:
            - query_window.granularity: Query window size (e.g., "1h")
            - query_window.x_days_back: Max lookback limit (e.g., "7d")
            - query_window.acceptable_data_fetch_start_time: ISO string (optional)
            - query_window.acceptable_data_fetch_end_time: ISO string or None (optional)
        current_time: Current time (defaults to now)

    Returns:
        tuple: (query_window_start, query_window_end)

    Raises:
        ValueError: If constraints cannot be satisfied

    Example:
        >>> config = {
        ...     'query_window': {
        ...         'granularity': '1h',
        ...         'x_days_back': '7d',
        ...         'acceptable_data_fetch_start_time': '2025-01-01T00:00:00+00:00'
        ...     }
        ... }
        >>> start, end = calculate_query_window(config)
    """
    query_window_config = config.get('query_window', {})

    # Get granularity
    granularity_str = query_window_config.get('granularity', '1h')
    granularity = parse_time_duration(granularity_str)

    # Get x_days_back
    x_days_back_str = query_window_config.get('x_days_back', '0d')
    x_days_back = parse_time_duration(x_days_back_str)

    # Get current time and round to granularity
    if current_time is None:
        current_time = datetime.now(ZoneInfo('UTC'))
    elif current_time.tzinfo is None:
        current_time = current_time.replace(tzinfo=ZoneInfo('UTC'))

    current_time_rounded = round_to_granularity(current_time, granularity, direction='down')

    logger.info(f"Current time (rounded): {current_time_rounded}")
    logger.info(f"Granularity: {granularity_str}")
    logger.info(f"X days back: {x_days_back_str}")

    # Calculate earliest allowed start time based on x_days_back
    earliest_allowed_start = current_time_rounded - x_days_back

    # Get acceptable data fetch boundaries from config
    acceptable_start_str = query_window_config.get('acceptable_data_fetch_start_time')
    acceptable_end_str = query_window_config.get('acceptable_data_fetch_end_time')

    acceptable_start_time = None
    acceptable_end_time = None

    if acceptable_start_str:
        acceptable_start_time = datetime.fromisoformat(acceptable_start_str.replace('Z', '+00:00'))
        logger.info(f"Acceptable data fetch start time: {acceptable_start_time}")

    if acceptable_end_str:
        acceptable_end_time = datetime.fromisoformat(acceptable_end_str.replace('Z', '+00:00'))
        logger.info(f"Acceptable data fetch end time: {acceptable_end_time}")
    else:
        logger.info("Acceptable data fetch end time: None (no limit)")

    # Get last completed pipeline run to determine next window start
    pipeline_name = config.get('pipeline_metadata', {}).get('pipeline_name')

    if pipeline_name:
        # Query for last successful run
        last_run = _get_last_completed_run(config, pipeline_name)

        if last_run:
            # Next window starts where last one ended
            window_start = last_run['query_window_end_timestamp']
            logger.info(f"Last completed run found: {last_run['pipeline_id']}")
            logger.info(f"Last query window end: {window_start}")
        else:
            # No previous run, start from acceptable_start_time or earliest_allowed_start
            if acceptable_start_time:
                window_start = acceptable_start_time
            else:
                window_start = earliest_allowed_start
            logger.info(f"No previous runs found, starting from: {window_start}")
    else:
        # No pipeline name, start from earliest_allowed_start
        window_start = earliest_allowed_start
        logger.info(f"No pipeline name configured, starting from: {window_start}")

    # Ensure window_start is timezone-aware
    if window_start.tzinfo is None:
        window_start = window_start.replace(tzinfo=ZoneInfo('UTC'))

    # Round window_start to granularity
    window_start = round_to_granularity(window_start, granularity, direction='down')

    # Validate against x_days_back constraint
    if window_start < earliest_allowed_start:
        logger.warning(
            f"Window start {window_start} is before earliest allowed time {earliest_allowed_start} "
            f"(current_time - x_days_back). Adjusting to earliest allowed time."
        )
        window_start = earliest_allowed_start

    # Validate against acceptable_data_fetch_start_time
    if acceptable_start_time and window_start < acceptable_start_time:
        logger.warning(
            f"Window start {window_start} is before acceptable_data_fetch_start_time {acceptable_start_time}. "
            f"Adjusting to acceptable start time."
        )
        window_start = acceptable_start_time

    # Calculate window_end
    window_end = window_start + granularity

    # Validate against current time (can't fetch data from the future)
    if window_end > current_time_rounded:
        logger.warning(
            f"Window end {window_end} is in the future (current time: {current_time_rounded}). "
            f"Adjusting to current time."
        )
        window_end = current_time_rounded

    # Validate against acceptable_data_fetch_end_time
    if acceptable_end_time and window_end > acceptable_end_time:
        logger.warning(
            f"Window end {window_end} is after acceptable_data_fetch_end_time {acceptable_end_time}. "
            f"Adjusting to acceptable end time."
        )
        window_end = acceptable_end_time

    # Validate that we have a valid window
    if window_start >= window_end:
        raise ValueError(
            f"Invalid query window: start ({window_start}) >= end ({window_end}). "
            f"This may indicate that all data has been fetched or constraints are too restrictive."
        )

    logger.info(f"Calculated query window: {window_start} to {window_end}")

    return window_start, window_end


def detect_gaps(
    config: dict,
    next_window_start: datetime,
    granularity: timedelta
) -> List[Tuple[datetime, datetime]]:
    """
    Detect gaps between last completed run and next window start.

    Args:
        config: Pipeline configuration
        next_window_start: Start of next query window
        granularity: Query window granularity

    Returns:
        list: List of gap intervals as (start, end) tuples

    Example:
        >>> gaps = detect_gaps(config, next_window_start, timedelta(hours=1))
        >>> # Returns: [(t1, t2), (t2, t3), (t3, t4)] if there are 3 hours of gap
    """
    pipeline_name = config.get('pipeline_metadata', {}).get('pipeline_name')

    if not pipeline_name:
        logger.warning("No pipeline name configured, cannot detect gaps")
        return []

    # Get last completed run
    last_run = _get_last_completed_run(config, pipeline_name)

    if not last_run:
        logger.info("No previous runs found, no gaps to detect")
        return []

    last_window_end = last_run['query_window_end_timestamp']

    # Ensure timezone-aware
    if last_window_end.tzinfo is None:
        last_window_end = last_window_end.replace(tzinfo=ZoneInfo('UTC'))
    if next_window_start.tzinfo is None:
        next_window_start = next_window_start.replace(tzinfo=ZoneInfo('UTC'))

    # Check if there's a gap
    if last_window_end >= next_window_start:
        logger.info("No gap detected, last window end >= next window start")
        return []

    # Calculate gap intervals
    gap_intervals = []
    current_start = last_window_end

    while current_start < next_window_start:
        current_end = current_start + granularity
        if current_end > next_window_start:
            current_end = next_window_start

        gap_intervals.append((current_start, current_end))
        current_start = current_end

    logger.info(f"Detected {len(gap_intervals)} gap intervals")

    return gap_intervals


def create_gap_intervals(
    gap_intervals: List[Tuple[datetime, datetime]]
) -> List[str]:
    """
    Format gap intervals for alert message.

    Args:
        gap_intervals: List of (start, end) tuples

    Returns:
        list: Formatted interval strings

    Example:
        >>> intervals = [(t1, t2), (t2, t3)]
        >>> create_gap_intervals(intervals)
        ['[2025-11-16T10:00:00+00:00, 2025-11-16T11:00:00+00:00]', ...]
    """
    formatted_intervals = []

    for start, end in gap_intervals:
        formatted_intervals.append(f"[{start.isoformat()}, {end.isoformat()}]")

    return formatted_intervals


def handle_gaps(
    config: dict,
    next_window_start: datetime,
    granularity: timedelta
) -> Dict[str, Any]:
    """
    Detect gaps, send alert, and create drive table entries.

    This function:
    1. Detects gaps between last completed run and next window
    2. Sends email alert with gap intervals
    3. Creates drive table rows for each gap interval with default values

    Args:
        config: Pipeline configuration
        next_window_start: Start of next query window
        granularity: Query window granularity

    Returns:
        dict: {
            'gap_detected': bool,
            'gap_count': int,
            'gap_intervals': list,
            'drive_table_entries_created': list
        }

    Example:
        >>> result = handle_gaps(config, next_window_start, granularity)
        >>> if result['gap_detected']:
        ...     print(f"Found {result['gap_count']} gap intervals")
    """
    # Import here to avoid circular dependency and optional imports
    from framework_scripts import snowflake_operations as sf_ops
    from framework_scripts import alerting

    # Detect gaps
    gap_intervals = detect_gaps(config, next_window_start, granularity)

    result = {
        'gap_detected': len(gap_intervals) > 0,
        'gap_count': len(gap_intervals),
        'gap_intervals': gap_intervals,
        'drive_table_entries_created': []
    }

    if not gap_intervals:
        logger.info("No gaps detected")
        return result

    # Format gap intervals for alert
    formatted_intervals = create_gap_intervals(gap_intervals)

    # Send email alert
    pipeline_name = config.get('pipeline_metadata', {}).get('pipeline_name', 'Unknown Pipeline')

    alert_message = f"""
Gap Detected in Pipeline Execution
===================================

Pipeline: {pipeline_name}
Gap Count: {len(gap_intervals)}

Gap Intervals:
--------------
{chr(10).join(formatted_intervals)}

Action Taken:
-------------
Created {len(gap_intervals)} drive table entries for gap intervals with status 'GAP_DETECTED'.

These intervals were skipped and need manual investigation.

This is an automated alert from the data pipeline framework.
"""

    logger.warning(f"Gap detected! {len(gap_intervals)} intervals missing")
    logger.warning(alert_message)

    # Send alert
    try:
        alerting.alerting_func(config, alert_message)
    except Exception as e:
        logger.error(f"Failed to send gap alert: {str(e)}")

    # Create drive table entries for gap intervals
    for i, (gap_start, gap_end) in enumerate(gap_intervals):
        try:
            # Generate pipeline ID for gap
            gap_pipeline_id = f"{pipeline_name}_gap_{gap_start.strftime('%Y%m%d_%H%M%S')}_to_{gap_end.strftime('%H%M%S')}"

            # Create drive table entry
            record = sf_ops.initialize_pipeline_run(
                config=config,
                pipeline_id=gap_pipeline_id,
                query_window_start=gap_start,
                query_window_end=gap_end,
                retry_number=0
            )

            # Update status to GAP_DETECTED
            update_sql = """
                UPDATE pipeline_execution_drive
                SET pipeline_status = 'GAP_DETECTED',
                    pipeline_end_timestamp = CURRENT_TIMESTAMP(),
                    updated_at = CURRENT_TIMESTAMP()
                WHERE pipeline_id = %(pipeline_id)s
            """

            with sf_ops.SnowflakeConnection(config) as conn:
                cursor = conn.cursor()
                cursor.execute(update_sql, {'pipeline_id': gap_pipeline_id})
                conn.commit()

            result['drive_table_entries_created'].append(gap_pipeline_id)

            logger.info(f"Created drive table entry for gap interval {i+1}/{len(gap_intervals)}: {gap_pipeline_id}")

        except Exception as e:
            logger.error(f"Failed to create drive table entry for gap interval {i+1}: {str(e)}")

    return result


def _get_last_completed_run(config: dict, pipeline_name: str) -> Optional[Dict[str, Any]]:
    """
    Get the last successfully completed pipeline run.

    Args:
        config: Pipeline configuration
        pipeline_name: Name of pipeline

    Returns:
        dict: Last completed run record or None
    """
    # Import here to avoid circular dependency
    from framework_scripts import snowflake_operations as sf_ops
    from snowflake.connector import DictCursor

    query_sql = """
        SELECT *
        FROM pipeline_execution_drive
        WHERE pipeline_name = %(pipeline_name)s
          AND pipeline_status = 'SUCCESS'
        ORDER BY query_window_end_timestamp DESC
        LIMIT 1
    """

    try:
        with sf_ops.SnowflakeConnection(config) as conn:
            cursor = conn.cursor(DictCursor)
            cursor.execute(query_sql, {'pipeline_name': pipeline_name})
            result = cursor.fetchone()
            return result
    except Exception as e:
        logger.error(f"Failed to query last completed run: {str(e)}")
        return None


# Example usage
if __name__ == "__main__":
    import json

    # Test parse_time_duration
    test_durations = ["30s", "1h", "30m", "2d", "1w", "1month", "45millisec"]

    print("Testing parse_time_duration:")
    print("=" * 80)
    for duration_str in test_durations:
        try:
            td = parse_time_duration(duration_str)
            print(f"{duration_str:15s} -> {td} ({td.total_seconds()} seconds)")
        except ValueError as e:
            print(f"{duration_str:15s} -> ERROR: {e}")

    print("\n" + "=" * 80)

    # Test round_to_granularity
    test_time = datetime(2025, 11, 16, 10, 37, 42, tzinfo=ZoneInfo('UTC'))
    test_granularities = [
        timedelta(hours=1),
        timedelta(minutes=15),
        timedelta(minutes=30),
        timedelta(days=1)
    ]

    print("\nTesting round_to_granularity:")
    print("=" * 80)
    print(f"Original time: {test_time}")

    for granularity in test_granularities:
        rounded_down = round_to_granularity(test_time, granularity, 'down')
        rounded_up = round_to_granularity(test_time, granularity, 'up')
        print(f"Granularity {granularity}:")
        print(f"  Down:    {rounded_down}")
        print(f"  Up:      {rounded_up}")

    print("\n" + "=" * 80)
    print("Module loaded successfully")
