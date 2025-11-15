"""
Duration Utilities Module
=========================
Provides utilities for calculating and formatting pipeline execution durations.

Functions:
    - calculate_duration: Get duration between two timestamps
    - format_duration: Convert duration to human-readable string
    - parse_duration: Convert string back to seconds
"""

from datetime import datetime, timedelta
from typing import Union, Optional
import re


def calculate_duration(
    start_timestamp: Union[datetime, str],
    end_timestamp: Union[datetime, str]
) -> timedelta:
    """
    Calculate duration between two timestamps.

    Args:
        start_timestamp: Start time (datetime or ISO string)
        end_timestamp: End time (datetime or ISO string)

    Returns:
        timedelta: Duration between timestamps

    Raises:
        ValueError: If timestamps are invalid or end < start

    Example:
        >>> start = datetime(2025, 11, 15, 10, 0, 0)
        >>> end = datetime(2025, 11, 15, 10, 30, 45)
        >>> duration = calculate_duration(start, end)
        >>> print(duration)
        0:30:45
    """
    # Convert strings to datetime if needed
    if isinstance(start_timestamp, str):
        start_timestamp = datetime.fromisoformat(start_timestamp.replace('Z', '+00:00'))
    if isinstance(end_timestamp, str):
        end_timestamp = datetime.fromisoformat(end_timestamp.replace('Z', '+00:00'))

    # Validate
    if end_timestamp < start_timestamp:
        raise ValueError(f"End timestamp {end_timestamp} is before start timestamp {start_timestamp}")

    return end_timestamp - start_timestamp


def format_duration(duration: Union[timedelta, float, int]) -> str:
    """
    Convert duration to human-readable string.

    Args:
        duration: timedelta object, or seconds (float/int)

    Returns:
        str: Human-readable duration (e.g., "1h 30m 45s", "45m", "2h")

    Example:
        >>> format_duration(timedelta(hours=1, minutes=30, seconds=45))
        '1h 30m 45s'
        >>> format_duration(3665)  # 1 hour, 1 minute, 5 seconds
        '1h 1m 5s'
        >>> format_duration(45)  # 45 seconds
        '45s'
    """
    # Convert to seconds if timedelta
    if isinstance(duration, timedelta):
        total_seconds = int(duration.total_seconds())
    else:
        total_seconds = int(duration)

    if total_seconds < 0:
        raise ValueError("Duration cannot be negative")

    # Calculate components
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60

    # Build string
    parts = []
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")
    if seconds > 0 or not parts:  # Always show seconds if duration is 0
        parts.append(f"{seconds}s")

    return " ".join(parts)


def parse_duration(duration_str: str) -> int:
    """
    Parse human-readable duration string to seconds.

    Args:
        duration_str: Duration string (e.g., "1h 30m 45s", "45m", "2h")

    Returns:
        int: Total seconds

    Raises:
        ValueError: If duration string is invalid

    Example:
        >>> parse_duration("1h 30m 45s")
        5445
        >>> parse_duration("45m")
        2700
        >>> parse_duration("2h")
        7200
    """
    if not duration_str:
        raise ValueError("Duration string cannot be empty")

    # Pattern to match duration components
    pattern = r'(?:(\d+)h)?\s*(?:(\d+)m)?\s*(?:(\d+)s)?'
    match = re.fullmatch(pattern, duration_str.strip())

    if not match:
        raise ValueError(f"Invalid duration format: {duration_str}")

    hours = int(match.group(1) or 0)
    minutes = int(match.group(2) or 0)
    seconds = int(match.group(3) or 0)

    if hours == 0 and minutes == 0 and seconds == 0:
        raise ValueError(f"Duration string resulted in zero duration: {duration_str}")

    return hours * 3600 + minutes * 60 + seconds


def get_duration_seconds(duration: Union[timedelta, str, float, int]) -> float:
    """
    Get duration in seconds from various input types.

    Args:
        duration: Can be timedelta, duration string, or numeric seconds

    Returns:
        float: Duration in seconds

    Example:
        >>> get_duration_seconds(timedelta(hours=1))
        3600.0
        >>> get_duration_seconds("1h 30m")
        5400.0
        >>> get_duration_seconds(3600)
        3600.0
    """
    if isinstance(duration, timedelta):
        return duration.total_seconds()
    elif isinstance(duration, str):
        return float(parse_duration(duration))
    else:
        return float(duration)


def is_duration_exceeded(
    actual_duration: Union[timedelta, str, float, int],
    expected_duration: Union[timedelta, str, float, int],
    multiplier_threshold: float = 2.0
) -> bool:
    """
    Check if actual duration exceeds expected duration by threshold multiplier.

    Args:
        actual_duration: Actual execution duration
        expected_duration: Expected/baseline duration
        multiplier_threshold: Alert if actual > (expected * threshold)

    Returns:
        bool: True if duration exceeded, False otherwise

    Example:
        >>> is_duration_exceeded("10m", "5m", 2.0)
        False  # 10 minutes <= 5 * 2 = 10 minutes
        >>> is_duration_exceeded("11m", "5m", 2.0)
        True   # 11 minutes > 5 * 2 = 10 minutes
    """
    actual_seconds = get_duration_seconds(actual_duration)
    expected_seconds = get_duration_seconds(expected_duration)

    threshold = expected_seconds * multiplier_threshold

    return actual_seconds > threshold


# Example usage and tests
if __name__ == "__main__":
    # Test calculate_duration
    start = datetime(2025, 11, 15, 10, 0, 0)
    end = datetime(2025, 11, 15, 11, 30, 45)
    duration = calculate_duration(start, end)
    print(f"Duration: {duration}")

    # Test format_duration
    formatted = format_duration(duration)
    print(f"Formatted: {formatted}")

    # Test parse_duration
    parsed = parse_duration("1h 30m 45s")
    print(f"Parsed: {parsed} seconds")

    # Test is_duration_exceeded
    exceeded = is_duration_exceeded("11m", "5m", 2.0)
    print(f"Duration exceeded: {exceeded}")

    # Test various formats
    test_cases = [
        (3665, "1h 1m 5s"),
        (60, "1m"),
        (3600, "1h"),
        (45, "45s"),
        (5445, "1h 30m 45s"),
        (0, "0s"),
    ]

    print("\nTest cases:")
    for seconds, expected in test_cases:
        result = format_duration(seconds)
        status = "✓" if result == expected else "✗"
        print(f"{status} {seconds}s -> '{result}' (expected: '{expected}')")
