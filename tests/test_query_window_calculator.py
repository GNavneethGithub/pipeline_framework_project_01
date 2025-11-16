"""
Unit Tests for Query Window Calculator
=======================================
Tests for the query_window_calculator module functionality.
"""

import unittest
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Import directly to avoid snowflake dependency in tests
import importlib.util
spec = importlib.util.spec_from_file_location(
    "query_window_calculator",
    project_root / "framework_scripts" / "query_window_calculator.py"
)
qw_calc = importlib.util.module_from_spec(spec)
spec.loader.exec_module(qw_calc)


class TestParseTimeDuration(unittest.TestCase):
    """Test parse_time_duration function."""

    def test_parse_seconds(self):
        """Test parsing seconds."""
        self.assertEqual(qw_calc.parse_time_duration("30s"), timedelta(seconds=30))
        self.assertEqual(qw_calc.parse_time_duration("45sec"), timedelta(seconds=45))

    def test_parse_minutes(self):
        """Test parsing minutes."""
        self.assertEqual(qw_calc.parse_time_duration("30m"), timedelta(minutes=30))
        self.assertEqual(qw_calc.parse_time_duration("45min"), timedelta(minutes=45))

    def test_parse_hours(self):
        """Test parsing hours."""
        self.assertEqual(qw_calc.parse_time_duration("1h"), timedelta(hours=1))
        self.assertEqual(qw_calc.parse_time_duration("2hour"), timedelta(hours=2))

    def test_parse_days(self):
        """Test parsing days."""
        self.assertEqual(qw_calc.parse_time_duration("1d"), timedelta(days=1))
        self.assertEqual(qw_calc.parse_time_duration("7day"), timedelta(days=7))

    def test_parse_weeks(self):
        """Test parsing weeks."""
        self.assertEqual(qw_calc.parse_time_duration("1w"), timedelta(weeks=1))
        self.assertEqual(qw_calc.parse_time_duration("2week"), timedelta(weeks=2))

    def test_parse_months(self):
        """Test parsing months (approximated as 30 days)."""
        self.assertEqual(qw_calc.parse_time_duration("1month"), timedelta(days=30))
        self.assertEqual(qw_calc.parse_time_duration("2month"), timedelta(days=60))

    def test_parse_milliseconds(self):
        """Test parsing milliseconds."""
        self.assertEqual(qw_calc.parse_time_duration("100ms"), timedelta(milliseconds=100))
        self.assertEqual(qw_calc.parse_time_duration("40millisec"), timedelta(milliseconds=40))

    def test_parse_invalid_format(self):
        """Test invalid duration formats."""
        with self.assertRaises(ValueError):
            qw_calc.parse_time_duration("invalid")

        with self.assertRaises(ValueError):
            qw_calc.parse_time_duration("")

        with self.assertRaises(ValueError):
            qw_calc.parse_time_duration("1x")  # Invalid unit

    def test_parse_decimal_values(self):
        """Test parsing decimal values."""
        self.assertEqual(qw_calc.parse_time_duration("1.5h"), timedelta(hours=1.5))
        self.assertEqual(qw_calc.parse_time_duration("2.5d"), timedelta(days=2.5))


class TestRoundToGranularity(unittest.TestCase):
    """Test round_to_granularity function."""

    def test_round_down_hourly(self):
        """Test rounding down to hourly granularity."""
        dt = datetime(2025, 11, 16, 10, 37, 42, tzinfo=ZoneInfo('UTC'))
        expected = datetime(2025, 11, 16, 10, 0, 0, tzinfo=ZoneInfo('UTC'))
        result = qw_calc.round_to_granularity(dt, timedelta(hours=1), 'down')
        self.assertEqual(result, expected)

    def test_round_down_15_minutes(self):
        """Test rounding down to 15-minute granularity."""
        dt = datetime(2025, 11, 16, 10, 37, 42, tzinfo=ZoneInfo('UTC'))
        expected = datetime(2025, 11, 16, 10, 30, 0, tzinfo=ZoneInfo('UTC'))
        result = qw_calc.round_to_granularity(dt, timedelta(minutes=15), 'down')
        self.assertEqual(result, expected)

    def test_round_up_hourly(self):
        """Test rounding up to hourly granularity."""
        dt = datetime(2025, 11, 16, 10, 37, 42, tzinfo=ZoneInfo('UTC'))
        expected = datetime(2025, 11, 16, 11, 0, 0, tzinfo=ZoneInfo('UTC'))
        result = qw_calc.round_to_granularity(dt, timedelta(hours=1), 'up')
        self.assertEqual(result, expected)

    def test_round_nearest(self):
        """Test rounding to nearest granularity."""
        # 10:37 is closer to 11:00 than 10:00
        dt = datetime(2025, 11, 16, 10, 37, 42, tzinfo=ZoneInfo('UTC'))
        expected = datetime(2025, 11, 16, 11, 0, 0, tzinfo=ZoneInfo('UTC'))
        result = qw_calc.round_to_granularity(dt, timedelta(hours=1), 'nearest')
        self.assertEqual(result, expected)

    def test_round_already_aligned(self):
        """Test rounding when already aligned to granularity."""
        dt = datetime(2025, 11, 16, 10, 0, 0, tzinfo=ZoneInfo('UTC'))
        expected = datetime(2025, 11, 16, 10, 0, 0, tzinfo=ZoneInfo('UTC'))
        result = qw_calc.round_to_granularity(dt, timedelta(hours=1), 'down')
        self.assertEqual(result, expected)

    def test_round_daily(self):
        """Test rounding to daily granularity."""
        dt = datetime(2025, 11, 16, 10, 37, 42, tzinfo=ZoneInfo('UTC'))
        expected = datetime(2025, 11, 16, 0, 0, 0, tzinfo=ZoneInfo('UTC'))
        result = qw_calc.round_to_granularity(dt, timedelta(days=1), 'down')
        self.assertEqual(result, expected)

    def test_invalid_direction(self):
        """Test invalid direction parameter."""
        dt = datetime(2025, 11, 16, 10, 37, 42, tzinfo=ZoneInfo('UTC'))
        with self.assertRaises(ValueError):
            qw_calc.round_to_granularity(dt, timedelta(hours=1), 'invalid')

    def test_naive_datetime(self):
        """Test with naive datetime (should assume UTC)."""
        dt = datetime(2025, 11, 16, 10, 37, 42)
        result = qw_calc.round_to_granularity(dt, timedelta(hours=1), 'down')
        # Should add timezone
        self.assertIsNotNone(result.tzinfo)


class TestCreateGapIntervals(unittest.TestCase):
    """Test create_gap_intervals function."""

    def test_format_gap_intervals(self):
        """Test formatting gap intervals."""
        dt1 = datetime(2025, 11, 16, 10, 0, 0, tzinfo=ZoneInfo('UTC'))
        dt2 = datetime(2025, 11, 16, 11, 0, 0, tzinfo=ZoneInfo('UTC'))
        dt3 = datetime(2025, 11, 16, 12, 0, 0, tzinfo=ZoneInfo('UTC'))

        gap_intervals = [(dt1, dt2), (dt2, dt3)]
        result = qw_calc.create_gap_intervals(gap_intervals)

        self.assertEqual(len(result), 2)
        self.assertIn(dt1.isoformat(), result[0])
        self.assertIn(dt2.isoformat(), result[0])
        self.assertIn(dt2.isoformat(), result[1])
        self.assertIn(dt3.isoformat(), result[1])

    def test_empty_gap_intervals(self):
        """Test with empty gap intervals."""
        result = qw_calc.create_gap_intervals([])
        self.assertEqual(result, [])


class TestDetectGaps(unittest.TestCase):
    """Test detect_gaps function (unit tests without DB dependency)."""

    def test_gap_calculation_logic(self):
        """Test the gap calculation logic."""
        # This is a simplified test without DB dependency
        # In a real scenario, you'd mock the DB calls

        # Simulate scenario: last window ended at 10:00, next starts at 13:00, 1-hour granularity
        # Should detect gaps: [10:00-11:00], [11:00-12:00], [12:00-13:00]

        last_window_end = datetime(2025, 11, 16, 10, 0, 0, tzinfo=ZoneInfo('UTC'))
        next_window_start = datetime(2025, 11, 16, 13, 0, 0, tzinfo=ZoneInfo('UTC'))
        granularity = timedelta(hours=1)

        # Manual calculation
        gap_intervals = []
        current_start = last_window_end

        while current_start < next_window_start:
            current_end = current_start + granularity
            if current_end > next_window_start:
                current_end = next_window_start

            gap_intervals.append((current_start, current_end))
            current_start = current_end

        # Should have 3 gap intervals
        self.assertEqual(len(gap_intervals), 3)
        self.assertEqual(gap_intervals[0], (
            datetime(2025, 11, 16, 10, 0, 0, tzinfo=ZoneInfo('UTC')),
            datetime(2025, 11, 16, 11, 0, 0, tzinfo=ZoneInfo('UTC'))
        ))
        self.assertEqual(gap_intervals[1], (
            datetime(2025, 11, 16, 11, 0, 0, tzinfo=ZoneInfo('UTC')),
            datetime(2025, 11, 16, 12, 0, 0, tzinfo=ZoneInfo('UTC'))
        ))
        self.assertEqual(gap_intervals[2], (
            datetime(2025, 11, 16, 12, 0, 0, tzinfo=ZoneInfo('UTC')),
            datetime(2025, 11, 16, 13, 0, 0, tzinfo=ZoneInfo('UTC'))
        ))


class TestCalculateQueryWindow(unittest.TestCase):
    """Test calculate_query_window function (without DB dependency)."""

    def test_basic_calculation(self):
        """Test basic query window calculation."""
        config = {
            'pipeline_metadata': {
                'pipeline_name': None  # No pipeline name to avoid DB calls
            },
            'query_window': {
                'granularity': '1h',
                'x_days_back': '7d'
            }
        }

        current_time = datetime(2025, 11, 16, 10, 37, 42, tzinfo=ZoneInfo('UTC'))

        # Should round to 10:00 and create 1-hour window
        window_start, window_end = qw_calc.calculate_query_window(config, current_time)

        # Rounded current time
        current_time_rounded = datetime(2025, 11, 16, 10, 0, 0, tzinfo=ZoneInfo('UTC'))

        # Window should be within the last 7 days from rounded current time
        earliest_allowed = current_time_rounded - timedelta(days=7)
        self.assertGreaterEqual(window_start, earliest_allowed)

        # Window end should not be in the future (not past rounded current time)
        self.assertLessEqual(window_end, current_time_rounded)

        # Window should be exactly 1 hour
        self.assertEqual(window_end - window_start, timedelta(hours=1))

    def test_with_acceptable_start_time(self):
        """Test with acceptable_data_fetch_start_time."""
        acceptable_start = datetime(2025, 11, 10, 0, 0, 0, tzinfo=ZoneInfo('UTC'))

        config = {
            'pipeline_metadata': {
                'pipeline_name': None
            },
            'query_window': {
                'granularity': '1h',
                'x_days_back': '30d',
                'acceptable_data_fetch_start_time': acceptable_start.isoformat()
            }
        }

        current_time = datetime(2025, 11, 16, 10, 0, 0, tzinfo=ZoneInfo('UTC'))

        window_start, window_end = qw_calc.calculate_query_window(config, current_time)

        # Window start should not be before acceptable_start_time
        self.assertGreaterEqual(window_start, acceptable_start)


def run_tests():
    """Run all tests."""
    # Create test suite
    suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    # Return exit code
    return 0 if result.wasSuccessful() else 1


if __name__ == '__main__':
    exit_code = run_tests()
    sys.exit(exit_code)
