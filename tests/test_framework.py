"""
Framework Unit Tests
====================
Basic unit tests for framework components.
"""

import pytest
from datetime import datetime, timedelta
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from framework_scripts.duration_utils import (
    calculate_duration,
    format_duration,
    parse_duration,
    is_duration_exceeded
)


class TestDurationUtils:
    """Test duration utility functions."""

    def test_format_duration_hours_minutes_seconds(self):
        """Test formatting duration with hours, minutes, and seconds."""
        result = format_duration(3665)  # 1h 1m 5s
        assert result == "1h 1m 5s"

    def test_format_duration_minutes_only(self):
        """Test formatting duration with only minutes."""
        result = format_duration(120)  # 2m
        assert result == "2m"

    def test_format_duration_seconds_only(self):
        """Test formatting duration with only seconds."""
        result = format_duration(45)
        assert result == "45s"

    def test_format_duration_zero(self):
        """Test formatting zero duration."""
        result = format_duration(0)
        assert result == "0s"

    def test_parse_duration_full(self):
        """Test parsing full duration string."""
        result = parse_duration("1h 30m 45s")
        assert result == 5445

    def test_parse_duration_hours_only(self):
        """Test parsing hours only."""
        result = parse_duration("2h")
        assert result == 7200

    def test_parse_duration_minutes_only(self):
        """Test parsing minutes only."""
        result = parse_duration("45m")
        assert result == 2700

    def test_calculate_duration(self):
        """Test calculating duration between timestamps."""
        start = datetime(2025, 11, 15, 10, 0, 0)
        end = datetime(2025, 11, 15, 10, 30, 45)
        duration = calculate_duration(start, end)

        assert duration.total_seconds() == 1845  # 30m 45s

    def test_is_duration_exceeded_true(self):
        """Test duration exceeded check - should return True."""
        result = is_duration_exceeded("11m", "5m", 2.0)
        assert result is True

    def test_is_duration_exceeded_false(self):
        """Test duration exceeded check - should return False."""
        result = is_duration_exceeded("10m", "5m", 2.0)
        assert result is False


class TestConfigValidation:
    """Test configuration validation."""

    def test_config_template_creation(self):
        """Test creating configuration template."""
        from config_handler_scripts.config_validator import create_config_template

        template = create_config_template()

        assert 'pipeline_metadata' in template
        assert 'source_system' in template
        assert 'stage_system' in template
        assert 'target_system' in template
        assert 'phases' in template

    def test_config_loader(self):
        """Test loading configuration."""
        from config_handler_scripts.config_loader import load_config

        # Load example config
        config_path = project_root / 'projects' / 'example_pipeline' / 'config.json'

        if config_path.exists():
            config = load_config(str(config_path))

            assert config['pipeline_metadata']['pipeline_name'] == 'example_pipeline'
            assert 'source_system' in config
            assert 'phases' in config


# Run tests
if __name__ == '__main__':
    pytest.main([__file__, '-v'])
