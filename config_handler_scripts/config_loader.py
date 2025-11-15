"""
Configuration Loader Module
===========================
Loads and provides access to pipeline configuration.

Functions:
    - load_config: Load configuration from JSON file
    - get_config_value: Get nested configuration value
    - validate_config_exists: Check if config file exists
"""

import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional


logger = logging.getLogger(__name__)


def load_config(config_path: str) -> Dict[str, Any]:
    """
    Load pipeline configuration from JSON file.

    Args:
        config_path: Path to config.json file

    Returns:
        dict: Parsed configuration

    Raises:
        FileNotFoundError: If config file doesn't exist
        json.JSONDecodeError: If config file is invalid JSON

    Example:
        >>> config = load_config('projects/genetic_tests/config.json')
        >>> pipeline_name = config['pipeline_metadata']['pipeline_name']
    """
    config_file = Path(config_path)

    if not config_file.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    logger.info(f"Loading configuration from: {config_path}")

    try:
        with open(config_file, 'r') as f:
            config = json.load(f)

        logger.info(f"Configuration loaded successfully")
        logger.info(f"Pipeline: {config.get('pipeline_metadata', {}).get('pipeline_name', 'Unknown')}")

        return config

    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in configuration file: {e}")
        raise


def get_config_value(
    config: Dict[str, Any],
    key_path: str,
    default: Any = None
) -> Any:
    """
    Get nested configuration value using dot notation.

    Args:
        config: Configuration dictionary
        key_path: Dot-separated path (e.g., 'source_system.type')
        default: Default value if key not found

    Returns:
        Configuration value or default

    Example:
        >>> source_type = get_config_value(config, 'source_system.type', 'unknown')
        >>> tolerance = get_config_value(config, 'phases.audit.count_tolerances.source_to_stage_tolerance_percent', 2.0)
    """
    keys = key_path.split('.')
    value = config

    for key in keys:
        if isinstance(value, dict) and key in value:
            value = value[key]
        else:
            return default

    return value


def validate_config_exists(config_path: str) -> bool:
    """
    Check if configuration file exists.

    Args:
        config_path: Path to config file

    Returns:
        bool: True if exists, False otherwise
    """
    return Path(config_path).exists()


def merge_configs(base_config: Dict[str, Any], override_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge two configurations (override takes precedence).

    Args:
        base_config: Base configuration
        override_config: Override configuration

    Returns:
        dict: Merged configuration

    Example:
        >>> # Useful for environment-specific overrides
        >>> base = load_config('config.json')
        >>> prod_overrides = load_config('config.prod.json')
        >>> final_config = merge_configs(base, prod_overrides)
    """
    result = base_config.copy()

    for key, value in override_config.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            # Recursively merge nested dicts
            result[key] = merge_configs(result[key], value)
        else:
            # Override value
            result[key] = value

    return result


def save_config(config: Dict[str, Any], config_path: str) -> None:
    """
    Save configuration to JSON file.

    Args:
        config: Configuration dictionary
        config_path: Path to save file

    Example:
        >>> save_config(config, 'projects/my_pipeline/config.json')
    """
    config_file = Path(config_path)

    # Create parent directories if they don't exist
    config_file.parent.mkdir(parents=True, exist_ok=True)

    with open(config_file, 'w') as f:
        json.dump(config, f, indent=4)

    logger.info(f"Configuration saved to: {config_path}")


# Example usage
if __name__ == "__main__":
    # Example configuration
    example_config = {
        'pipeline_metadata': {
            'pipeline_name': 'example_pipeline',
            'owner': 'Data Team'
        },
        'source_system': {
            'type': 'elasticsearch',
            'endpoint': 'https://es.example.com:9200'
        },
        'phases': {
            'audit': {
                'enabled': True,
                'count_tolerances': {
                    'source_to_stage_tolerance_percent': 2.0
                }
            }
        }
    }

    print("Configuration Loader Examples:")
    print("=" * 60)

    # Example 1: Get nested value
    pipeline_name = get_config_value(example_config, 'pipeline_metadata.pipeline_name')
    print(f"Pipeline name: {pipeline_name}")

    # Example 2: Get value with default
    unknown_value = get_config_value(example_config, 'does.not.exist', 'default_value')
    print(f"Unknown value: {unknown_value}")

    # Example 3: Get deeply nested value
    tolerance = get_config_value(
        example_config,
        'phases.audit.count_tolerances.source_to_stage_tolerance_percent',
        1.0
    )
    print(f"Tolerance: {tolerance}%")
