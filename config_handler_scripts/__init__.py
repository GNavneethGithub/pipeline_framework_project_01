"""
Configuration Handler Scripts Package
=====================================
Configuration management for pipeline framework.

Modules:
    - config_loader: Load and access configuration
    - config_validator: Validate configuration correctness
"""

from config_handler_scripts.config_loader import (
    load_config,
    get_config_value,
    validate_config_exists,
    merge_configs,
    save_config
)

from config_handler_scripts.config_validator import (
    ConfigValidationError,
    validate_config,
    validate_pipeline_metadata,
    validate_source_system,
    validate_stage_system,
    validate_target_system,
    validate_phases,
    validate_tolerances,
    validate_required_fields,
    create_config_template
)

__all__ = [
    # config_loader
    'load_config',
    'get_config_value',
    'validate_config_exists',
    'merge_configs',
    'save_config',

    # config_validator
    'ConfigValidationError',
    'validate_config',
    'validate_pipeline_metadata',
    'validate_source_system',
    'validate_stage_system',
    'validate_target_system',
    'validate_phases',
    'validate_tolerances',
    'validate_required_fields',
    'create_config_template',
]

__version__ = '1.0.0'
