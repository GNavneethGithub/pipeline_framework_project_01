"""
Configuration Validator Module
==============================
Validates pipeline configuration for correctness and completeness.

Functions:
    - validate_config: Validate complete configuration
    - validate_required_fields: Check required fields present
    - validate_tolerances: Check tolerance values are valid
    - validate_phases: Check phase configuration
"""

import logging
from typing import Dict, Any, List, Tuple


logger = logging.getLogger(__name__)


class ConfigValidationError(Exception):
    """Raised when configuration validation fails."""
    pass


def validate_config(config: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Validate complete pipeline configuration.

    Args:
        config: Pipeline configuration dictionary

    Returns:
        tuple: (is_valid: bool, errors: list of error messages)

    Example:
        >>> is_valid, errors = validate_config(config)
        >>> if not is_valid:
        >>>     for error in errors:
        >>>         print(f"ERROR: {error}")
    """
    logger.info("Validating pipeline configuration...")

    errors = []

    # Validate required top-level sections
    required_sections = [
        'pipeline_metadata',
        'source_system',
        'stage_system',
        'target_system',
        'phases',
        'snowflake_connection'
    ]

    for section in required_sections:
        if section not in config:
            errors.append(f"Missing required section: {section}")

    if errors:
        return False, errors

    # Validate each section
    errors.extend(validate_pipeline_metadata(config.get('pipeline_metadata', {})))
    errors.extend(validate_source_system(config.get('source_system', {})))
    errors.extend(validate_stage_system(config.get('stage_system', {})))
    errors.extend(validate_target_system(config.get('target_system', {})))
    errors.extend(validate_phases(config.get('phases', {})))
    errors.extend(validate_tolerances(config.get('phases', {}).get('audit', {}).get('count_tolerances', {})))

    is_valid = len(errors) == 0

    if is_valid:
        logger.info("✓ Configuration validation passed")
    else:
        logger.error(f"✗ Configuration validation failed with {len(errors)} error(s)")
        for error in errors:
            logger.error(f"  - {error}")

    return is_valid, errors


def validate_pipeline_metadata(metadata: Dict[str, Any]) -> List[str]:
    """Validate pipeline metadata section."""
    errors = []

    required_fields = ['pipeline_name', 'owner_email']
    for field in required_fields:
        if field not in metadata or not metadata[field]:
            errors.append(f"pipeline_metadata.{field} is required")

    # Validate email format (basic check)
    if 'owner_email' in metadata:
        email = metadata['owner_email']
        if '@' not in email:
            errors.append(f"pipeline_metadata.owner_email is invalid: {email}")

    return errors


def validate_source_system(source: Dict[str, Any]) -> List[str]:
    """Validate source system configuration."""
    errors = []

    required_fields = ['type']
    for field in required_fields:
        if field not in source or not source[field]:
            errors.append(f"source_system.{field} is required")

    # Validate source type
    valid_source_types = ['elasticsearch', 'mysql', 'postgresql', 'api', 'servicenow']
    if 'type' in source and source['type'] not in valid_source_types:
        errors.append(
            f"source_system.type '{source['type']}' is not valid. "
            f"Valid types: {', '.join(valid_source_types)}"
        )

    return errors


def validate_stage_system(stage: Dict[str, Any]) -> List[str]:
    """Validate stage system configuration."""
    errors = []

    required_fields = ['database', 'schema', 'table']
    for field in required_fields:
        if field not in stage or not stage[field]:
            errors.append(f"stage_system.{field} is required")

    return errors


def validate_target_system(target: Dict[str, Any]) -> List[str]:
    """Validate target system configuration."""
    errors = []

    required_fields = ['database', 'schema', 'table']
    for field in required_fields:
        if field not in target or not target[field]:
            errors.append(f"target_system.{field} is required")

    return errors


def validate_phases(phases: Dict[str, Any]) -> List[str]:
    """Validate phases configuration."""
    errors = []

    # Required phases
    required_phases = [
        'stale_pipeline_handling',
        'pre_validation',
        'source_to_stage_transfer',
        'stage_to_target_transfer',
        'audit'
    ]

    for phase in required_phases:
        if phase not in phases:
            errors.append(f"phases.{phase} configuration is missing")
        else:
            phase_config = phases[phase]
            if 'enabled' not in phase_config:
                errors.append(f"phases.{phase}.enabled is required")

    return errors


def validate_tolerances(tolerances: Dict[str, Any]) -> List[str]:
    """Validate tolerance percentages."""
    errors = []

    tolerance_fields = [
        'source_to_stage_tolerance_percent',
        'stage_to_target_tolerance_percent'
    ]

    for field in tolerance_fields:
        if field not in tolerances:
            errors.append(f"count_tolerances.{field} is required")
        else:
            value = tolerances[field]

            # Check if numeric
            if not isinstance(value, (int, float)):
                errors.append(f"count_tolerances.{field} must be numeric, got {type(value).__name__}")
            else:
                # Check range (0-100)
                if value < 0 or value > 100:
                    errors.append(f"count_tolerances.{field} must be between 0 and 100, got {value}")

    return errors


def validate_required_fields(
    config: Dict[str, Any],
    required_fields: List[str]
) -> List[str]:
    """
    Check if required fields are present.

    Args:
        config: Configuration dictionary
        required_fields: List of required field paths (dot notation)

    Returns:
        list: Error messages for missing fields

    Example:
        >>> errors = validate_required_fields(
        >>>     config,
        >>>     ['pipeline_metadata.pipeline_name', 'source_system.type']
        >>> )
    """
    from config_handler_scripts.config_loader import get_config_value

    errors = []

    for field_path in required_fields:
        value = get_config_value(config, field_path)
        if value is None or value == '':
            errors.append(f"Required field missing or empty: {field_path}")

    return errors


def create_config_template() -> Dict[str, Any]:
    """
    Create a configuration template with all required fields.

    Returns:
        dict: Configuration template

    Example:
        >>> template = create_config_template()
        >>> # Customize template
        >>> template['pipeline_metadata']['pipeline_name'] = 'my_pipeline'
        >>> # Save to file
        >>> from config_handler_scripts.config_loader import save_config
        >>> save_config(template, 'projects/my_pipeline/config.json')
    """
    return {
        'pipeline_metadata': {
            'pipeline_name': 'REQUIRED: your_pipeline_name',
            'pipeline_description': 'Description of what this pipeline does',
            'owner_name': 'Your Name',
            'owner_email': 'your.email@company.com',
            'notification_emails': ['team@company.com'],
            'slack_channel': '#data-alerts'
        },
        'dag_schedule': {
            'frequency': 'hourly',
            'cron_expression': '0 * * * *',
            'timezone': 'UTC',
            'start_date': '2025-11-15',
            'catchup': False
        },
        'query_window': {
            'type': 'hourly',
            'duration': '1h'
        },
        'source_system': {
            'type': 'REQUIRED: elasticsearch|mysql|postgresql|api|servicenow',
            'endpoint': 'REQUIRED: connection endpoint',
            'batch_size': 5000
        },
        'stage_system': {
            'type': 'snowflake',
            'database': 'REQUIRED: CADS_DB',
            'schema': 'REQUIRED: stg_your_pipeline',
            'table': 'REQUIRED: stage_table_name'
        },
        'target_system': {
            'type': 'snowflake',
            'database': 'REQUIRED: CDW_DB',
            'schema': 'REQUIRED: prod_your_pipeline',
            'table': 'REQUIRED: target_table_name'
        },
        'phases': {
            'stale_pipeline_handling': {
                'enabled': True,
                'expected_run_duration': '1m',
                'timeout_minutes': 120
            },
            'pre_validation': {
                'enabled': True,
                'expected_run_duration': '1m'
            },
            'source_to_stage_transfer': {
                'enabled': True,
                'expected_run_duration': '10m'
            },
            'stage_to_target_transfer': {
                'enabled': True,
                'expected_run_duration': '5m'
            },
            'audit': {
                'enabled': True,
                'expected_run_duration': '2m',
                'count_tolerances': {
                    'source_to_stage_tolerance_percent': 2.0,
                    'stage_to_target_tolerance_percent': 1.0
                }
            },
            'stage_cleaning': {
                'enabled': True,
                'expected_run_duration': '3m',
                'archive_before_delete': True,
                'archive_location': 's3://your-bucket/archive/',
                'delete_after_days': 7
            },
            'target_cleaning': {
                'enabled': False,
                'expected_run_duration': '5m'
            }
        },
        'retry_configuration': {
            'max_retries': 3,
            'backoff_strategy': 'exponential'
        },
        'monitoring': {
            'alert_on_failure': True,
            'alert_on_duration_exceed': True,
            'duration_multiplier_threshold': 2.0
        },
        'snowflake_connection': {
            'user': 'REQUIRED: your_snowflake_user',
            'password': 'REQUIRED: your_snowflake_password',
            'account': 'REQUIRED: your_account',
            'warehouse': 'REQUIRED: your_warehouse',
            'database': 'REQUIRED: your_database',
            'schema': 'REQUIRED: your_schema',
            'role': 'REQUIRED: your_role'
        }
    }


# Example usage
if __name__ == "__main__":
    # Create and validate a template
    template = create_config_template()

    print("Configuration Template Created")
    print("=" * 60)

    # This will fail validation because required fields have placeholder values
    is_valid, errors = validate_config(template)

    print(f"\nValidation result: {'PASSED' if is_valid else 'FAILED'}")
    print(f"Errors found: {len(errors)}")

    if errors:
        print("\nErrors:")
        for error in errors:
            print(f"  - {error}")

    print("\nTo use this template:")
    print("1. Replace all 'REQUIRED:' values with actual values")
    print("2. Customize other settings as needed")
    print("3. Save to projects/your_pipeline/config.json")
    print("4. Run validation again")
