"""
Config Handler Module
======================
Handles configuration flattening and placeholder replacement.

This module provides utilities to:
1. Flatten nested JSON configurations
2. Replace placeholders in values with actual config values
3. Merge extra dictionaries and perform additional replacements

Functions:
    - flatten_dict: Flatten nested dictionary with underscore-separated keys
    - replace_placeholders: Replace {key} placeholders in values
    - apply_extra_dicts: Merge extra dictionaries and replace placeholders
    - load_and_process_config: Main orchestration function
"""

import json
import re
import logging
from pathlib import Path
from typing import Dict, Any, List, Union


logger = logging.getLogger(__name__)


def flatten_dict(
    nested_dict: Dict[str, Any],
    parent_key: str = '',
    separator: str = '_'
) -> Dict[str, Any]:
    """
    Flatten a nested dictionary by joining keys with separator.

    Args:
        nested_dict: The nested dictionary to flatten
        parent_key: The parent key prefix (used in recursion)
        separator: Separator to use between keys (default: '_')

    Returns:
        dict: Flattened dictionary with keys like 'parent_child_grandchild'

    Example:
        >>> nested = {
        ...     'elasticsearch': {
        ...         'index_old_name': 'lsf_x',
        ...         'index_new_name': 'completed_jobs'
        ...     },
        ...     'aws': {
        ...         'aws_s3_bucket': 'bucket_x'
        ...     }
        ... }
        >>> flatten_dict(nested)
        {
            'elasticsearch_index_old_name': 'lsf_x',
            'elasticsearch_index_new_name': 'completed_jobs',
            'aws_aws_s3_bucket': 'bucket_x'
        }
    """
    flattened = {}

    for key, value in nested_dict.items():
        # Create new key with parent prefix
        new_key = f"{parent_key}{separator}{key}" if parent_key else key

        if isinstance(value, dict):
            # Recursively flatten nested dictionaries
            flattened.update(flatten_dict(value, new_key, separator))
        elif isinstance(value, list):
            # Handle lists - store as is (can be processed later)
            flattened[new_key] = value
        else:
            # Store primitive values directly
            flattened[new_key] = value

    return flattened


def replace_placeholders(
    flattened_dict: Dict[str, Any],
    max_iterations: int = 10
) -> Dict[str, Any]:
    """
    Replace {placeholder} patterns in values with actual values from the dictionary.

    This function performs multiple passes to handle nested placeholders
    (e.g., when a value contains a placeholder that itself references another placeholder).

    Args:
        flattened_dict: Flattened dictionary with potential placeholders
        max_iterations: Maximum number of replacement passes (prevents infinite loops)

    Returns:
        dict: Dictionary with all placeholders replaced

    Example:
        >>> config = {
        ...     'elasticsearch_index_old_name': 'lsf_x',
        ...     'elasticsearch_index_new_name': 'completed_jobs',
        ...     'snowflake_raw_database': 'db_{index_new_name}',
        ...     'snowflake_raw_table': '{index_old_name}_tbl'
        ... }
        >>> replace_placeholders(config)
        {
            'elasticsearch_index_old_name': 'lsf_x',
            'elasticsearch_index_new_name': 'completed_jobs',
            'snowflake_raw_database': 'db_completed_jobs',
            'snowflake_raw_table': 'lsf_x_tbl'
        }
    """
    result = flattened_dict.copy()
    placeholder_pattern = re.compile(r'\{([^}]+)\}')

    # Perform multiple passes to resolve nested placeholders
    for iteration in range(max_iterations):
        replacements_made = False

        for key, value in result.items():
            if isinstance(value, str):
                # Find all placeholders in this value
                matches = placeholder_pattern.findall(value)

                for placeholder in matches:
                    # Look for the placeholder value in the flattened dict
                    # Try exact match first
                    replacement_value = None

                    if placeholder in result:
                        replacement_value = result[placeholder]
                    else:
                        # Try searching for the placeholder in any key ending with it
                        for dict_key, dict_value in result.items():
                            if dict_key.endswith(f"_{placeholder}") or dict_key == placeholder:
                                replacement_value = dict_value
                                break

                    # Perform replacement if value found
                    if replacement_value is not None:
                        # Convert to string if not already
                        replacement_str = str(replacement_value)
                        result[key] = result[key].replace(f'{{{placeholder}}}', replacement_str)
                        replacements_made = True
                        logger.debug(f"Replaced {{{placeholder}}} in {key} with {replacement_str}")

            elif isinstance(value, list):
                # Handle lists - replace placeholders in list items
                new_list = []
                for item in value:
                    if isinstance(item, str):
                        matches = placeholder_pattern.findall(item)
                        item_copy = item

                        for placeholder in matches:
                            replacement_value = None

                            if placeholder in result:
                                replacement_value = result[placeholder]
                            else:
                                for dict_key, dict_value in result.items():
                                    if dict_key.endswith(f"_{placeholder}") or dict_key == placeholder:
                                        replacement_value = dict_value
                                        break

                            if replacement_value is not None:
                                replacement_str = str(replacement_value)
                                item_copy = item_copy.replace(f'{{{placeholder}}}', replacement_str)
                                replacements_made = True

                        new_list.append(item_copy)
                    else:
                        new_list.append(item)

                result[key] = new_list

        # If no replacements were made, we're done
        if not replacements_made:
            logger.info(f"Placeholder replacement completed in {iteration + 1} iteration(s)")
            break
    else:
        logger.warning(f"Reached maximum iterations ({max_iterations}) for placeholder replacement")

    return result


def apply_extra_dicts(
    processed_dict: Dict[str, Any],
    extra_dicts: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Merge extra dictionaries and perform placeholder replacement again.

    This allows users to add custom key-value pairs and have placeholders
    resolved using both the original config and the extra values.

    Args:
        processed_dict: Already processed configuration dictionary
        extra_dicts: Additional key-value pairs to merge

    Returns:
        dict: Final dictionary with extra values merged and placeholders replaced

    Example:
        >>> config = {
        ...     'elasticsearch_index_new_name': 'completed_jobs',
        ...     'snowflake_raw_database': 'db_completed_jobs'
        ... }
        >>> extra = {
        ...     'env': 'production',
        ...     'custom_prefix': 'pipeline_{env}_{index_new_name}'
        ... }
        >>> apply_extra_dicts(config, extra)
        {
            'elasticsearch_index_new_name': 'completed_jobs',
            'snowflake_raw_database': 'db_completed_jobs',
            'env': 'production',
            'custom_prefix': 'pipeline_production_completed_jobs'
        }
    """
    # Merge extra_dicts into processed_dict
    result = processed_dict.copy()
    result.update(extra_dicts)

    logger.info(f"Merged {len(extra_dicts)} extra key-value pair(s)")

    # Perform another round of placeholder replacement
    result = replace_placeholders(result)

    return result


def load_and_process_config(
    absolute_root_to_project_folder_path: str,
    relative_project_to_config_file_path: str,
    extra_dicts: Dict[str, Any] = None
) -> Dict[str, Any]:
    """
    Main orchestration function to load, flatten, and process configuration.

    This function:
    1. Loads JSON config from the specified path
    2. Flattens the nested structure
    3. Replaces placeholders in values
    4. Merges extra_dicts (if provided) and replaces placeholders again

    Args:
        absolute_root_to_project_folder_path: Absolute path to project root
        relative_project_to_config_file_path: Relative path from root to config file
        extra_dicts: Optional additional key-value pairs to merge

    Returns:
        dict: Final processed and flattened configuration

    Raises:
        FileNotFoundError: If config file doesn't exist
        json.JSONDecodeError: If config file is invalid JSON

    Example:
        >>> config = load_and_process_config(
        ...     absolute_root_to_project_folder_path='/home/user/pipeline_framework_project_01',
        ...     relative_project_to_config_file_path='projects/example/config.json',
        ...     extra_dicts={'env': 'production', 'region': 'us-east-1'}
        ... )
        >>> print(config['elasticsearch_index_new_name'])
        'completed_jobs'
    """
    # Construct full path to config file
    project_root = Path(absolute_root_to_project_folder_path)
    config_path = project_root / relative_project_to_config_file_path

    logger.info(f"Loading config from: {config_path}")

    # Check if file exists
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    # Load JSON config
    try:
        with open(config_path, 'r') as f:
            nested_config = json.load(f)
        logger.info("Configuration loaded successfully")
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in configuration file: {e}")
        raise

    # Step 1: Flatten the configuration
    logger.info("Flattening configuration...")
    flattened_config = flatten_dict(nested_config)
    logger.info(f"Flattened to {len(flattened_config)} key-value pairs")

    # Step 2: Replace placeholders
    logger.info("Replacing placeholders...")
    processed_config = replace_placeholders(flattened_config)

    # Step 3: Apply extra_dicts if provided
    if extra_dicts:
        logger.info("Applying extra dictionaries...")
        processed_config = apply_extra_dicts(processed_config, extra_dicts)

    logger.info("Configuration processing complete")
    return processed_config


# Example usage and testing
if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(levelname)s - %(message)s'
    )

    print("\n" + "=" * 80)
    print("CONFIG HANDLER - Example Usage")
    print("=" * 80)

    # Example 1: Flatten a nested dictionary
    print("\n1. FLATTENING EXAMPLE")
    print("-" * 80)

    nested_example = {
        "elasticsearch": {
            "index_old_name": "lsf_x",
            "index_new_name": "completed_jobs"
        },
        "aws": {
            "aws_s3_bucket": "bucket_x",
            "prefix_list": ["pipeline_x_{env}", "{index_new_name}", "{index_old_name}"]
        },
        "snowflake": {
            "raw_database": "db_{index_new_name}",
            "raw_schema": "sch_{index_new_name}",
            "raw_table": "{index_old_name}_tbl",
            "drive_database": "db_drive",
            "drive_schema": "sch_drive",
            "drive_tbl": "pipeline_drive_tbl"
        }
    }

    print("Original nested config:")
    print(json.dumps(nested_example, indent=2))

    flattened = flatten_dict(nested_example)
    print("\nFlattened config:")
    for key, value in sorted(flattened.items()):
        print(f"  {key}: {value}")

    # Example 2: Replace placeholders
    print("\n2. PLACEHOLDER REPLACEMENT EXAMPLE")
    print("-" * 80)

    processed = replace_placeholders(flattened)
    print("After placeholder replacement:")
    for key, value in sorted(processed.items()):
        print(f"  {key}: {value}")

    # Example 3: Apply extra dicts
    print("\n3. EXTRA DICTS EXAMPLE")
    print("-" * 80)

    extra = {
        "env": "production",
        "region": "us-east-1",
        "custom_bucket": "custom_{env}_{region}",
        "full_prefix": "{env}_{index_new_name}_data"
    }

    print("Extra dictionaries to merge:")
    for key, value in extra.items():
        print(f"  {key}: {value}")

    final = apply_extra_dicts(processed, extra)
    print("\nFinal config after merging extra dicts:")
    for key, value in sorted(final.items()):
        print(f"  {key}: {value}")

    # Example 4: Full workflow with a sample config file
    print("\n4. FULL WORKFLOW EXAMPLE")
    print("-" * 80)
    print("To use the full workflow:")
    print("""
    from config_handler_scripts.config_handler import load_and_process_config

    config = load_and_process_config(
        absolute_root_to_project_folder_path='/home/user/pipeline_framework_project_01',
        relative_project_to_config_file_path='projects/example_pipeline/config.json',
        extra_dicts={
            'env': 'production',
            'region': 'us-east-1'
        }
    )
    """)

    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print("""
The config handler provides three main functions:

1. flatten_dict(nested_dict)
   - Flattens nested JSON into single-level dict
   - Keys are joined with underscores (e.g., 'parent_child_grandchild')

2. replace_placeholders(flattened_dict)
   - Replaces {placeholder} patterns with actual values
   - Handles nested placeholders through multiple passes
   - Works with both string values and lists

3. apply_extra_dicts(processed_dict, extra_dicts)
   - Merges custom key-value pairs into config
   - Performs another round of placeholder replacement
   - Allows for custom modifications

Main function: load_and_process_config(root_path, config_path, extra_dicts)
   - Orchestrates the entire process
   - Returns fully processed configuration dictionary
    """)
