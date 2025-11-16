"""
Test script for config_handler module
Demonstrates the full workflow with a real config file
"""

import json
import logging
from config_handler_scripts.config_handler import (
    load_and_process_config,
    flatten_dict,
    replace_placeholders,
    apply_extra_dicts
)


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def test_full_workflow():
    """Test the complete config handling workflow"""
    print("\n" + "=" * 100)
    print("CONFIG HANDLER - Full Workflow Test")
    print("=" * 100)

    # Test with the sample config file
    config = load_and_process_config(
        absolute_root_to_project_folder_path='/home/user/pipeline_framework_project_01',
        relative_project_to_config_file_path='sample_config.json',
        extra_dicts={
            'env': 'production',
            'region': 'us-east-1',
            'team': 'data-engineering',
            'custom_path': 's3://{aws_s3_bucket}/{env}/{index_new_name}'
        }
    )

    print("\n" + "-" * 100)
    print("FINAL PROCESSED CONFIGURATION")
    print("-" * 100)

    for key, value in sorted(config.items()):
        print(f"  {key:45} = {value}")

    return config


def test_individual_functions():
    """Test individual functions step by step"""
    print("\n" + "=" * 100)
    print("CONFIG HANDLER - Step-by-Step Test")
    print("=" * 100)

    # Load raw config
    with open('/home/user/pipeline_framework_project_01/sample_config.json', 'r') as f:
        raw_config = json.load(f)

    print("\nSTEP 1: Original Nested Config")
    print("-" * 100)
    print(json.dumps(raw_config, indent=2))

    # Flatten
    print("\nSTEP 2: Flattened Config")
    print("-" * 100)
    flattened = flatten_dict(raw_config)
    for key, value in sorted(flattened.items()):
        print(f"  {key:45} = {value}")

    # Replace placeholders
    print("\nSTEP 3: After Placeholder Replacement")
    print("-" * 100)
    processed = replace_placeholders(flattened)
    for key, value in sorted(processed.items()):
        print(f"  {key:45} = {value}")

    # Apply extra dicts
    print("\nSTEP 4: After Applying Extra Dicts")
    print("-" * 100)
    extra = {
        'env': 'staging',
        'app_name': 'pipeline_{index_new_name}',
        'full_table_name': '{raw_database}.{raw_schema}.{raw_table}'
    }
    print("Extra dicts being added:")
    for key, value in extra.items():
        print(f"  {key:45} = {value}")

    final = apply_extra_dicts(processed, extra)
    print("\nFinal config:")
    for key, value in sorted(final.items()):
        print(f"  {key:45} = {value}")


def test_edge_cases():
    """Test edge cases and complex scenarios"""
    print("\n" + "=" * 100)
    print("CONFIG HANDLER - Edge Cases Test")
    print("=" * 100)

    # Test nested placeholders
    print("\nTest 1: Nested Placeholders")
    print("-" * 100)

    nested_placeholders = {
        "base_name": "my_pipeline",
        "env": "prod",
        "level1": "{base_name}_{env}",
        "level2": "{level1}_database",
        "level3": "table_{level2}"
    }

    print("Original:")
    for k, v in nested_placeholders.items():
        print(f"  {k:30} = {v}")

    result = replace_placeholders(nested_placeholders)
    print("\nAfter replacement:")
    for k, v in result.items():
        print(f"  {k:30} = {v}")

    # Test with lists containing placeholders
    print("\nTest 2: Lists with Placeholders")
    print("-" * 100)

    list_config = {
        "project": "analytics",
        "env": "production",
        "tables": [
            "{project}_raw_{env}",
            "{project}_stage_{env}",
            "{project}_final_{env}"
        ]
    }

    print("Original:")
    for k, v in list_config.items():
        print(f"  {k:30} = {v}")

    result = replace_placeholders(list_config)
    print("\nAfter replacement:")
    for k, v in result.items():
        print(f"  {k:30} = {v}")


if __name__ == "__main__":
    # Run all tests
    print("\n\n")
    print("#" * 100)
    print("# RUNNING ALL CONFIG HANDLER TESTS")
    print("#" * 100)

    # Test 1: Full workflow
    test_full_workflow()

    # Test 2: Individual functions
    test_individual_functions()

    # Test 3: Edge cases
    test_edge_cases()

    print("\n" + "#" * 100)
    print("# ALL TESTS COMPLETED SUCCESSFULLY")
    print("#" * 100)
    print("\n")
