#!/usr/bin/env python3
"""
Configuration Validation Script
================================
Validates pipeline configuration before deployment.

Usage:
    python deployment/validate_config.py projects/my_pipeline/config.json
"""

import sys
import argparse
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config_handler_scripts import config_loader, config_validator


def main():
    """Main validation function."""
    parser = argparse.ArgumentParser(
        description='Validate pipeline configuration'
    )
    parser.add_argument(
        'config_path',
        help='Path to config.json file'
    )
    parser.add_argument(
        '--verbose',
        '-v',
        action='store_true',
        help='Verbose output'
    )

    args = parser.parse_args()

    print("=" * 80)
    print("Pipeline Configuration Validation")
    print("=" * 80)
    print(f"Config file: {args.config_path}")
    print()

    # Check if config file exists
    if not Path(args.config_path).exists():
        print(f"ERROR: Configuration file not found: {args.config_path}")
        sys.exit(1)

    try:
        # Load configuration
        print("Loading configuration...")
        config = config_loader.load_config(args.config_path)

        pipeline_name = config.get('pipeline_metadata', {}).get('pipeline_name', 'Unknown')
        print(f"Pipeline name: {pipeline_name}")
        print()

        # Validate configuration
        print("Validating configuration...")
        is_valid, errors = config_validator.validate_config(config)

        if is_valid:
            print()
            print("=" * 80)
            print("✓ VALIDATION PASSED")
            print("=" * 80)
            print()
            print("Configuration is valid and ready for deployment.")
            print()

            if args.verbose:
                print("Configuration summary:")
                print(f"  Pipeline: {pipeline_name}")
                print(f"  Source: {config.get('source_system', {}).get('type', 'Unknown')}")
                print(f"  Owner: {config.get('pipeline_metadata', {}).get('owner_email', 'Unknown')}")
                print(f"  Schedule: {config.get('dag_schedule', {}).get('cron_expression', 'Unknown')}")
                print()

            sys.exit(0)

        else:
            print()
            print("=" * 80)
            print("✗ VALIDATION FAILED")
            print("=" * 80)
            print()
            print(f"Found {len(errors)} error(s):")
            print()

            for i, error in enumerate(errors, 1):
                print(f"{i}. {error}")

            print()
            print("Please fix these errors before deploying.")
            print()

            sys.exit(1)

    except Exception as e:
        print()
        print("=" * 80)
        print("✗ VALIDATION ERROR")
        print("=" * 80)
        print()
        print(f"Unexpected error during validation: {e}")
        print()

        if args.verbose:
            import traceback
            traceback.print_exc()

        sys.exit(1)


if __name__ == '__main__':
    main()
