"""
Connectivity Checker Module
============================
Orchestrates connectivity checks to all required systems.

This module runs all connectivity checks and aggregates results.
"""

import logging
import sys
from pathlib import Path

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from user_scripts.connectivity_checks import (
    check_snowflake,
    check_source,
    check_stage,
    check_target
)

logger = logging.getLogger(__name__)


def check_all_connections(config: dict) -> dict:
    """
    Check connectivity to all required systems.

    This function calls all individual connectivity check functions
    and aggregates the results. If any check fails, the entire
    connectivity check fails.

    Args:
        config: Pipeline configuration dictionary

    Returns:
        dict: {
            'skip_dag_run': bool,           # True if any check failed
            'error_message': str or None,   # Aggregated error messages
            'connected': bool,              # True if all checks passed
            'snowflake': dict,              # Individual check result
            'source': dict,                 # Individual check result
            'stage': dict,                  # Individual check result
            'target': dict                  # Individual check result
        }
    """
    logger.info("=" * 80)
    logger.info("Starting connectivity checks...")
    logger.info("=" * 80)

    # Initialize overall result
    overall_result = {
        'skip_dag_run': False,
        'error_message': None,
        'connected': True,
        'checks': {}
    }

    error_messages = []

    # ========================================================================
    # Check 1: Snowflake Connectivity
    # ========================================================================
    logger.info("\n[1/4] Checking Snowflake connectivity...")
    try:
        snowflake_result = check_snowflake.check_snowflake_connection(config)
        overall_result['checks']['snowflake'] = snowflake_result

        if snowflake_result.get('skip_dag_run', False):
            logger.error(f"  ✗ Snowflake check FAILED: {snowflake_result.get('error_message')}")
            error_messages.append(f"Snowflake: {snowflake_result.get('error_message')}")
        else:
            logger.info("  ✓ Snowflake check PASSED")

    except Exception as e:
        error_msg = f"Snowflake connectivity check crashed: {str(e)}"
        logger.error(f"  ✗ {error_msg}")
        error_messages.append(error_msg)
        overall_result['checks']['snowflake'] = {
            'skip_dag_run': True,
            'error_message': error_msg,
            'connected': False
        }

    # ========================================================================
    # Check 2: Source Connectivity
    # ========================================================================
    logger.info("\n[2/4] Checking Source connectivity...")
    try:
        source_result = check_source.check_source_connection(config)
        overall_result['checks']['source'] = source_result

        if source_result.get('skip_dag_run', False):
            logger.error(f"  ✗ Source check FAILED: {source_result.get('error_message')}")
            error_messages.append(f"Source: {source_result.get('error_message')}")
        else:
            logger.info("  ✓ Source check PASSED")

    except Exception as e:
        error_msg = f"Source connectivity check crashed: {str(e)}"
        logger.error(f"  ✗ {error_msg}")
        error_messages.append(error_msg)
        overall_result['checks']['source'] = {
            'skip_dag_run': True,
            'error_message': error_msg,
            'connected': False
        }

    # ========================================================================
    # Check 3: Stage Connectivity
    # ========================================================================
    logger.info("\n[3/4] Checking Stage connectivity...")
    try:
        stage_result = check_stage.check_stage_connection(config)
        overall_result['checks']['stage'] = stage_result

        if stage_result.get('skip_dag_run', False):
            logger.error(f"  ✗ Stage check FAILED: {stage_result.get('error_message')}")
            error_messages.append(f"Stage: {stage_result.get('error_message')}")
        else:
            logger.info("  ✓ Stage check PASSED")

    except Exception as e:
        error_msg = f"Stage connectivity check crashed: {str(e)}"
        logger.error(f"  ✗ {error_msg}")
        error_messages.append(error_msg)
        overall_result['checks']['stage'] = {
            'skip_dag_run': True,
            'error_message': error_msg,
            'connected': False
        }

    # ========================================================================
    # Check 4: Target Connectivity
    # ========================================================================
    logger.info("\n[4/4] Checking Target connectivity...")
    try:
        target_result = check_target.check_target_connection(config)
        overall_result['checks']['target'] = target_result

        if target_result.get('skip_dag_run', False):
            logger.error(f"  ✗ Target check FAILED: {target_result.get('error_message')}")
            error_messages.append(f"Target: {target_result.get('error_message')}")
        else:
            logger.info("  ✓ Target check PASSED")

    except Exception as e:
        error_msg = f"Target connectivity check crashed: {str(e)}"
        logger.error(f"  ✗ {error_msg}")
        error_messages.append(error_msg)
        overall_result['checks']['target'] = {
            'skip_dag_run': True,
            'error_message': error_msg,
            'connected': False
        }

    # ========================================================================
    # Aggregate Results
    # ========================================================================
    logger.info("\n" + "=" * 80)
    if error_messages:
        overall_result['skip_dag_run'] = True
        overall_result['connected'] = False
        overall_result['error_message'] = "Connectivity checks failed:\n" + "\n".join(f"  - {msg}" for msg in error_messages)

        logger.error("Connectivity checks FAILED")
        logger.error(overall_result['error_message'])
        logger.info("=" * 80)
    else:
        overall_result['skip_dag_run'] = False
        overall_result['connected'] = True
        overall_result['error_message'] = None

        logger.info("All connectivity checks PASSED ✓")
        logger.info("=" * 80)

    return overall_result


def main():
    """
    Main function for testing connectivity checker.

    This can be run standalone to test connectivity checks.
    """
    import json

    # For testing, load config from example pipeline
    config_path = project_root / 'projects' / 'example_pipeline' / 'config.json'

    if config_path.exists():
        with open(config_path, 'r') as f:
            config = json.load(f)

        result = check_all_connections(config)

        print("\n" + "=" * 80)
        print("CONNECTIVITY CHECK RESULTS")
        print("=" * 80)
        print(json.dumps(result, indent=2))

        sys.exit(0 if result['connected'] else 1)
    else:
        print(f"Config file not found: {config_path}")
        sys.exit(1)


if __name__ == "__main__":
    main()
