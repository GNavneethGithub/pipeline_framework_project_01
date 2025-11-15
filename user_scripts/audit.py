"""
Audit Phase User Script
========================
This script validates data integrity across all stages by comparing counts
and ensuring data wasn't lost or corrupted during transfer.

Function Signature:
    audit(config: Dict, record: Dict) -> Dict

Return Values:
    {
        'skip_dag_run': bool,  # True = audit failed, False = passed
        'error_message': str or None,
        'audit_passed': bool,  # True if validation passed
        'source_count': int,
        'stage_count': int,
        'target_count': int,
        'source_to_stage_loss_percent': float,
        'stage_to_target_loss_percent': float,
        'validation_status': str  # PASSED or FAILED
    }
"""

import logging
from typing import Dict, Any
import json


logger = logging.getLogger(__name__)


def audit(config: Dict[str, Any], record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate data integrity across source, stage, and target.

    Args:
        config: Pipeline configuration
        record: Current drive table row

    Returns:
        dict: Audit result

    This function:
    1. Extracts counts from previous phases (stored in VARIANT columns)
    2. Calculates data loss percentages
    3. Compares against configured tolerances
    4. Returns PASS or FAIL
    """
    logger.info("Starting audit phase")

    try:
        # Step 1: Extract counts from phase VARIANT columns
        logger.info("Extracting counts from previous phases...")

        # Get source_count from source_to_stage_transfer_phase
        source_to_stage_phase = record.get('source_to_stage_transfer_phase', {})
        if isinstance(source_to_stage_phase, str):
            source_to_stage_phase = json.loads(source_to_stage_phase)

        source_count = source_to_stage_phase.get('source_count', 0)
        stage_count = source_to_stage_phase.get('stage_count', 0)

        # Get target_count from stage_to_target_transfer_phase
        stage_to_target_phase = record.get('stage_to_target_transfer_phase', {})
        if isinstance(stage_to_target_phase, str):
            stage_to_target_phase = json.loads(stage_to_target_phase)

        target_count = stage_to_target_phase.get('target_count', 0)

        logger.info(f"Source count: {source_count}")
        logger.info(f"Stage count: {stage_count}")
        logger.info(f"Target count: {target_count}")

        # Step 2: Calculate loss percentages
        logger.info("Calculating data loss percentages...")

        # Source to Stage loss
        if source_count > 0:
            source_to_stage_loss_percent = ((source_count - stage_count) / source_count) * 100
        else:
            source_to_stage_loss_percent = 0.0

        # Stage to Target loss
        if stage_count > 0:
            stage_to_target_loss_percent = ((stage_count - target_count) / stage_count) * 100
        else:
            stage_to_target_loss_percent = 0.0

        logger.info(f"Source to Stage loss: {source_to_stage_loss_percent:.2f}%")
        logger.info(f"Stage to Target loss: {stage_to_target_loss_percent:.2f}%")

        # Step 3: Get tolerances from config
        audit_config = config.get('phases', {}).get('audit', {})
        count_tolerances = audit_config.get('count_tolerances', {})

        source_to_stage_tolerance = count_tolerances.get('source_to_stage_tolerance_percent', 2.0)
        stage_to_target_tolerance = count_tolerances.get('stage_to_target_tolerance_percent', 1.0)

        logger.info(f"Source to Stage tolerance: {source_to_stage_tolerance}%")
        logger.info(f"Stage to Target tolerance: {stage_to_target_tolerance}%")

        # Step 4: Validate counts
        validation_results = []

        # Validate source to stage
        source_to_stage_passed = source_to_stage_loss_percent <= source_to_stage_tolerance

        if source_to_stage_passed:
            logger.info("✓ Source to Stage validation PASSED")
            validation_results.append({
                'stage': 'source_to_stage',
                'passed': True,
                'loss_percent': source_to_stage_loss_percent,
                'tolerance_percent': source_to_stage_tolerance
            })
        else:
            logger.error(f"✗ Source to Stage validation FAILED")
            logger.error(f"  Loss: {source_to_stage_loss_percent:.2f}% exceeds tolerance: {source_to_stage_tolerance}%")
            validation_results.append({
                'stage': 'source_to_stage',
                'passed': False,
                'loss_percent': source_to_stage_loss_percent,
                'tolerance_percent': source_to_stage_tolerance,
                'reason': f'Loss {source_to_stage_loss_percent:.2f}% exceeds tolerance {source_to_stage_tolerance}%'
            })

        # Validate stage to target
        stage_to_target_passed = stage_to_target_loss_percent <= stage_to_target_tolerance

        if stage_to_target_passed:
            logger.info("✓ Stage to Target validation PASSED")
            validation_results.append({
                'stage': 'stage_to_target',
                'passed': True,
                'loss_percent': stage_to_target_loss_percent,
                'tolerance_percent': stage_to_target_tolerance
            })
        else:
            logger.error(f"✗ Stage to Target validation FAILED")
            logger.error(f"  Loss: {stage_to_target_loss_percent:.2f}% exceeds tolerance: {stage_to_target_tolerance}%")
            validation_results.append({
                'stage': 'stage_to_target',
                'passed': False,
                'loss_percent': stage_to_target_loss_percent,
                'tolerance_percent': stage_to_target_tolerance,
                'reason': f'Loss {stage_to_target_loss_percent:.2f}% exceeds tolerance {stage_to_target_tolerance}%'
            })

        # Overall validation status
        audit_passed = source_to_stage_passed and stage_to_target_passed

        if audit_passed:
            logger.info("=" * 60)
            logger.info("AUDIT RESULT: PASSED ✓")
            logger.info("=" * 60)
            validation_status = "PASSED"
            error_message = None
            skip_dag_run = False
        else:
            logger.error("=" * 60)
            logger.error("AUDIT RESULT: FAILED ✗")
            logger.error("=" * 60)
            validation_status = "FAILED"

            # Build error message
            failed_checks = [v for v in validation_results if not v['passed']]
            error_message = "Audit validation failed: " + "; ".join([
                f"{v['stage']}: {v.get('reason', 'Unknown')}" for v in failed_checks
            ])

            skip_dag_run = True

        # Return audit result
        return {
            'skip_dag_run': skip_dag_run,
            'error_message': error_message,
            'audit_passed': audit_passed,
            'source_count': source_count,
            'stage_count': stage_count,
            'target_count': target_count,
            'source_to_stage_loss_percent': round(source_to_stage_loss_percent, 2),
            'source_to_stage_tolerance_percent': source_to_stage_tolerance,
            'stage_to_target_loss_percent': round(stage_to_target_loss_percent, 2),
            'stage_to_target_tolerance_percent': stage_to_target_tolerance,
            'validation_status': validation_status,
            'validation_results': validation_results,
            'loss_reasons': determine_loss_reasons(source_count, stage_count, target_count)
        }

    except Exception as e:
        logger.error(f"Audit phase failed with exception: {e}")
        return {
            'skip_dag_run': True,
            'error_message': f"Audit failed: {str(e)}",
            'audit_passed': False,
            'source_count': 0,
            'stage_count': 0,
            'target_count': 0,
            'source_to_stage_loss_percent': 0.0,
            'stage_to_target_loss_percent': 0.0,
            'validation_status': 'FAILED'
        }


def determine_loss_reasons(
    source_count: int,
    stage_count: int,
    target_count: int
) -> str:
    """
    Determine likely reasons for data loss.

    Args:
        source_count: Count at source
        stage_count: Count at stage
        target_count: Count at target

    Returns:
        str: Explanation of data loss
    """
    reasons = []

    # Source to Stage loss
    source_to_stage_loss = source_count - stage_count
    if source_to_stage_loss > 0:
        reasons.append(
            f"Source to Stage: {source_to_stage_loss} records lost "
            f"(likely due to validation failures, schema mismatches, or data quality issues)"
        )
    elif source_to_stage_loss < 0:
        reasons.append(
            f"Source to Stage: {abs(source_to_stage_loss)} extra records "
            f"(possible duplicate insertion - investigate!)"
        )

    # Stage to Target loss
    stage_to_target_loss = stage_count - target_count
    if stage_to_target_loss > 0:
        reasons.append(
            f"Stage to Target: {stage_to_target_loss} records lost "
            f"(likely due to deduplication, filtering, or constraint violations)"
        )
    elif stage_to_target_loss < 0:
        reasons.append(
            f"Stage to Target: {abs(stage_to_target_loss)} extra records "
            f"(possible duplicate insertion - investigate!)"
        )

    if not reasons:
        reasons.append("No data loss detected - perfect transfer!")

    return "; ".join(reasons)


# Example usage
if __name__ == "__main__":
    # Test scenarios
    test_scenarios = [
        {
            'name': 'Perfect transfer (no loss)',
            'source_count': 10000,
            'stage_count': 10000,
            'target_count': 10000,
            'tolerances': {'source_to_stage': 2.0, 'stage_to_target': 1.0}
        },
        {
            'name': 'Acceptable loss within tolerance',
            'source_count': 10000,
            'stage_count': 9800,  # 2% loss
            'target_count': 9800,  # 0% loss
            'tolerances': {'source_to_stage': 2.0, 'stage_to_target': 1.0}
        },
        {
            'name': 'Excessive loss - audit fails',
            'source_count': 10000,
            'stage_count': 8000,  # 20% loss!
            'target_count': 8000,
            'tolerances': {'source_to_stage': 2.0, 'stage_to_target': 1.0}
        }
    ]

    print("Audit Script Test Scenarios")
    print("=" * 60)

    for scenario in test_scenarios:
        print(f"\nScenario: {scenario['name']}")
        print(f"  Source: {scenario['source_count']}")
        print(f"  Stage: {scenario['stage_count']}")
        print(f"  Target: {scenario['target_count']}")

        source_loss = ((scenario['source_count'] - scenario['stage_count']) / scenario['source_count']) * 100
        stage_loss = ((scenario['stage_count'] - scenario['target_count']) / scenario['stage_count']) * 100 if scenario['stage_count'] > 0 else 0

        print(f"  Source→Stage loss: {source_loss:.2f}% (tolerance: {scenario['tolerances']['source_to_stage']}%)")
        print(f"  Stage→Target loss: {stage_loss:.2f}% (tolerance: {scenario['tolerances']['stage_to_target']}%)")

        would_pass = (
            source_loss <= scenario['tolerances']['source_to_stage'] and
            stage_loss <= scenario['tolerances']['stage_to_target']
        )

        print(f"  Result: {'✓ PASS' if would_pass else '✗ FAIL'}")
