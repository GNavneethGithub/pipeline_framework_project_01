"""
Phase Executor Module
=====================
Orchestrates execution of pipeline phases and manages state updates.

This is the core framework component that:
- Imports and executes user scripts dynamically
- Handles return values from user scripts
- Updates drive table with phase results
- Manages error handling and retries
"""

import logging
import importlib
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, Callable

from framework_scripts import snowflake_operations as sf_ops
from framework_scripts import error_handling as err_handler
from framework_scripts.duration_utils import format_duration, calculate_duration


logger = logging.getLogger(__name__)


class PhaseExecutor:
    """
    Executes pipeline phases and manages state transitions.
    """

    def __init__(self, config: Dict[str, Any], pipeline_id: str):
        """
        Initialize phase executor.

        Args:
            config: Pipeline configuration
            pipeline_id: Current pipeline execution ID
        """
        self.config = config
        self.pipeline_id = pipeline_id
        self.record = None

    def execute_phase(
        self,
        phase_name: str,
        user_script_path: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Execute a single pipeline phase.

        Args:
            phase_name: Name of the phase to execute
            user_script_path: Optional custom path to user script
                            (defaults to user_scripts/{phase_name}.py)

        Returns:
            dict: Phase execution result

        Example:
            >>> executor = PhaseExecutor(config, pipeline_id)
            >>> result = executor.execute_phase("source_count")
            >>> if result['skip_dag_run']:
            >>>     # Stop pipeline
            >>>     sys.exit(1)
        """
        logger.info(f"=" * 80)
        logger.info(f"Executing phase: {phase_name}")
        logger.info(f"Pipeline ID: {self.pipeline_id}")
        logger.info(f"=" * 80)

        start_time = datetime.now()

        # Get current pipeline record
        self.record = sf_ops.get_pipeline_run(self.config, self.pipeline_id)
        if not self.record:
            raise ValueError(f"Pipeline run not found: {self.pipeline_id}")

        # Check if phase is enabled
        if not self._is_phase_enabled(phase_name):
            logger.info(f"Phase {phase_name} is disabled in configuration")
            return self._handle_skipped_phase(phase_name, start_time, "Phase disabled in configuration")

        # Check if phase should be skipped (already completed in previous run)
        if self._should_skip_phase(phase_name):
            logger.info(f"Phase {phase_name} already completed in previous run")
            return self._handle_skipped_phase(phase_name, start_time, "Already completed in previous run")

        try:
            # Load and execute user script
            user_function = self._load_user_script(phase_name, user_script_path)

            logger.info(f"Calling user script function for {phase_name}")
            result = user_function(self.config, self.record)

            end_time = datetime.now()

            # Validate result
            if not isinstance(result, dict):
                raise ValueError(f"User script must return a dictionary, got {type(result)}")

            # Handle result
            return self._handle_phase_result(phase_name, result, start_time, end_time)

        except Exception as e:
            end_time = datetime.now()
            logger.error(f"Phase {phase_name} failed with exception: {e}")
            return self._handle_phase_failure(phase_name, e, start_time, end_time)

    def _load_user_script(
        self,
        phase_name: str,
        custom_path: Optional[str] = None
    ) -> Callable:
        """
        Dynamically load user script for phase.

        Args:
            phase_name: Name of the phase
            custom_path: Optional custom path to script

        Returns:
            Callable: User script function

        Raises:
            ImportError: If script cannot be loaded
        """
        if custom_path:
            script_path = Path(custom_path)
        else:
            script_path = Path("user_scripts") / f"{phase_name}.py"

        if not script_path.exists():
            raise ImportError(f"User script not found: {script_path}")

        # Import the module
        module_name = f"user_scripts.{phase_name}"

        try:
            # Add user_scripts to path if not already there
            user_scripts_dir = str(Path("user_scripts").absolute())
            if user_scripts_dir not in sys.path:
                sys.path.insert(0, user_scripts_dir)

            # Import module
            if module_name in sys.modules:
                # Reload if already imported
                module = importlib.reload(sys.modules[module_name])
            else:
                module = importlib.import_module(module_name)

            # Get the function
            function_name = phase_name  # Function name matches phase name
            if not hasattr(module, function_name):
                raise AttributeError(
                    f"User script {script_path} must define function '{function_name}'"
                )

            return getattr(module, function_name)

        except Exception as e:
            logger.error(f"Failed to load user script {script_path}: {e}")
            raise ImportError(f"Cannot load user script {script_path}: {e}")

    def _is_phase_enabled(self, phase_name: str) -> bool:
        """Check if phase is enabled in configuration."""
        phases_config = self.config.get('phases', {})
        phase_config = phases_config.get(phase_name, {})
        return phase_config.get('enabled', True)

    def _should_skip_phase(self, phase_name: str) -> bool:
        """Check if phase should be skipped (already completed in previous run)."""
        if not self.record:
            return False

        phases_skipped = self.record.get('phases_skipped', [])
        return phase_name in phases_skipped

    def _handle_phase_result(
        self,
        phase_name: str,
        result: Dict[str, Any],
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Any]:
        """
        Handle successful phase execution result.

        Args:
            phase_name: Name of phase
            result: Result from user script
            start_time: Phase start time
            end_time: Phase end time

        Returns:
            dict: Processed result
        """
        skip_dag_run = result.get('skip_dag_run', False)
        error_message = result.get('error_message')

        # Get expected duration from config
        expected_duration = self.config.get('phases', {}).get(phase_name, {}).get('expected_run_duration')

        # Create phase data for VARIANT
        phase_data = err_handler.create_phase_result(
            phase_name=phase_name,
            status='FAILED' if skip_dag_run else 'COMPLETED',
            start_timestamp=start_time,
            end_timestamp=end_time,
            expected_duration=expected_duration,
            error_message=error_message
        )

        # Add phase-specific data from result
        for key, value in result.items():
            if key not in ['skip_dag_run', 'error_message']:
                phase_data[key] = value

        # Update drive table
        sf_ops.update_phase_variant(
            self.config,
            self.pipeline_id,
            phase_name,
            phase_data
        )

        # Update phase arrays
        status = 'FAILED' if skip_dag_run else 'COMPLETED'
        sf_ops.update_phase_arrays(
            self.config,
            self.pipeline_id,
            phase_name,
            status
        )

        # Log result
        duration = format_duration(calculate_duration(start_time, end_time))
        if skip_dag_run:
            logger.error(f"Phase {phase_name} failed: {error_message}")
            logger.info(f"Duration: {duration}")
        else:
            logger.info(f"Phase {phase_name} completed successfully")
            logger.info(f"Duration: {duration}")

        return result

    def _handle_phase_failure(
        self,
        phase_name: str,
        error: Exception,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Any]:
        """
        Handle phase execution failure (exception raised).

        Args:
            phase_name: Name of phase
            error: Exception that occurred
            start_time: Phase start time
            end_time: Phase end time

        Returns:
            dict: Error result
        """
        # Use error handler to process error
        result = err_handler.handle_phase_error(
            phase_name,
            error,
            self.config,
            self.record
        )

        # Get expected duration from config
        expected_duration = self.config.get('phases', {}).get(phase_name, {}).get('expected_run_duration')

        # Create phase data for VARIANT
        phase_data = err_handler.log_to_variant(
            {
                'phase_name': phase_name,
                'status': 'FAILED',
                'start_timestamp': start_time.isoformat(),
                'end_timestamp': end_time.isoformat(),
                'actual_run_duration': format_duration(calculate_duration(start_time, end_time)),
                'expected_run_duration': expected_duration
            },
            error
        )

        # Update drive table
        sf_ops.update_phase_variant(
            self.config,
            self.pipeline_id,
            phase_name,
            phase_data
        )

        # Update phase arrays
        sf_ops.update_phase_arrays(
            self.config,
            self.pipeline_id,
            phase_name,
            'FAILED'
        )

        logger.error(f"Phase {phase_name} failed with exception")
        logger.error(f"Duration: {format_duration(calculate_duration(start_time, end_time))}")

        return result

    def _handle_skipped_phase(
        self,
        phase_name: str,
        start_time: datetime,
        reason: str
    ) -> Dict[str, Any]:
        """
        Handle skipped phase.

        Args:
            phase_name: Name of phase
            start_time: Phase start time
            reason: Reason for skipping

        Returns:
            dict: Skipped result
        """
        end_time = datetime.now()

        # Create phase data for VARIANT
        phase_data = {
            'phase_name': phase_name,
            'status': 'SKIPPED',
            'start_timestamp': start_time.isoformat(),
            'end_timestamp': end_time.isoformat(),
            'actual_run_duration': format_duration(calculate_duration(start_time, end_time)),
            'skip_reason': reason
        }

        # Update drive table
        sf_ops.update_phase_variant(
            self.config,
            self.pipeline_id,
            phase_name,
            phase_data
        )

        # Update phase arrays
        sf_ops.update_phase_arrays(
            self.config,
            self.pipeline_id,
            phase_name,
            'SKIPPED'
        )

        logger.info(f"Phase {phase_name} skipped: {reason}")

        return {
            'skip_dag_run': False,
            'error_message': None,
            'status': 'SKIPPED'
        }


def execute_pipeline(
    config: Dict[str, Any],
    pipeline_id: str,
    phases_to_execute: Optional[list] = None
) -> bool:
    """
    Execute complete pipeline or subset of phases.

    Args:
        config: Pipeline configuration
        pipeline_id: Pipeline execution ID
        phases_to_execute: Optional list of phases to execute
                          (defaults to all enabled phases)

    Returns:
        bool: True if all phases succeeded, False if any failed

    Example:
        >>> success = execute_pipeline(config, pipeline_id)
        >>> if not success:
        >>>     sys.exit(1)
    """
    executor = PhaseExecutor(config, pipeline_id)

    # Default phases
    if phases_to_execute is None:
        phases_to_execute = [
            'stale_pipeline_handling',
            'pre_validation',
            'source_to_stage_transfer',
            'stage_to_target_transfer',
            'audit',
            'stage_cleaning',
            'target_cleaning'
        ]

    all_succeeded = True

    for phase_name in phases_to_execute:
        result = executor.execute_phase(phase_name)

        if result.get('skip_dag_run', False):
            logger.error(f"Pipeline stopped at phase: {phase_name}")
            all_succeeded = False

            # Finalize pipeline as FAILED
            sf_ops.finalize_pipeline_run(config, pipeline_id, 'FAILED')

            break

    if all_succeeded:
        logger.info("All phases completed successfully")
        # Finalize pipeline as SUCCESS
        sf_ops.finalize_pipeline_run(config, pipeline_id, 'SUCCESS')

    return all_succeeded


# Example usage
if __name__ == "__main__":
    print("Phase Executor Module")
    print("=" * 80)
    print("Main class: PhaseExecutor")
    print("Main function: execute_pipeline()")
    print("\nUsage:")
    print("  executor = PhaseExecutor(config, pipeline_id)")
    print("  result = executor.execute_phase('source_count')")
    print("  if result['skip_dag_run']:")
    print("      # Stop pipeline")
    print("      sys.exit(1)")
