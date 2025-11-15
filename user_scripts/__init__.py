"""
User Scripts Package
===================
User-provided phase implementations for pipeline execution.

Modules:
    - stale_pipeline_handling: Handle stuck/stale records
    - pre_validation: Validate prerequisites and determine execution strategy
    - source_to_stage_transfer: Extract from source and load to stage
    - stage_to_target_transfer: Transform and load from stage to target
    - audit: Validate data integrity across stages
    - stage_cleaning: Clean up temporary stage data
    - target_cleaning: Optional target data cleanup
"""

__version__ = '1.0.0'
