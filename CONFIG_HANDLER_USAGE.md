# Config Handler Usage Guide

## Overview

The Config Handler module provides powerful utilities to flatten nested JSON configurations and replace placeholders dynamically. This makes it easy to manage complex configurations with interdependent values.

## Core Functions

### 1. `flatten_dict(nested_dict, parent_key='', separator='_')`

Flattens a nested dictionary by joining keys with underscores.

**Example:**
```python
from config_handler_scripts import flatten_dict

nested = {
    "elasticsearch": {
        "index_old_name": "lsf_x",
        "index_new_name": "completed_jobs"
    },
    "aws": {
        "aws_s3_bucket": "bucket_x"
    }
}

flattened = flatten_dict(nested)
# Result:
# {
#     'elasticsearch_index_old_name': 'lsf_x',
#     'elasticsearch_index_new_name': 'completed_jobs',
#     'aws_aws_s3_bucket': 'bucket_x'
# }
```

### 2. `replace_placeholders(flattened_dict, max_iterations=10)`

Replaces `{placeholder}` patterns in values with actual values from the dictionary.

**Example:**
```python
from config_handler_scripts import replace_placeholders

config = {
    'elasticsearch_index_old_name': 'lsf_x',
    'elasticsearch_index_new_name': 'completed_jobs',
    'snowflake_raw_database': 'db_{index_new_name}',
    'snowflake_raw_table': '{index_old_name}_tbl'
}

result = replace_placeholders(config)
# Result:
# {
#     'elasticsearch_index_old_name': 'lsf_x',
#     'elasticsearch_index_new_name': 'completed_jobs',
#     'snowflake_raw_database': 'db_completed_jobs',
#     'snowflake_raw_table': 'lsf_x_tbl'
# }
```

**Key Features:**
- Automatically searches for placeholder keys ending with the placeholder name
- Handles nested placeholders through multiple iterations
- Works with both string values and lists containing placeholders
- Prevents infinite loops with max_iterations parameter

### 3. `apply_extra_dicts(processed_dict, extra_dicts)`

Merges additional key-value pairs and performs another round of placeholder replacement.

**Example:**
```python
from config_handler_scripts import apply_extra_dicts

config = {
    'elasticsearch_index_new_name': 'completed_jobs',
    'snowflake_raw_database': 'db_completed_jobs'
}

extra = {
    'env': 'production',
    'custom_prefix': 'pipeline_{env}_{index_new_name}'
}

final = apply_extra_dicts(config, extra)
# Result:
# {
#     'elasticsearch_index_new_name': 'completed_jobs',
#     'snowflake_raw_database': 'db_completed_jobs',
#     'env': 'production',
#     'custom_prefix': 'pipeline_production_completed_jobs'
# }
```

### 4. `load_and_process_config(absolute_root_to_project_folder_path, relative_project_to_config_file_path, extra_dicts=None)`

Main orchestration function that performs the complete workflow.

**Example:**
```python
from config_handler_scripts import load_and_process_config

config = load_and_process_config(
    absolute_root_to_project_folder_path='/home/user/pipeline_framework_project_01',
    relative_project_to_config_file_path='sample_config.json',
    extra_dicts={
        'env': 'production',
        'region': 'us-east-1',
        'custom_path': 's3://{aws_s3_bucket}/{env}/{index_new_name}'
    }
)

# Access the processed values
print(config['elasticsearch_index_new_name'])  # 'completed_jobs'
print(config['snowflake_raw_database'])        # 'db_completed_jobs'
print(config['custom_path'])                   # 's3://bucket_x/production/completed_jobs'
```

## Complete Workflow

The `load_and_process_config` function performs these steps:

1. **Load JSON config** from the specified file path
2. **Flatten** the nested structure with underscore-separated keys
3. **Replace placeholders** in all values (including lists)
4. **Merge extra_dicts** (if provided)
5. **Replace placeholders again** to resolve any new placeholders from extra_dicts
6. **Return** the final processed configuration dictionary

## Sample Config Structure

```json
{
  "elasticsearch": {
    "index_old_name": "lsf_x",
    "index_new_name": "completed_jobs"
  },
  "aws": {
    "aws_s3_bucket": "bucket_x",
    "prefix_list": [
      "pipeline_x_{env}",
      "{index_new_name}",
      "{index_old_name}"
    ]
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
```

## Advanced Features

### Nested Placeholder Resolution

The handler can resolve nested placeholders automatically:

```python
config = {
    "base_name": "my_pipeline",
    "env": "prod",
    "level1": "{base_name}_{env}",
    "level2": "{level1}_database",
    "level3": "table_{level2}"
}

result = replace_placeholders(config)
# Result:
# {
#     'base_name': 'my_pipeline',
#     'env': 'prod',
#     'level1': 'my_pipeline_prod',
#     'level2': 'my_pipeline_prod_database',
#     'level3': 'table_my_pipeline_prod_database'
# }
```

### List Placeholder Replacement

Works seamlessly with lists:

```python
config = {
    "project": "analytics",
    "env": "production",
    "tables": [
        "{project}_raw_{env}",
        "{project}_stage_{env}",
        "{project}_final_{env}"
    ]
}

result = replace_placeholders(config)
# Result:
# {
#     'project': 'analytics',
#     'env': 'production',
#     'tables': [
#         'analytics_raw_production',
#         'analytics_stage_production',
#         'analytics_final_production'
#     ]
# }
```

## Testing

Run the comprehensive test suite:

```bash
python test_config_handler.py
```

Or run the module directly to see examples:

```bash
python config_handler_scripts/config_handler.py
```

## Benefits

1. **Easy Configuration Management**: Dump many key-value pairs in nested JSON
2. **Automatic Flattening**: Get a single-level dictionary with clear, hierarchical keys
3. **Dynamic Placeholder Resolution**: Reference values anywhere in your config
4. **Custom Modifications**: Use `extra_dicts` to add environment-specific or computed values
5. **DRY Principle**: Define values once and reference them throughout your config

## Files Created

- `config_handler_scripts/config_handler.py` - Main module
- `test_config_handler.py` - Comprehensive test suite
- `sample_config.json` - Example configuration file
- `CONFIG_HANDLER_USAGE.md` - This documentation

## Integration with Existing Code

The config handler is already integrated into the `config_handler_scripts` package and can be imported directly:

```python
# Import individual functions
from config_handler_scripts import (
    flatten_dict,
    replace_placeholders,
    apply_extra_dicts,
    load_and_process_config
)

# Or import the whole module
from config_handler_scripts import config_handler
```
