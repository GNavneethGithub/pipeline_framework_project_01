# Robust Data Pipeline Framework

A production-grade, plug-and-play data pipeline framework for incremental data loads from multiple sources to Snowflake.

## Overview

This framework provides a complete solution for building robust, maintainable data pipelines with:

- **Incremental Data Loading**: Process data in configurable time windows
- **Automatic Retry & Continuation**: Failed pipelines resume from failure point
- **Comprehensive Tracking**: Central drive table tracks all execution metadata
- **Flexible Phase Configuration**: Enable/disable phases, customize behavior
- **Data Validation**: Count-based validation with configurable tolerances
- **Automatic Cleanup**: Stage data archival and cleanup

## Quick Start

### 1. Prerequisites

- Python 3.8+
- Apache Airflow 2.0+
- Snowflake account and credentials
- Source system access (Elasticsearch, MySQL, etc.)

### 2. Installation

```bash
# Clone the repository
git clone <repository-url>
cd pipeline_framework_project_01

# Install dependencies
pip install -r requirements.txt

# Create Snowflake drive table
snowsql -f database_schemas/drive_table_ddl.sql
```

### 3. Create Your First Pipeline

```bash
# Create a new pipeline directory
mkdir -p projects/my_pipeline

# Generate configuration template
python -c "
from config_handler_scripts.config_validator import create_config_template
from config_handler_scripts.config_loader import save_config

config = create_config_template()
config['pipeline_metadata']['pipeline_name'] = 'my_pipeline'
save_config(config, 'projects/my_pipeline/config.json')
"

# Edit the configuration
nano projects/my_pipeline/config.json

# Validate configuration
python deployment/validate_config.py projects/my_pipeline/config.json

# Copy the DAG template
cp projects/example_pipeline/main_dag.py projects/my_pipeline/
```

### 4. Deploy to Airflow

```bash
# Copy DAG to Airflow DAGs folder
cp projects/my_pipeline/main_dag.py $AIRFLOW_HOME/dags/

# Airflow will auto-discover the DAG
# Check Airflow UI after ~60 seconds
```

## Project Structure

```
pipeline_framework_project_01/
├── framework_scripts/          # Core framework components
│   ├── duration_utils.py       # Duration calculation and formatting
│   ├── snowflake_operations.py # Snowflake database operations
│   ├── error_handling.py       # Error handling and classification
│   └── phase_executor.py       # Phase execution orchestration
│
├── user_scripts/               # User-provided phase implementations
│   ├── stale_pipeline_handling.py
│   ├── pre_validation.py
│   ├── source_to_stage_transfer.py
│   ├── stage_to_target_transfer.py
│   ├── audit.py
│   ├── stage_cleaning.py
│   └── target_cleaning.py
│
├── config_handler_scripts/     # Configuration management
│   ├── config_loader.py
│   └── config_validator.py
│
├── database_schemas/           # DDL scripts
│   └── drive_table_ddl.sql
│
├── projects/                   # Individual pipeline implementations
│   └── example_pipeline/
│       ├── config.json
│       └── main_dag.py
│
├── deployment/                 # Deployment utilities
├── tests/                      # Unit and integration tests
└── docs/                       # Additional documentation
```

## Pipeline Phases

Each pipeline executes through the following phases:

1. **Stale Pipeline Handling**: Detect and resolve stuck records
2. **Pre-Validation**: Validate prerequisites, determine fresh/retry run
3. **Source to Stage Transfer**: Extract from source, load to stage
4. **Stage to Target Transfer**: Transform and load to target
5. **Audit**: Validate data integrity across stages
6. **Stage Cleaning**: Archive and cleanup temporary data
7. **Target Cleaning** (optional): Custom target transformations

## Configuration

Pipeline behavior is controlled via `config.json`:

```json
{
    "pipeline_metadata": {
        "pipeline_name": "my_pipeline",
        "owner_email": "team@company.com"
    },
    "source_system": {
        "type": "elasticsearch",
        "endpoint": "https://es.company.com:9200"
    },
    "phases": {
        "audit": {
            "enabled": true,
            "count_tolerances": {
                "source_to_stage_tolerance_percent": 2.0
            }
        }
    }
}
```

### Required Configuration Sections

- `pipeline_metadata`: Pipeline name, owner, notifications
- `source_system`: Source connection details
- `stage_system`: Snowflake stage table info
- `target_system`: Snowflake target table info
- `phases`: Phase-specific configuration
- `snowflake_connection`: Snowflake credentials

## User Scripts

Implement business logic by customizing user scripts:

### Example: source_to_stage_transfer.py

```python
def source_to_stage_transfer(config, record):
    """Extract from source and load to stage."""

    # 1. Connect to source
    source_type = config['source_system']['type']

    # 2. Extract data for query window
    query_window_start = record['query_window_start_timestamp']
    query_window_end = record['query_window_end_timestamp']

    # 3. Validate and load to stage
    records_loaded = extract_and_load(config, query_window_start, query_window_end)

    # 4. Return result
    return {
        'skip_dag_run': False,
        'error_message': None,
        'transfer_completed': True,
        'stage_count': records_loaded
    }
```

## Monitoring

### Drive Table Queries

```sql
-- Find failed pipelines in last 24 hours
SELECT pipeline_name, phase_failed, created_at
FROM pipeline_execution_drive
WHERE pipeline_status = 'FAILED'
  AND created_at > DATEADD(hour, -24, CURRENT_TIMESTAMP());

-- Get audit details for a specific run
SELECT audit_phase:source_count,
       audit_phase:stage_count,
       audit_phase:target_count,
       audit_phase:validation_status
FROM pipeline_execution_drive
WHERE pipeline_id = 'my_pipeline_20251115_10h_run1';
```

### Airflow UI

- Monitor real-time pipeline progress
- View task logs
- Trigger manual runs
- Check execution history

## Error Handling

The framework automatically handles errors:

- **Transient Errors** (timeouts, locks): Automatic retry on next run
- **Permanent Errors** (schema mismatch): Alert and require manual fix
- **Configuration Errors**: Validation prevents deployment

### Retry Behavior

When a phase fails:
1. Pipeline stops immediately
2. Drive table records failure point
3. On next execution, framework skips completed phases
4. Resumes from failed phase

## Testing

```bash
# Run unit tests
python -m pytest tests/

# Validate configuration
python deployment/validate_config.py projects/my_pipeline/config.json

# Test user scripts
python -m pytest tests/test_user_scripts.py
```

## Deployment

### Pre-Deployment Checklist

- [ ] Configuration validated
- [ ] Source system accessible
- [ ] Snowflake credentials configured
- [ ] Drive table created
- [ ] User scripts implemented
- [ ] Dry-run tested

### Deployment Steps

1. **Validate Configuration**
   ```bash
   python deployment/validate_config.py projects/my_pipeline/config.json
   ```

2. **Deploy to Airflow**
   ```bash
   cp projects/my_pipeline/main_dag.py $AIRFLOW_HOME/dags/
   ```

3. **Enable in Airflow UI**
   - Navigate to Airflow UI
   - Find DAG `pipeline_my_pipeline`
   - Toggle to enabled

4. **Monitor First Run**
   - Trigger manual run or wait for schedule
   - Watch task progress
   - Check drive table for results

## Troubleshooting

### Common Issues

**Issue**: Pipeline fails at source_to_stage_transfer

**Solution**: Check source system accessibility and credentials

---

**Issue**: Audit phase fails with count mismatch

**Solution**: Review tolerance settings, check data quality

---

**Issue**: Stage cleaning fails

**Solution**: Check S3 permissions and archive location

## Documentation

- [Complete Project Documentation](PIPELINE_DECISIONS_SUMMARY.md)
- [Database Schema Details](database_schemas/drive_table_ddl.sql)
- [Configuration Reference](config_handler_scripts/config_validator.py)

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make changes
4. Run tests
5. Submit pull request

## License

MIT License - see LICENSE file for details

## Support

For issues and questions:
- GitHub Issues: [repository-url]/issues
- Email: data-team@company.com
- Slack: #data-pipeline-support

## Version History

- **1.0.0** (2025-11-15): Initial release
  - Core framework components
  - User scripts templates
  - Configuration management
  - Airflow integration
  - Example pipeline

---

Built with ❤️ by the Data Engineering Team
