# Connectivity Checks

This directory contains user-provided scripts to check connectivity to all required systems before running the pipeline.

## Overview

The connectivity check task is the **first task** that runs in the DAG. It verifies that the pipeline can connect to:
- **Snowflake** - Data warehouse
- **Source** - Source data system
- **Stage** - Staging area (S3, Azure Blob, etc.)
- **Target** - Target system

If **any** connectivity check fails, the entire DAG will be stopped and an alert will be sent.

## How It Works

1. The DAG calls `check_all_connections()` from `framework_scripts/connectivity_checker.py`
2. This function calls each individual check script in this directory
3. Each script returns a standardized result dictionary
4. If any check fails (`skip_dag_run=True`), the DAG stops and sends an alert
5. If all checks pass, the pipeline proceeds

## Return Format

Each connectivity check script must return a dictionary with this exact structure:

```python
{
    'skip_dag_run': bool,           # True = fail, False = pass
    'error_message': str or None,   # Error details if failed
    'connected': bool               # True if connected successfully
}
```

## Error Handling

- Each script uses `try/except` to catch errors
- Errors are captured in `error_message` and returned to the top level
- **DO NOT use `raise`** - return the error in the dictionary instead
- The framework handles displaying errors in Airflow

## User Implementation Required

You must implement the actual connectivity logic in these files:

### 1. `check_snowflake.py`

Implement `check_snowflake_connection(config: dict)` to verify Snowflake connectivity.

**Example implementation:**

```python
def check_snowflake_connection(config: dict) -> dict:
    result = {
        'skip_dag_run': False,
        'error_message': None,
        'connected': False
    }

    try:
        snowflake_config = config.get('snowflake', {})

        conn = snowflake.connector.connect(
            user=snowflake_config.get('user'),
            password=snowflake_config.get('password'),
            account=snowflake_config.get('account'),
            warehouse=snowflake_config.get('warehouse'),
            database=snowflake_config.get('database'),
            schema=snowflake_config.get('schema')
        )

        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_VERSION()")
        version = cursor.fetchone()
        cursor.close()
        conn.close()

        logger.info(f"Connected to Snowflake version: {version[0]}")
        result['connected'] = True

    except Exception as e:
        logger.error(f"Failed to connect to Snowflake: {str(e)}")
        result['skip_dag_run'] = True
        result['error_message'] = f"Snowflake connection failed: {str(e)}"
        result['connected'] = False

    return result
```

### 2. `check_source.py`

Implement `check_source_connection(config: dict)` to verify source system connectivity.

This could be:
- Database connection (PostgreSQL, MySQL, SQL Server, etc.)
- API endpoint check
- File system access
- Message queue connection

### 3. `check_stage.py`

Implement `check_stage_connection(config: dict)` to verify staging area connectivity.

This could be:
- S3 bucket access
- Azure Blob Storage
- Google Cloud Storage
- SFTP server
- Local file system

### 4. `check_target.py`

Implement `check_target_connection(config: dict)` to verify target system connectivity.

This is often similar to Snowflake check but may verify:
- Specific schema/table access
- Write permissions
- Quota/space availability

## Configuration

Add connectivity configuration to your `config.json`:

```json
{
  "snowflake": {
    "user": "...",
    "password": "...",
    "account": "...",
    "warehouse": "...",
    "database": "...",
    "schema": "..."
  },
  "source": {
    "type": "postgres",
    "host": "...",
    "port": 5432,
    "database": "...",
    "user": "...",
    "password": "..."
  },
  "stage": {
    "type": "s3",
    "bucket": "...",
    "region": "...",
    "access_key": "...",
    "secret_key": "..."
  },
  "target": {
    "schema": "...",
    "table": "..."
  }
}
```

## Testing

You can test connectivity checks standalone:

```bash
cd /path/to/pipeline_framework_project_01
python framework_scripts/connectivity_checker.py
```

This will run all connectivity checks and print results.

## Alerting

If any connectivity check fails, the `alerting_func()` will be called to send notifications.

Make sure to configure SMTP settings in your `config.json`:

```json
{
  "smtp_config": {
    "host": "smtp.gmail.com",
    "port": 587,
    "user": "your-email@gmail.com",
    "password": "your-app-password",
    "from_email": "your-email@gmail.com",
    "use_tls": true
  },
  "pipeline_metadata": {
    "notification_emails": ["team@company.com", "oncall@company.com"]
  }
}
```

## Important Notes

1. **Remove placeholder code** - The template scripts contain placeholder warnings. Remove these and implement actual checks.

2. **Never raise exceptions** - Always capture errors in try/except and return them in the dictionary.

3. **Credentials security** - Use environment variables or secret management for credentials, not plain text in config files.

4. **Test thoroughly** - Test each connectivity check independently before deploying.

5. **Fail fast** - If a system is unreachable, it's better to fail the DAG early rather than waste time on doomed processing.
