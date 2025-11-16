"""
Alerting Module
===============
Handles failure notifications for pipeline tasks.

This module provides functionality to send alerts when tasks fail.
"""

import logging
from typing import Optional
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

logger = logging.getLogger(__name__)


def alerting_func(config: dict, error_message_for_mail: str) -> None:
    """
    Send alert notification when a task fails.

    This function is called upon task failure to notify stakeholders.
    Uses try/except to ensure alerting failures don't crash the pipeline.

    Args:
        config: Pipeline configuration dictionary
        error_message_for_mail: Error message to include in the alert

    Returns:
        None
    """
    try:
        logger.info("Sending failure alert...")

        # Extract notification configuration
        pipeline_metadata = config.get('pipeline_metadata', {})
        pipeline_name = pipeline_metadata.get('pipeline_name', 'Unknown Pipeline')
        notification_emails = pipeline_metadata.get('notification_emails', [])

        # Get SMTP configuration (user should provide this in config)
        smtp_config = config.get('smtp_config', {})

        if not notification_emails:
            logger.warning("No notification emails configured, skipping alert")
            return

        if not smtp_config:
            logger.warning("No SMTP configuration found, skipping email alert")
            logger.error(f"Alert message (not sent): {error_message_for_mail}")
            return

        # ============================================================
        # USER: Customize the email content as needed
        # ============================================================

        subject = f"[ALERT] Pipeline Failure: {pipeline_name}"

        body = f"""
Pipeline Failure Alert
======================

Pipeline: {pipeline_name}
Status: FAILED

Error Message:
--------------
{error_message_for_mail}

Configuration:
--------------
Owner: {pipeline_metadata.get('owner_name', 'N/A')}
Environment: {pipeline_metadata.get('environment', 'N/A')}

Please investigate and take necessary action.

This is an automated alert from the data pipeline framework.
"""

        # Send email
        _send_email(
            smtp_config=smtp_config,
            recipients=notification_emails,
            subject=subject,
            body=body
        )

        logger.info(f"Alert sent successfully to: {', '.join(notification_emails)}")

    except Exception as e:
        # Log error but don't raise - alerting should never crash the pipeline
        logger.error(f"Failed to send alert: {str(e)}")
        logger.error(f"Original error message that failed to send: {error_message_for_mail}")


def _send_email(smtp_config: dict, recipients: list, subject: str, body: str) -> None:
    """
    Send email using SMTP.

    Args:
        smtp_config: SMTP configuration dictionary
        recipients: List of recipient email addresses
        subject: Email subject
        body: Email body

    Raises:
        Exception: If email sending fails
    """
    # Extract SMTP configuration
    smtp_host = smtp_config.get('host', 'localhost')
    smtp_port = smtp_config.get('port', 587)
    smtp_user = smtp_config.get('user')
    smtp_password = smtp_config.get('password')
    smtp_from = smtp_config.get('from_email', smtp_user)
    use_tls = smtp_config.get('use_tls', True)

    # Create message
    msg = MIMEMultipart()
    msg['From'] = smtp_from
    msg['To'] = ', '.join(recipients)
    msg['Subject'] = subject

    # Attach body
    msg.attach(MIMEText(body, 'plain'))

    # Send email
    if use_tls:
        server = smtplib.SMTP(smtp_host, smtp_port)
        server.starttls()
    else:
        server = smtplib.SMTP_SSL(smtp_host, smtp_port)

    if smtp_user and smtp_password:
        server.login(smtp_user, smtp_password)

    server.send_message(msg)
    server.quit()


def get_alerting_callback(config: dict):
    """
    Get a callback function for Airflow task failure.

    This returns a function that can be passed to Airflow's on_failure_callback.

    Args:
        config: Pipeline configuration dictionary

    Returns:
        Callable: Function to use as on_failure_callback

    Example:
        task = PythonOperator(
            task_id='my_task',
            python_callable=my_function,
            on_failure_callback=get_alerting_callback(config)
        )
    """
    def failure_callback(context):
        """
        Callback function for Airflow task failure.

        Args:
            context: Airflow context dictionary
        """
        try:
            # Extract error information from context
            task_instance = context.get('task_instance')
            exception = context.get('exception')

            error_message = f"""
Task Failed: {task_instance.task_id}
DAG: {task_instance.dag_id}
Execution Date: {context.get('execution_date')}
Try Number: {task_instance.try_number}

Exception:
{str(exception)}

Log URL: {task_instance.log_url}
"""

            # Call alerting function
            alerting_func(config, error_message)

        except Exception as e:
            logger.error(f"Error in failure callback: {str(e)}")

    return failure_callback
