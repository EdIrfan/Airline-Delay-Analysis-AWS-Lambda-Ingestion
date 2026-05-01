import os
import json
import boto3
import logging
from datetime import datetime, timezone
import traceback
from html import escape

# Set up structured logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize clients
sqs_client = boto3.client('sqs')
sns_client = boto3.client('sns')
logs_client = boto3.client('logs')

ERROR_QUEUE_URL = os.environ.get('ERROR_QUEUE_URL')
ERROR_TOPIC_ARN = os.environ.get('ERROR_TOPIC_ARN')


def format_html_email(error_data):
    lambda_name = error_data.get('lambda_name', 'Unknown')
    error_message = error_data.get('error_message', 'No error message provided')
    error_traceback = error_data.get('error_traceback', 'No traceback available')
    timestamp = error_data.get('timestamp', datetime.now(timezone.utc).isoformat())
    log_group = error_data.get('log_group', 'N/A')
    log_stream = error_data.get('log_stream', 'N/A')
    event_context = error_data.get('event_context', {})
    error_source = error_data.get('error_source', 'Lambda')

    logger.info("Formatting HTML email...")
    logger.info(f"  → Lambda: {lambda_name}")
    logger.info(f"  → Error Source: {error_source}")
    logger.info(f"  → Log Group: {log_group}")
    logger.info(f"  → Log Stream: {log_stream}")

    # Generate deep-link to CloudWatch Logs Insights for the specific log stream
    # Allows ops teams to quickly investigate without manual navigation
    log_link = f"https://console.aws.amazon.com/cloudwatch/home?#logsV2:logs-insights$3FqueryDetail$3D~(end~0~start~-3600~timeType~'RELATIVE~unit~'seconds~editorString~'fields*20*40timestamp*2c*20*40message*0a*7c*20filter*20*40logStream*3d*22{log_stream}*22*0a*7c*20stats*20count()*20by*20*40message~source~'{log_group})"

    # HTML escape all user-controlled content to prevent injection attacks
    # Error message and traceback might contain special characters that need escaping in HTML context
    escaped_error_msg = escape(error_message)
    escaped_traceback = escape(error_traceback)
    escaped_log_group = escape(log_group)
    escaped_log_stream = escape(log_stream)
    
    html_body = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; background-color: #f5f5f5; margin: 0; padding: 20px; }}
            .container {{ background-color: #ffffff; border-left: 5px solid #d32f2f; border-radius: 4px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); padding: 20px; max-width: 800px; margin: 0 auto; }}
            .header {{ background-color: #d32f2f; color: #ffffff; padding: 15px; border-radius: 4px 4px 0 0; margin: -20px -20px 20px -20px; }}
            h1 {{ margin: 0; font-size: 24px; }}
            .timestamp {{ color: #999; font-size: 12px; margin-top: 5px; }}
            .section {{ margin: 20px 0; }}
            .section-title {{ font-weight: bold; color: #d32f2f; margin-bottom: 10px; font-size: 14px; text-transform: uppercase; }}
            .error-message {{ background-color: #fff3cd; border-left: 4px solid #ffc107; padding: 12px; border-radius: 4px; margin: 10px 0; font-family: 'Courier New', monospace; font-size: 13px; color: #856404; overflow-x: auto; white-space: pre-wrap; word-wrap: break-word; }}
            .traceback {{ background-color: #f8d7da; border-left: 4px solid #dc3545; padding: 12px; border-radius: 4px; margin: 10px 0; font-family: 'Courier New', monospace; font-size: 12px; color: #721c24; overflow-x: auto; white-space: pre-wrap; word-wrap: break-word; max-height: 400px; overflow-y: auto; }}
            .log-link {{ background-color: #e3f2fd; border-left: 4px solid #2196f3; padding: 12px; border-radius: 4px; margin: 10px 0; }}
            .log-link a {{ color: #1976d2; text-decoration: none; font-weight: bold; }}
            .log-link a:hover {{ text-decoration: underline; }}
            .metadata {{ background-color: #f5f5f5; padding: 10px; border-radius: 4px; margin: 10px 0; }}
            .metadata-row {{ margin: 5px 0; font-size: 13px; }}
            .label {{ font-weight: bold; color: #333; }}
            .footer {{ margin-top: 30px; padding-top: 20px; border-top: 1px solid #eee; color: #999; font-size: 11px; }}
            code {{ background-color: #f0f0f0; padding: 2px 6px; border-radius: 3px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>⚠️ Pipeline Error Notification</h1>
                <div class="timestamp">Generated: {timestamp}</div>
            </div>
            
            <div class="section">
                <div class="section-title">Error Summary</div>
                <div class="metadata">
                    <div class="metadata-row"><span class="label">Source:</span> {error_source}</div>
                    <div class="metadata-row"><span class="label">Lambda Function:</span> <code>{escape(lambda_name)}</code></div>
                    <div class="metadata-row"><span class="label">Timestamp:</span> {timestamp}</div>
                </div>
            </div>

            <div class="section">
                <div class="section-title">Error Message</div>
                <div class="error-message">{escaped_error_msg}</div>
            </div>

            <div class="section">
                <div class="section-title">Python Traceback</div>
                <div class="traceback">{escaped_traceback}</div>
            </div>

            <div class="section">
                <div class="section-title">CloudWatch Logs</div>
                <div class="log-link">
                    <strong>Log Group:</strong> {escaped_log_group}<br>
                    <strong>Log Stream:</strong> {escaped_log_stream}<br><br>
                    <a href="{log_link}" target="_blank">📊 View in CloudWatch Logs Insights</a>
                </div>
            </div>

            <div class="section">
                <div class="section-title">Event Context</div>
                <div class="metadata">
                    <pre style="background-color: #f0f0f0; padding: 10px; border-radius: 4px; overflow-x: auto; margin: 0;">{escape(json.dumps(event_context, indent=2))}</pre>
                </div>
            </div>

            <div class="footer">
                <p>This email was automatically generated by the Airline Pipeline Error Notification System. Please do not reply to this email.</p>
                <p>For support, check your AWS CloudWatch Logs or contact your DevOps team.</p>
            </div>
        </div>
    </body>
    </html>
    """
    return html_body


def lambda_handler(event, context):
    logger.info("=" * 80)
    logger.info("ERROR HANDLER FUNCTION STARTED - Processing pipeline error")
    logger.info("=" * 80)

    try:
        logger.info(f"Event received: {json.dumps(event)}")

        # Two invocation modes:
        # 1. SQS polling: EventSourceMapping triggers this function with batch of error messages
        # 2. Direct Step Function: Failure state in Step Function calls this directly for immediate notification
        if 'Records' in event:
            logger.info("Message type: SQS batch event (from EventSourceMapping)")
            return process_sqs_messages(event)
        else:
            logger.info("Message type: Direct invocation from Step Function failure state")
            return process_direct_error(event, context)

    except Exception as e:
        logger.error("=" * 80)
        logger.error("ERROR HANDLER FUNCTION FAILED")
        logger.error("=" * 80)
        logger.error(f"Error Type: {type(e).__name__}")
        logger.error(f"Error Message: {str(e)}")
        logger.exception("Full stack trace:")

        return {
            "statusCode": 500,
            "status": "ERROR",
            "message": f"Error handler failed: {str(e)}"
        }


def process_sqs_messages(event):
    logger.info("=" * 80)
    logger.info("PROCESSING SQS BATCH MESSAGES")
    logger.info("=" * 80)

    total_messages = len(event['Records'])
    logger.info(f"Processing {total_messages} error message(s) from SQS queue")

    queue_url = os.environ.get('ERROR_QUEUE_URL')
    logger.info(f"SQS Queue URL: {queue_url}")

    failed_messages = []

    for idx, record in enumerate(event['Records'], 1):
        logger.info(f"Processing message {idx}/{total_messages}...")
        try:
            message_body = json.loads(record['body'])
            receipt_handle = record['receiptHandle']

            logger.info(f"  → Lambda Function: {message_body.get('lambda_name', 'Unknown')}")
            logger.info(f"  → Error Source: {message_body.get('error_source', 'Unknown')}")
            logger.info(f"  → Timestamp: {message_body.get('timestamp', 'N/A')}")

            # Publish to SNS first, then delete from SQS
            # If SNS publish fails, the message stays in SQS and will be retried by EventSourceMapping
            publish_error_notification(message_body)
            logger.info(f"  ✓ Error notification published to SNS")

            # Delete only after successful SNS publish to ensure at-least-once delivery
            sqs_client.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle
            )
            logger.info(f"  ✓ Message deleted from SQS queue")

        except json.JSONDecodeError:
            logger.error(f"  ✗ Failed to parse JSON in message {idx}")
            logger.error(f"  → Raw message body: {record['body'][:200]}...")
            # Don't delete malformed messages - they'll stay in SQS and trigger redrive policy
            failed_messages.append(record['receiptHandle'])
        except Exception as e:
            logger.error(f"  ✗ Error processing message {idx}: {str(e)}")
            logger.exception(f"  → Full error details:")
            # Keep failed messages in queue for retry
            failed_messages.append(record['receiptHandle'])

    logger.info("=" * 80)
    logger.info("SQS BATCH PROCESSING COMPLETE")
    logger.info("=" * 80)
    logger.info(f"Summary:")
    logger.info(f"  → Total Messages: {total_messages}")
    logger.info(f"  → Successfully Processed: {total_messages - len(failed_messages)}")
    logger.info(f"  → Failed: {len(failed_messages)}")

    return {
        "statusCode": 200,
        "status": "PROCESSED",
        "processed": total_messages - len(failed_messages),
        "failed": len(failed_messages)
    }


def process_direct_error(event, context):
    logger.info("=" * 80)
    logger.info("PROCESSING DIRECT ERROR INVOCATION")
    logger.info("=" * 80)

    try:
        if 'error_source' in event and event['error_source'] == 'StepFunction':
            logger.info("Error source: Step Function State Machine")
            error_data = {
                'lambda_name': 'StepFunction',
                'error_source': 'Step Function State Machine',
                'error_message': event.get('error_message', 'Unknown error'),
                'error_traceback': 'See CloudWatch Logs for details',
                'timestamp': event.get('timestamp', datetime.now(timezone.utc).isoformat()),
                'log_group': context.log_group_name if hasattr(context, 'log_group_name') else 'N/A',
                'log_stream': context.log_stream_name if hasattr(context, 'log_stream_name') else 'N/A',
                'event_context': event.get('event_context', {})
            }
        else:
            logger.info("Error source: Direct Lambda invocation")
            error_data = event

        logger.info(f"Error Details:")
        logger.info(f"  → Lambda/Source: {error_data.get('lambda_name', 'Unknown')}")
        logger.info(f"  → Message: {error_data.get('error_message', 'N/A')[:100]}...")
        logger.info(f"  → Timestamp: {error_data.get('timestamp', 'N/A')}")

        logger.info("Publishing error notification to SNS...")
        publish_error_notification(error_data)
        logger.info("✓ Error notification successfully published to SNS")

        logger.info("=" * 80)
        logger.info("DIRECT ERROR PROCESSING COMPLETE")
        logger.info("=" * 80)

        return {
            "statusCode": 200,
            "status": "PUBLISHED",
            "message": "Error notification published to SNS"
        }

    except Exception as e:
        logger.error("=" * 80)
        logger.error("DIRECT ERROR PROCESSING FAILED")
        logger.error("=" * 80)
        logger.error(f"Error Type: {type(e).__name__}")
        logger.error(f"Error Message: {str(e)}")
        logger.exception("Full stack trace:")

        return {
            "statusCode": 500,
            "status": "ERROR",
            "message": f"Failed to publish error: {str(e)}"
        }


def publish_error_notification(error_data):
    logger.info("Preparing error notification for email...")
    logger.info(f"  → Lambda Function: {error_data.get('lambda_name', 'Unknown')}")
    logger.info(f"  → Error Source: {error_data.get('error_source', 'Unknown')}")

    topic_arn = os.environ.get('ERROR_TOPIC_ARN')
    logger.info(f"  → SNS Topic ARN: {topic_arn}")

    html_body = format_html_email(error_data)
    logger.info("  ✓ HTML email body formatted")

    text_body = f"""
PIPELINE ERROR NOTIFICATION

Lambda Function: {error_data.get('lambda_name', 'Unknown')}
Error Source: {error_data.get('error_source', 'Lambda')}
Timestamp: {error_data.get('timestamp', 'N/A')}

ERROR MESSAGE:
{error_data.get('error_message', 'No error message provided')}

PYTHON TRACEBACK:
{error_data.get('error_traceback', 'No traceback available')}

CloudWatch Logs:
Log Group: {error_data.get('log_group', 'N/A')}
Log Stream: {error_data.get('log_stream', 'N/A')}

EVENT CONTEXT:
{json.dumps(error_data.get('event_context', {}), indent=2)}
    """

    logger.info("Sending notification via SNS...")
    response = sns_client.publish(
        TopicArn=topic_arn,
        Subject=f"[AIRLINE PIPELINE ERROR] {error_data.get('lambda_name', 'Unknown')} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        Message=text_body
    )

    logger.info(f"✓ Error notification published to SNS successfully")
    logger.info(f"  → SNS MessageId: {response['MessageId']}")
    return response
