import os
import json
import boto3
import logging
from datetime import datetime
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
    """Format error information into a rich HTML email."""
    lambda_name = error_data.get('lambda_name', 'Unknown')
    error_message = error_data.get('error_message', 'No error message provided')
    error_traceback = error_data.get('error_traceback', 'No traceback available')
    timestamp = error_data.get('timestamp', datetime.utcnow().isoformat())
    log_group = error_data.get('log_group', 'N/A')
    log_stream = error_data.get('log_stream', 'N/A')
    event_context = error_data.get('event_context', {})
    error_source = error_data.get('error_source', 'Lambda')
    
    # Create CloudWatch Logs deep link
    log_link = f"https://console.aws.amazon.com/cloudwatch/home?#logsV2:logs-insights$3FqueryDetail$3D~(end~0~start~-3600~timeType~'RELATIVE~unit~'seconds~editorString~'fields*20*40timestamp*2c*20*40message*0a*7c*20filter*20*40logStream*3d*22{log_stream}*22*0a*7c*20stats*20count()*20by*20*40message~source~'{log_group})"
    
    # Escape HTML entities for safety
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
    """
    Main error handler function.
    Processes messages from SQS and publishes formatted emails to SNS.
    Can be triggered by:
    1. SQS queue polling (via EventSourceMapping)
    2. Direct invocation from Step Function for failure reporting
    """
    logger.info(f"Error handler invoked with event: {json.dumps(event)}")
    
    try:
        # Check if this is a direct invocation from Step Function or SQS
        if 'Records' in event:
            # SQS batch event
            return process_sqs_messages(event)
        else:
            # Direct invocation (e.g., from Step Function failure state)
            return process_direct_error(event)
    
    except Exception as e:
        logger.exception("Error handler failed to process error")
        return {
            "statusCode": 500,
            "status": "ERROR",
            "message": f"Error handler failed: {str(e)}"
        }


def process_sqs_messages(event):
    """Process SQS batch messages."""
    logger.info(f"Processing {len(event['Records'])} SQS message(s)")
    
    failed_messages = []
    
    for record in event['Records']:
        try:
            message_body = json.loads(record['body'])
            receipt_handle = record['receiptHandle']
            
            # Publish to SNS
            publish_error_notification(message_body)
            
            # Delete from SQS
            sqs_client.delete_message(
                QueueUrl=ERROR_QUEUE_URL,
                ReceiptHandle=receipt_handle
            )
            logger.info(f"Successfully processed and deleted message: {receipt_handle}")
            
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON in SQS message: {record['body']}")
            failed_messages.append(record['receiptHandle'])
        except Exception as e:
            logger.exception(f"Failed to process SQS record: {str(e)}")
            failed_messages.append(record['receiptHandle'])
    
    return {
        "statusCode": 200,
        "status": "PROCESSED",
        "processed": len(event['Records']) - len(failed_messages),
        "failed": len(failed_messages)
    }


def process_direct_error(event):
    """Process direct invocation from Step Function or Lambda exception."""
    logger.info("Processing direct error invocation")
    
    try:
        # Convert Step Function error to error data format
        if 'error_source' in event and event['error_source'] == 'StepFunction':
            error_data = {
                'lambda_name': 'StepFunction',
                'error_source': 'Step Function State Machine',
                'error_message': event.get('error_message', 'Unknown error'),
                'error_traceback': 'See CloudWatch Logs for details',
                'timestamp': event.get('timestamp', datetime.utcnow().isoformat()),
                'log_group': context.log_group_name if hasattr(context, 'log_group_name') else 'N/A',
                'log_stream': context.log_stream_name if hasattr(context, 'log_stream_name') else 'N/A',
                'event_context': event.get('event_context', {})
            }
        else:
            # Generic error from Lambda
            error_data = event
        
        # Publish to SNS
        publish_error_notification(error_data)
        
        return {
            "statusCode": 200,
            "status": "PUBLISHED",
            "message": "Error notification published to SNS"
        }
    
    except Exception as e:
        logger.exception(f"Failed to process direct error: {str(e)}")
        return {
            "statusCode": 500,
            "status": "ERROR",
            "message": f"Failed to publish error: {str(e)}"
        }


def publish_error_notification(error_data):
    """Publish formatted error to SNS topic."""
    logger.info(f"Publishing error notification for Lambda: {error_data.get('lambda_name', 'Unknown')}")
    
    # Format HTML email
    html_body = format_html_email(error_data)
    
    # Create text version
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
    
    # Publish to SNS
    response = sns_client.publish(
        TopicArn=ERROR_TOPIC_ARN,
        Subject=f"[AIRLINE PIPELINE ERROR] {error_data.get('lambda_name', 'Unknown')} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        Message=text_body,
        MessageStructure='json',
        MessageAttributes={
            'Lambda': {'DataType': 'String', 'StringValue': error_data.get('lambda_name', 'Unknown')},
            'Timestamp': {'DataType': 'String', 'StringValue': error_data.get('timestamp', '')},
            'Source': {'DataType': 'String', 'StringValue': error_data.get('error_source', 'Lambda')}
        }
    )
    
    # Publish HTML version
    sns_client.publish(
        TopicArn=ERROR_TOPIC_ARN,
        Subject=f"[AIRLINE PIPELINE ERROR] {error_data.get('lambda_name', 'Unknown')} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        Message=html_body,
        MessageStructure='json',
        MessageAttributes={
            'Lambda': {'DataType': 'String', 'StringValue': error_data.get('lambda_name', 'Unknown')},
            'Timestamp': {'DataType': 'String', 'StringValue': error_data.get('timestamp', '')},
            'ContentType': {'DataType': 'String', 'StringValue': 'HTML'}
        }
    )
    
    logger.info(f"Error notification published. SNS MessageId: {response['MessageId']}")
    return response
