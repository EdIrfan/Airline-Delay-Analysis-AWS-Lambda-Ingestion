import json
import boto3
import logging
import traceback
from datetime import datetime, timezone
from botocore.exceptions import ClientError

# Set up structured logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize S3 client outside the handler for TCP connection reuse
s3_client = boto3.client('s3')

# Initialize SQS client for error reporting
sqs_client = boto3.client('sqs')

import os
ERROR_QUEUE_URL = os.environ.get('ERROR_QUEUE_URL')


def send_error_to_sqs(lambda_name, error_message, error_traceback, event, context):
    """
    Send error details to SQS for centralized error handling.
    """
    if not ERROR_QUEUE_URL:
        logger.warning("ERROR_QUEUE_URL not configured. Skipping SQS error reporting.")
        return
    
    try:
        error_data = {
            'lambda_name': lambda_name,
            'error_source': 'Lambda',
            'error_message': str(error_message),
            'error_traceback': error_traceback,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'log_group': context.log_group_name,
            'log_stream': context.log_stream_name,
            'event_context': event if isinstance(event, dict) else str(event)
        }
        
        sqs_client.send_message(
            QueueUrl=ERROR_QUEUE_URL,
            MessageBody=json.dumps(error_data)
        )
        logger.info(f"Error details sent to SQS queue: {ERROR_QUEUE_URL}")
    except Exception as sqs_error:
        logger.exception(f"Failed to send error to SQS: {str(sqs_error)}")


def lambda_handler(event, context):
    logger.info(f"Cleanup execution started with event: {json.dumps(event)}")
    try:
        # 1. Extract file info passed through the Step Function state
        bucket = event.get('bucket')
        file_key = event.get('file_key')
        
        if not bucket or not file_key:
            logger.error(f"Missing file identifiers. Bucket: {bucket}, Key: {file_key}")
            return {**event, "status": "ERROR", "message": "Missing S3 coordinates for cleanup."}

        logger.info(f"Attempting to delete processed file: s3://{bucket}/{file_key}")

        # 2. Execute Deletion
        try:
            # We don't check if it exists first (LBYL) to avoid an extra API call.
            # We just try to delete it (EAFP).
            s3_client.delete_object(Bucket=bucket, Key=file_key)
            
            logger.info(f"Successfully deleted s3://{bucket}/{file_key}")
            return {
                **event,
                "status": "COMPLETED",
                "cleanup_outcome": "DELETED"
            }

        except ClientError as e:
            # Check for 404/NoSuchKey to maintain idempotency
            error_code = e.response['Error']['Code']
            if error_code == '404' or error_code == 'NoSuchKey':
                logger.warning(f"File already missing: s3://{bucket}/{file_key}. Idempotency maintained.")
                return {**event, "status": "COMPLETED", "cleanup_outcome": "ALREADY_GONE"}
            else:
                logger.exception("AWS ClientError during S3 deletion.")
                return {**event, "status": "ERROR", "message": str(e)}

    except Exception as e:
        logger.exception("Unexpected system failure in Cleanup Lambda.")
        error_traceback = traceback.format_exc()
        send_error_to_sqs('CleanupFunction', str(e), error_traceback, event, context)
        return {**event, "status": "ERROR", "message": f"Unexpected Failure: {str(e)}"}