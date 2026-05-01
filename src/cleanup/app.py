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
    if not ERROR_QUEUE_URL:
        logger.warning("⚠️  ERROR_QUEUE_URL not configured - skipping centralized error reporting")
        return

    try:
        logger.info("Sending error details to SQS for centralized handling...")
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
        logger.info(f"✓ Error details successfully sent to SQS queue")
        logger.info(f"  → Lambda: {lambda_name}")
        logger.info(f"  → Queue URL: {ERROR_QUEUE_URL}")
    except Exception as sqs_error:
        logger.error(f"✗ Failed to send error to SQS queue")
        logger.error(f"  → Lambda: {lambda_name}")
        logger.error(f"  → Error: {str(sqs_error)}")
        logger.exception("Full error details:")


def lambda_handler(event, context):
    logger.info("=" * 80)
    logger.info("CLEANUP FUNCTION STARTED - Removing processed S3 file")
    logger.info("=" * 80)

    try:
        # Extract S3 file information
        logger.info("Step 1: Extracting S3 file information...")
        bucket = event.get('bucket')
        file_key = event.get('file_key')

        if not bucket or not file_key:
            logger.error(f"✗ Missing required file identifiers")
            logger.error(f"  → Bucket: {bucket}")
            logger.error(f"  → File Key: {file_key}")
            return {**event, "status": "ERROR", "message": "Missing S3 coordinates for cleanup."}

        logger.info(f"✓ File information extracted")
        logger.info(f"  → S3 Bucket: {bucket}")
        logger.info(f"  → S3 Key: {file_key}")

        # Delete the processed file from S3
        # Using EAFP (Easier to Ask for Forgiveness than Permission) - just delete without checking first
        # This reduces API calls (1 call instead of 2) and avoids race conditions
        logger.info("Step 2: Deleting processed file from S3...")
        logger.info(f"  → Target: s3://{bucket}/{file_key}")

        try:
            s3_client.delete_object(Bucket=bucket, Key=file_key)

            logger.info(f"✓ File successfully deleted from S3")
            logger.info(f"  → Bucket: {bucket}")
            logger.info(f"  → Key: {file_key}")
            logger.info("=" * 80)
            logger.info("CLEANUP FUNCTION COMPLETED - File removed")
            logger.info("=" * 80)

            return {
                **event,
                "status": "COMPLETED",
                "cleanup_outcome": "DELETED"
            }

        except ClientError as e:
            error_code = e.response['Error']['Code']
            # NoSuchKey (404) is expected if file was already deleted or doesn't exist
            # Treat this as idempotent success - cleanup already accomplished
            if error_code == '404' or error_code == 'NoSuchKey':
                logger.warning(f"✓ File already deleted (NoSuchKey)")
                logger.warning(f"  → Bucket: {bucket}")
                logger.warning(f"  → Key: {file_key}")
                logger.warning(f"  → Idempotency maintained - treating as success")
                logger.info("=" * 80)
                logger.info("CLEANUP FUNCTION COMPLETED - File was already gone")
                logger.info("=" * 80)
                return {**event, "status": "COMPLETED", "cleanup_outcome": "ALREADY_GONE"}
            else:
                logger.error(f"✗ AWS S3 error during file deletion")
                logger.error(f"  → Error Code: {error_code}")
                logger.error(f"  → Error Message: {str(e)}")
                logger.error(f"  → Bucket: {bucket}")
                logger.error(f"  → Key: {file_key}")
                return {**event, "status": "ERROR", "message": str(e)}

    except Exception as e:
        logger.error("=" * 80)
        logger.error("CLEANUP FUNCTION FAILED WITH UNEXPECTED ERROR")
        logger.error("=" * 80)
        logger.error(f"Error Type: {type(e).__name__}")
        logger.error(f"Error Message: {str(e)}")
        logger.exception("Full stack trace:")

        error_traceback = traceback.format_exc()
        send_error_to_sqs('CleanupFunction', str(e), error_traceback, event, context)
        return {**event, "status": "ERROR", "message": f"Unexpected Failure: {str(e)}"}