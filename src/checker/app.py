import os
import json
import boto3
import requests
import logging
import traceback
import time
from datetime import datetime, timezone
from botocore.exceptions import ClientError

# Set up structured logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize SQS client for error reporting
sqs_client = boto3.client('sqs')
ERROR_QUEUE_URL = os.environ.get('ERROR_QUEUE_URL')

# Token caching for performance (5-minute cache)
_cached_token = None
_token_cache_time = None
TOKEN_CACHE_TTL = 300


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


def get_db_token(secret_arn):
    global _cached_token, _token_cache_time

    current_time = time.time()
    # Token caching: Avoid repeated Secrets Manager API calls during repeated Checker invocations
    # This reduces API load and latency since Checker is called every 60 seconds in Step Function
    if _cached_token and _token_cache_time and (current_time - _token_cache_time < TOKEN_CACHE_TTL):
        cache_age = int(current_time - _token_cache_time)
        logger.info(f"✓ Using cached Databricks token (cached {cache_age}s ago, expires in {TOKEN_CACHE_TTL - cache_age}s)")
        return _cached_token

    client = boto3.client('secretsmanager')
    try:
        logger.info(f"Fetching fresh Databricks token from Secrets Manager: {secret_arn}")
        response = client.get_secret_value(SecretId=secret_arn)
        secret_dict = json.loads(response['SecretString'])
        token = secret_dict.get('token')

        if token:
            # Cache token in-memory for 5 minutes to reduce Secrets Manager load
            _cached_token = token
            _token_cache_time = current_time
            logger.info(f"✓ Fresh token obtained and cached (expires in {TOKEN_CACHE_TTL}s)")
        return token
    except Exception as e:
        logger.error(f"✗ Failed to retrieve Databricks token from Secrets Manager")
        logger.error(f"  → Secret ARN: {secret_arn}")
        logger.error(f"  → Error: {str(e)}")
        logger.exception("Full error details:")
        return None

def lambda_handler(event, context):
    logger.info("=" * 80)
    logger.info("CHECKER FUNCTION STARTED - Polling Databricks job status")
    logger.info("=" * 80)

    try:
        # Extract and validate run ID
        logger.info("Step 1: Validating incoming event...")
        run_id = event.get('run_id')
        if not run_id:
            logger.error("✗ Missing 'run_id' in incoming event")
            logger.error("Cannot proceed without a valid run ID from the Launcher function")
            return {**event, "status": "ERROR", "message": "Missing run_id."}

        logger.info(f"✓ Run ID extracted: {run_id}")
        logger.info(f"  → Full event: {json.dumps(event)}")

        # Load environment configuration
        logger.info("Step 2: Loading environment configuration...")
        db_host = os.environ.get('DATABRICKS_HOST')
        secret_arn = os.environ.get('SECRET_ARN')

        if not all([db_host, secret_arn]):
            logger.error("✗ Missing required environment variables (DATABRICKS_HOST or SECRET_ARN)")
            return {**event, "status": "ERROR", "message": "Missing Environment Variables."}

        logger.info(f"✓ Environment configuration loaded")
        logger.info(f"  → Databricks Host: {db_host}")

        # Authenticate with Databricks
        logger.info("Step 3: Authenticating with Databricks...")
        token = get_db_token(secret_arn)
        if not token:
            logger.error("✗ Failed to retrieve Databricks token")
            return {**event, "status": "ERROR", "message": "Authentication Failure."}

        logger.info("✓ Successfully authenticated with Databricks")

        # Poll job status from Databricks API
        # Using Jobs API to query run status - Step Function will retry on transient errors
        logger.info("Step 4: Polling Databricks job status...")
        api_url = f"{db_host.rstrip('/')}/api/2.1/jobs/runs/get"
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

        logger.info(f"  → API Endpoint: {api_url}")
        logger.info(f"  → Run ID: {run_id}")

        try:
            # 10s timeout: reasonable for status check, prevents hanging connections
            # Using params dict instead of f-string for proper URL encoding and safety
            response = requests.get(api_url, headers=headers, params={"run_id": run_id}, timeout=10)

            if response.status_code != 200:
                logger.error(f"✗ Databricks API returned an error")
                logger.error(f"  → HTTP Status Code: {response.status_code}")
                logger.error(f"  → API Response: {response.text}")
                # Strategy: Treat 5xx as transient (RUNNING), 4xx as permanent error (ERROR)
                # Step Function will retry RUNNING status automatically
                status = "RUNNING" if response.status_code >= 500 else "ERROR"
                logger.info(f"  → Returning status: {status} (will retry on 5xx errors)")
                return {**event, "status": status, "api_error": response.text}

            # Databricks Jobs API response structure:
            # state.life_cycle_state: PENDING, RUNNING, TERMINATING, TERMINATED, SKIPPED, INTERNAL_ERROR
            # state.result_state: SUCCESS, FAILED, TIMEDOUT, CANCELED (only when TERMINATED)
            data = response.json()
            current_state = data.get('state', {}).get('life_cycle_state')
            result_state = data.get('state', {}).get('result_state')

            logger.info(f"✓ Job status retrieved successfully")
            logger.info(f"  → Run ID: {run_id}")
            logger.info(f"  → Lifecycle State: {current_state}")
            logger.info(f"  → Result State: {result_state}")

            # Evaluate job completion status
            # Different states require different actions: continue waiting, success, or failure
            logger.info("Step 5: Evaluating job status...")

            if current_state == 'TERMINATED':
                # Job has finished execution, check result_state to determine success/failure
                if result_state == 'SUCCESS':
                    logger.info(f"✓ Job completed successfully!")
                    logger.info(f"  → Run ID: {run_id}")
                    logger.info(f"  → Status: SUCCESS")
                    logger.info("=" * 80)
                    logger.info("CHECKER FUNCTION COMPLETED - Job succeeded")
                    logger.info("=" * 80)
                    return {**event, "status": "SUCCESS", "raw_state": result_state}
                else:
                    # result_state can be FAILED, TIMEDOUT, or CANCELED
                    logger.error(f"✗ Job failed or was cancelled")
                    logger.error(f"  → Run ID: {run_id}")
                    logger.error(f"  → Result State: {result_state}")
                    logger.error(f"  → Status: FAILED")
                    logger.info("=" * 80)
                    logger.info("CHECKER FUNCTION COMPLETED - Job failed")
                    logger.info("=" * 80)
                    return {**event, "status": "FAILED", "raw_state": result_state}

            elif current_state in ['PENDING', 'RUNNING', 'TERMINATING']:
                # Job is still in progress, Step Function will wait and check again
                logger.info(f"⏳ Job is still running...")
                logger.info(f"  → Run ID: {run_id}")
                logger.info(f"  → Current State: {current_state}")
                logger.info(f"  → Will check again in next poll cycle")
                return {**event, "status": "RUNNING", "raw_state": current_state}

            else:
                # SKIPPED or INTERNAL_ERROR - unexpected state, treat as failure
                logger.error(f"✗ Job entered an unexpected state")
                logger.error(f"  → Run ID: {run_id}")
                logger.error(f"  → Unexpected State: {current_state}")
                logger.error(f"  → Status: FAILED")
                return {**event, "status": "FAILED", "raw_state": current_state}

        except requests.exceptions.RequestException as e:
            logger.error(f"✗ Network error while contacting Databricks API")
            logger.error(f"  → Run ID: {run_id}")
            logger.error(f"  → Error: {str(e)}")
            logger.info(f"✓ Returning RUNNING status to allow Step Function to retry")
            return {**event, "status": "RUNNING", "error": str(e)}

    except Exception as e:
        logger.error("=" * 80)
        logger.error("CHECKER FUNCTION FAILED WITH UNEXPECTED ERROR")
        logger.error("=" * 80)
        logger.error(f"Error Type: {type(e).__name__}")
        logger.error(f"Error Message: {str(e)}")
        logger.exception("Full stack trace:")

        error_traceback = traceback.format_exc()
        send_error_to_sqs('CheckerFunction', str(e), error_traceback, event, context)
        return {**event, "status": "ERROR", "message": f"Unexpected Failure: {str(e)}"}