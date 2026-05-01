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
    # Token caching: Avoid repeated Secrets Manager calls within same Lambda execution
    # This reduces API calls and latency since Secrets Manager has rate limits
    if _cached_token and _token_cache_time and (current_time - _token_cache_time < TOKEN_CACHE_TTL):
        cache_age = int(current_time - _token_cache_time)
        logger.info(f"✓ Using cached Databricks token (cached {cache_age}s ago, expires in {TOKEN_CACHE_TTL - cache_age}s)")
        return _cached_token

    client = boto3.client('secretsmanager')
    try:
        logger.info(f"Fetching fresh Databricks token from Secrets Manager: {secret_arn}")
        response = client.get_secret_value(SecretId=secret_arn)
        secret_dict = json.loads(response['SecretString'])

        # Extract token from Secrets Manager secret format (expected structure: {"token": "pat_xxx"})
        token = secret_dict.get('token')
        if not token:
            logger.error("✗ Secret retrieved but 'token' field is missing in the secret JSON")
            return None

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
    logger.info("LAUNCHER FUNCTION STARTED - Processing S3 file upload event")
    logger.info("=" * 80)

    try:
        # Extract S3 file information from EventBridge event
        # EventBridge sends S3 events with structure: event['detail']['bucket']['name'] and event['detail']['object']['key']
        try:
            logger.info("Step 1: Validating S3 event structure...")
            logger.info(f"Raw event received: {json.dumps(event)}")

            bucket = event['detail']['bucket']['name']
            file_key = event['detail']['object']['key']
            logger.info(f"✓ S3 Event validated successfully")
            logger.info(f"  → Bucket: {bucket}")
            logger.info(f"  → File Key: {file_key}")
        except (KeyError, IndexError, TypeError) as e:
            # EventBridge S3 events must have this structure - if not, data pipeline is misconfigured
            logger.error(f"✗ Failed to extract S3 information from event: {str(e)}")
            logger.error("Event structure does not match expected EventBridge S3 format")
            return {
                "statusCode": 400,
                "status": "ERROR",
                "message": "Malformed S3 Event structure."
            }

        # Load and validate environment configuration
        # These are set by CloudFormation during deployment and passed to Lambda via environment variables
        logger.info("Step 2: Loading environment configuration...")
        db_host = os.environ.get('DATABRICKS_HOST')
        secret_arn = os.environ.get('SECRET_ARN')
        job_id = os.environ.get('DATABRICKS_JOB_ID')
        env_type = os.environ.get('ENV_TYPE')

        if not all([db_host, secret_arn, job_id, env_type]):
            missing_vars = [v for v in [('DATABRICKS_HOST', db_host), ('SECRET_ARN', secret_arn),
                                       ('DATABRICKS_JOB_ID', job_id), ('ENV_TYPE', env_type)] if not v[1]]
            logger.error(f"✗ Missing required environment variables: {[m[0] for m in missing_vars]}")
            return {"statusCode": 500, "status": "ERROR", "message": "Missing Environment Variables."}

        logger.info(f"✓ Environment configuration loaded for environment: {env_type}")

        # Validate Databricks host configuration
        # Basic validation: must be HTTPS/HTTP and point to a Databricks workspace
        # This prevents SSRF attacks by ensuring we're connecting to the right service
        logger.info("Step 3: Validating Databricks host configuration...")
        if not db_host.startswith(('https://', 'http://')) or '.databricks.' not in db_host:
            logger.error(f"✗ Invalid DATABRICKS_HOST format: {db_host}")
            logger.error("Host must be a valid HTTPS/HTTP URL pointing to a Databricks workspace")
            return {"statusCode": 500, "status": "ERROR", "message": "Invalid Databricks host configuration."}
        logger.info(f"✓ Databricks host validation passed: {db_host}")

        # Validate and convert Job ID to integer
        # Databricks API requires job_id to be an integer, not a string
        # Validation here prevents ValueError crashes when calling int() on invalid input
        logger.info("Step 4: Validating Databricks Job ID...")
        try:
            job_id_int = int(job_id)
            logger.info(f"✓ Job ID is valid: {job_id_int}")
        except (ValueError, TypeError):
            logger.error(f"✗ Job ID must be numeric, received: {job_id}")
            return {"statusCode": 500, "status": "ERROR", "message": "Invalid Job ID configuration. Job ID must be numeric."}

        # Authenticate with Databricks
        logger.info("Step 5: Authenticating with Databricks...")
        token = get_db_token(secret_arn)
        if not token:
            logger.error("✗ Failed to retrieve Databricks token from Secrets Manager")
            return {
                "statusCode": 500,
                "status": "ERROR",
                "message": "Authentication Failure: Could not retrieve token."
            }
        logger.info("✓ Successfully authenticated with Databricks")

        # Setup headers for Databricks API calls
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

        # Get pipeline ID from environment
        logger.info("Step 6: Fetching current DLT pipeline configuration...")
        pipeline_id = os.environ.get('DATABRICKS_PIPELINE_ID')
        if not pipeline_id:
            logger.error("✗ Missing DATABRICKS_PIPELINE_ID environment variable")
            return {"statusCode": 500, "status": "ERROR", "message": "Missing Pipeline ID configuration."}

        pipeline_url = f"{db_host.rstrip('/')}/api/2.0/pipelines/{pipeline_id}"

        try:
            logger.info(f"Retrieving pipeline configuration...")
            logger.info(f"  → Pipeline ID: {pipeline_id}")

            get_response = requests.get(pipeline_url, headers=headers, timeout=15)

            if get_response.status_code != 200:
                logger.error(f"✗ Failed to fetch pipeline configuration")
                logger.error(f"  → HTTP Status Code: {get_response.status_code}")
                logger.error(f"  → Response: {get_response.text}")
                return {
                    "statusCode": get_response.status_code,
                    "status": "ERROR",
                    "message": f"Failed to fetch pipeline configuration: {get_response.text}"
                }

            # Get the complete pipeline config (preserves all UC settings)
            current_pipeline = get_response.json()
            logger.info(f"✓ Pipeline configuration retrieved successfully")

            # Merge our parameter updates into the existing config
            logger.info("Step 7: Updating DLT pipeline configuration...")
            if 'configuration' not in current_pipeline:
                current_pipeline['configuration'] = {}

            current_pipeline['configuration']['pipeline.env'] = env_type
            current_pipeline['configuration']['pipeline.landing_path'] = f"s3://{bucket}/"

            logger.info(f"Sending updated pipeline configuration...")
            logger.info(f"  → Pipeline ID: {pipeline_id}")
            logger.info(f"  → Environment: {env_type}")
            logger.info(f"  → Landing Path: s3://{bucket}/")

            put_response = requests.put(pipeline_url, headers=headers, json=current_pipeline, timeout=15)

            if put_response.status_code != 200:
                logger.error(f"✗ Failed to update pipeline configuration")
                logger.error(f"  → HTTP Status Code: {put_response.status_code}")
                logger.error(f"  → Response: {put_response.text}")
                return {
                    "statusCode": put_response.status_code,
                    "status": "ERROR",
                    "message": f"Pipeline configuration update failed: {put_response.text}"
                }

            logger.info(f"✓ Pipeline configuration updated successfully")
        except requests.exceptions.RequestException as e:
            logger.error(f"✗ Network error while updating pipeline configuration")
            logger.error(f"  → Error: {str(e)}")
            return {"statusCode": 502, "status": "ERROR", "message": f"Pipeline update failed: {str(e)}"}

        # Prepare and trigger Databricks Job
        # Using Jobs API (not Pipelines API) for better control and run-level monitoring
        logger.info("Step 8: Triggering Databricks Job...")
        api_url = f"{db_host.rstrip('/')}/api/2.1/jobs/run-now"

        # job_parameters are passed to the Databricks job and available in job context
        # This allows dynamic parameter injection without modifying the job definition
        # Using modern job_parameters field (not legacy pipeline_params) per latest Databricks API
        payload = {
            "job_id": job_id_int,
            "job_parameters": {
                "pipeline.env": env_type,
                "pipeline.landing_path": f"s3://{bucket}/"
            }
        }
        logger.info(f"  → Job ID: {job_id_int}")
        logger.info(f"  → Environment: {env_type}")
        logger.info(f"  → S3 Landing Path: s3://{bucket}/")

        try:
            # 15s timeout: sufficient for Databricks API response, prevents hanging connections
            response = requests.post(api_url, headers=headers, json=payload, timeout=15)

            if response.status_code != 200:
                # Databricks API returns error details in response.text for debugging
                logger.error(f"✗ Databricks API rejected the request")
                logger.error(f"  → HTTP Status Code: {response.status_code}")
                logger.error(f"  → API Response: {response.text}")
                return {
                    "statusCode": response.status_code,
                    "status": "ERROR",
                    "message": f"Databricks API Error: {response.text}"
                }

            # Jobs API returns run_id (different from Pipelines API which returns update_id)
            # run_id is used to poll job status via the Checker Lambda
            data = response.json()
            run_id = data.get('run_id')
            logger.info(f"✓ Databricks Job triggered successfully!")
            logger.info(f"  → Run ID: {run_id}")
            logger.info(f"  → Job ID: {job_id_int}")
            logger.info(f"  → Status: STARTED")
            logger.info("=" * 80)
            logger.info("LAUNCHER FUNCTION COMPLETED SUCCESSFULLY")
            logger.info("=" * 80)

            return {
                "statusCode": 200,
                "status": "STARTED",
                "run_id": run_id,
                "file_key": file_key,
                "bucket": bucket
            }

        except requests.exceptions.Timeout:
            logger.error("✗ Databricks API call timed out after 15 seconds")
            logger.error("The connection to Databricks took too long to complete")
            return {"statusCode": 504, "status": "ERROR", "message": "API Timeout."}
        except requests.exceptions.RequestException as e:
            logger.error(f"✗ Network error while communicating with Databricks API")
            logger.error(f"  → Error Details: {str(e)}")
            return {"statusCode": 502, "status": "ERROR", "message": f"Network Failure: {str(e)}"}

    except Exception as e:
        logger.error("=" * 80)
        logger.error("LAUNCHER FUNCTION FAILED WITH UNEXPECTED ERROR")
        logger.error("=" * 80)
        logger.error(f"Error Type: {type(e).__name__}")
        logger.error(f"Error Message: {str(e)}")
        logger.exception("Full stack trace:")

        error_traceback = traceback.format_exc()
        send_error_to_sqs('LauncherFunction', str(e), error_traceback, event, context)
        return {
            "statusCode": 500,
            "status": "ERROR",
            "message": f"Unexpected Lambda Failure: {str(e)}"
        }