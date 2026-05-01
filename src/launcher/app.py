import os
import json
import boto3
import requests
import logging
import traceback
from datetime import datetime, timezone
from botocore.exceptions import ClientError

# Set up structured logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize SQS client for error reporting
sqs_client = boto3.client('sqs')
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


def get_db_token(secret_arn):
    """Fetches credentials from Secrets Manager with structured logging."""
    client = boto3.client('secretsmanager')
    try:
        logger.info(f"Attempting to fetch secret: {secret_arn}")
        response = client.get_secret_value(SecretId=secret_arn)
        secret_dict = json.loads(response['SecretString'])
        
        token = secret_dict.get('token')
        if not token:
            logger.error("Secret found, but 'token' key is missing in JSON.")
            return None
            
        return token
    except Exception as e:
        logger.exception(f"CRITICAL: Secrets Manager fetch failed.")
        return None

def lambda_handler(event, context):
    logger.info("Lambda execution started with event")
    try:
        # 1. Extract file info from S3 Event
        try:
            # Log the incoming event for debugging (Be careful with PII in production)
            logger.info(f"Incoming Event: {json.dumps(event)}")
            
            bucket = event['detail']['bucket']['name']
            file_key = event['detail']['object']['key']
            logger.info(f"Target file detected: s3://{bucket}/{file_key}")
        except (KeyError, IndexError, TypeError) as e:
            logger.error(f"Event structure validation failed: {str(e)}")
            return {
                "statusCode": 400,
                "status": "ERROR",
                "message": "Malformed S3 Event structure."
            }

        # 2. Setup Config
        db_host = os.environ.get('DATABRICKS_HOST')
        secret_arn = os.environ.get('SECRET_ARN')
        pipeline_id = os.environ.get('PIPELINE_ID')
        env_type = os.environ.get('ENV_TYPE')

        if not all([db_host, secret_arn, pipeline_id, env_type]):
            logger.error("Environment variables are incomplete.")
            return {"statusCode": 500, "status": "ERROR", "message": "Missing Environment Variables."}

        # 3. Authenticate
        token = get_db_token(secret_arn)
        if not token:
            return {
                "statusCode": 500, 
                "status": "ERROR",
                "message": "Authentication Failure: Could not retrieve token."
            }
        
        # 4. Update Pipeline Configuration with runtime parameters
        patch_url = f"{db_host.rstrip('/')}/api/2.0/pipelines/{pipeline_id}"
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        
        # First, update the pipeline configuration
        config_payload = {
            "configuration": {
                "pipeline.env": env_type,
                "pipeline.landing_path": f"s3://{bucket}/"
            }
        }
        
        logger.info(f"Updating pipeline configuration: {json.dumps(config_payload)}")
        
        try:
            config_response = requests.patch(patch_url, headers=headers, json=config_payload, timeout=15)
            if config_response.status_code not in [200, 201]:
                logger.error(f"Failed to update pipeline config: {config_response.status_code} - {config_response.text}")
                return {
                    "statusCode": config_response.status_code,
                    "status": "ERROR",
                    "message": f"Pipeline config update failed: {config_response.text}"
                }
            logger.info("Pipeline configuration updated successfully")
        except requests.exceptions.RequestException as e:
            logger.exception("Failed to update pipeline configuration")
            return {"statusCode": 502, "status": "ERROR", "message": f"Config update failed: {str(e)}"}
        
        # 5. Now trigger the pipeline update
        api_url = f"{db_host.rstrip('/')}/api/2.0/pipelines/{pipeline_id}/updates"
        payload = {"full_refresh": False}

        logger.info(f"Triggering DLT Pipeline: {pipeline_id} for ENV: {env_type}")
        
        try:
            response = requests.post(api_url, headers=headers, json=payload, timeout=15)
            
            if response.status_code != 200:
                logger.error(f"Databricks API Rejected Request. Code: {response.status_code}, Response: {response.text}")
                return {
                    "statusCode": response.status_code,
                    "status": "ERROR",
                    "message": f"Databricks API Error: {response.text}"
                }

            data = response.json()
            update_id = data.get('update_id')
            logger.info(f"DLT Update Triggered Successfully. Update ID: {update_id}")
            
            return {
                "statusCode": 200,
                "status": "STARTED",
                "update_id": update_id,
                "file_key": file_key,
                "bucket": bucket
            }
            
        except requests.exceptions.Timeout:
            logger.warning("Databricks API call timed out after 15 seconds.")
            return {"statusCode": 504, "status": "ERROR", "message": "API Timeout."}
        except requests.exceptions.RequestException as e:
            logger.exception("Network failure during Databricks API call.")
            return {"statusCode": 502, "status": "ERROR", "message": f"Network Failure: {str(e)}"}

    except Exception as e:
        # 5. Global Catch-All
        logger.exception("Unexpected system failure.")
        error_traceback = traceback.format_exc()
        send_error_to_sqs('LauncherFunction', str(e), error_traceback, event, context)
        return {
            "statusCode": 500,
            "status": "ERROR",
            "message": f"Unexpected Lambda Failure: {str(e)}"
        }