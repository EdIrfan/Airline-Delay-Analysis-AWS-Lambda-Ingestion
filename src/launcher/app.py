import os
import json
import boto3
import requests
import logging
from botocore.exceptions import ClientError

# Set up structured logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

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
    logger.info("Lambda execution started with event--")
    try:
        # 1. Extract file info from S3 Event
        try:
            # Log the incoming event for debugging (Be careful with PII in production)
            logger.info(f"Incoming Event: {json.dumps(event)}")
            
            bucket = event['Records'][0]['s3']['bucket']['name']
            file_key = event['Records'][0]['s3']['object']['key']
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
        
        # 4. Trigger Databricks DLT Update
        api_url = f"{db_host.rstrip('/')}/api/2.0/pipelines/{pipeline_id}/updates"
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        payload = {
            "full_refresh": False, 
            "cause": "API_TRIGGER",
            "pipeline_parameters": {
                "pipeline.env": env_type,
                "pipeline.landing_path": f"s3://{bucket}/"
            }
        }

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
        return {
            "statusCode": 500,
            "status": "ERROR",
            "message": f"Unexpected Lambda Failure: {str(e)}"
        }