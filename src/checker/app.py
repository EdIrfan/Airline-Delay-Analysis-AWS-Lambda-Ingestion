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
    """Securely fetches the Databricks token."""
    client = boto3.client('secretsmanager')
    try:
        logger.info(f"Fetching secret for token: {secret_arn}")
        response = client.get_secret_value(SecretId=secret_arn)
        secret_dict = json.loads(response['SecretString'])
        return secret_dict.get('token')
    except Exception:
        logger.exception("CRITICAL: Failed to retrieve Databricks token from Secrets Manager.")
        return None

def lambda_handler(event, context):
    logger.info(f"Checker Lambda execution started with event: {json.dumps(event)}")
    
    try:
        # 1. Configuration & Input Validation
        update_id = event.get('update_id')
        db_host = os.environ.get('DATABRICKS_HOST')
        secret_arn = os.environ.get('SECRET_ARN')
        pipeline_id = os.environ.get('PIPELINE_ID')

        if not update_id:
            logger.error("Missing 'update_id' in incoming event. Checker cannot proceed.")
            return {**event, "status": "ERROR", "message": "Missing update_id."}

        if not all([db_host, secret_arn, pipeline_id]):
            logger.error("Environment variables are misconfigured.")
            return {**event, "status": "ERROR", "message": "Missing Environment Variables."}

        # 2. Authentication
        token = get_db_token(secret_arn)
        if not token:
            return {**event, "status": "ERROR", "message": "Authentication Failure."}

        # 3. Poll Databricks API
        api_url = f"{db_host.rstrip('/')}/api/2.0/pipelines/{pipeline_id}/updates/{update_id}"
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

        logger.info(f"Polling DLT Status for Update: {update_id}")
        
        try:
            # Short timeout for polling (10s)
            response = requests.get(api_url, headers=headers, timeout=10)
            
            if response.status_code != 200:
                logger.error(f"Databricks API Error: {response.status_code} - {response.text}")
                # We return RUNNING to retry if it's a transient 5xx, or ERROR if it's a 4xx
                status = "RUNNING" if response.status_code >= 500 else "ERROR"
                return {**event, "status": status, "api_error": response.text}

            data = response.json()
            # Defensive check for the nested JSON structure
            current_state = data.get('update', {}).get('state')
            logger.info(f"Update {update_id} current state: {current_state}")

            # 4. State Mapping for Step Function
            # 'COMPLETED' -> Success
            # 'FAILED', 'CANCELED' -> Hard Failure
            # 'INITIALIZING', 'SETTING_UP', 'RUNNING', 'QUEUED', 'WAITING_FOR_RESOURCES' -> Keep Waiting
            
            if current_state == 'COMPLETED':
                logger.info(f"DLT Pipeline Sync Finished: {update_id}")
                return {**event, "status": "SUCCESS", "raw_state": current_state}
            
            elif current_state in ['FAILED', 'CANCELED']:
                logger.error(f"DLT Pipeline {current_state}: {update_id}")
                return {**event, "status": "FAILED", "raw_state": current_state}
            
            else:
                # Still in progress
                logger.info("---Still waiting---")
                return {**event, "status": "RUNNING", "raw_state": current_state}

        except requests.exceptions.RequestException as e:
            logger.exception("Network failure while polling Databricks.")
            # Return RUNNING so the Step Function retries the poll instead of killing the pipeline
            return {**event, "status": "RUNNING", "error": str(e)}

    except Exception as e:
        logger.exception("Unexpected system failure in Checker Lambda.")
        return {**event, "status": "ERROR", "message": f"Unexpected Failure: {str(e)}"}