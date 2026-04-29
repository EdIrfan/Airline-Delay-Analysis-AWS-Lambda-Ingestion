import pytest
import os

@pytest.fixture(autouse=True)
def setup_env():
    """Sets up fake environment variables for all tests."""
    os.environ['DATABRICKS_HOST'] = 'https://fake-workspace.cloud.databricks.com'
    os.environ['SECRET_ARN'] = 'arn:aws:secretsmanager:us-east-1:123456789012:secret:db-token'
    os.environ['PIPELINE_ID'] = 'fake-pipeline-123'
    os.environ['ENV_TYPE'] = 'dev'
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-2'
    os.environ['ERROR_QUEUE_URL'] = 'https://sqs.us-east-2.amazonaws.com/123456789012/airline-error-notification-dev'
    os.environ['ERROR_TOPIC_ARN'] = 'arn:aws:sns:us-east-2:123456789012:airline-error-notifications-dev'

@pytest.fixture
def mock_s3_event():
    """Returns a dummy S3 event structure."""
    return {
        "Records": [{
            "s3": {
                "bucket": {"name": "fake-landing-bucket"},
                "object": {"key": "input/airline_data.csv"}
            }
        }]
    }