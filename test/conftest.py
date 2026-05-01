import pytest
import os

# Set environment variables BEFORE any imports
os.environ['DATABRICKS_HOST'] = 'https://fake-workspace.cloud.databricks.com'
os.environ['SECRET_ARN'] = 'arn:aws:secretsmanager:us-east-1:123456789012:secret:db-token'
os.environ['DATABRICKS_JOB_ID'] = '12345'
os.environ['DATABRICKS_PIPELINE_ID'] = 'f4ed740f-01c3-4cac-8d09-939815359b64'
os.environ['ENV_TYPE'] = 'dev'
os.environ['AWS_DEFAULT_REGION'] = 'us-east-2'
os.environ['ERROR_QUEUE_URL'] = 'https://sqs.us-east-2.amazonaws.com/123456789012/airline-error-notification-dev'
os.environ['ERROR_TOPIC_ARN'] = 'arn:aws:sns:us-east-2:123456789012:airline-error-notifications-dev'

@pytest.fixture(autouse=True)
def setup_env():
    """Ensures environment variables are set for all tests."""
    pass

@pytest.fixture
def mock_s3_event():
    """Returns a dummy EventBridge S3 event structure."""
    return {
        "version": "0",
        "id": "test-event-id",
        "detail-type": "Object Created",
        "source": "aws.s3",
        "detail": {
            "bucket": {"name": "fake-landing-bucket"},
            "object": {"key": "input/airline_data.csv"}
        }
    }

@pytest.fixture
def mock_context():
    """Returns a mock Lambda context object."""
    class MockContext:
        def __init__(self):
            self.log_group_name = "/aws/lambda/test-function"
            self.log_stream_name = "2024/01/01/[$LATEST]test123"
            self.function_name = "test-function"
            self.memory_limit_in_mb = 128
            self.invoked_function_arn = "arn:aws:lambda:us-east-1:123456789012:function:test-function"
            self.aws_request_id = "test-request-id"
    
    return MockContext()