import pytest
from src.cleanup.app import lambda_handler

def test_cleanup_success(mocker):
    # 1. Patch the global instance ALREADY created in the app file
    # This prevents 'NoCredentialsError' during the test run
    mock_s3 = mocker.patch('src.cleanup.app.s3_client') 
    
    # 2. Define the event matching our Step Function state
    event = {"bucket": "fake-bucket", "file_key": "fake-file.csv"}
    
    # 3. Execute the handler
    response = lambda_handler(event, None)

    # 4. Assertions based on our NEW data contract (status vs statusCode)
    assert response['status'] == 'COMPLETED'
    assert response['cleanup_outcome'] == 'DELETED'
    
    # 5. Verify the S3 call parameters
    mock_s3.delete_object.assert_called_with(
        Bucket="fake-bucket", 
        Key="fake-file.csv"
    )