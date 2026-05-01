import pytest
from botocore.exceptions import ClientError
from src.cleanup.app import lambda_handler

def test_cleanup_success(mocker, mock_context):
    """Test successful file deletion."""
    mock_s3 = mocker.patch('src.cleanup.app.s3_client') 
    
    event = {"bucket": "fake-bucket", "file_key": "fake-file.csv"}
    response = lambda_handler(event, mock_context)

    assert response['status'] == 'COMPLETED'
    assert response['cleanup_outcome'] == 'DELETED'
    
    mock_s3.delete_object.assert_called_with(
        Bucket="fake-bucket", 
        Key="fake-file.csv"
    )


def test_cleanup_missing_parameters(mocker, mock_context):
    """Test cleanup when bucket or file_key is missing."""
    mocker.patch('src.cleanup.app.s3_client')
    
    event = {"bucket": "fake-bucket"}
    response = lambda_handler(event, mock_context)

    assert response['status'] == 'ERROR'
    assert 'Missing S3 coordinates' in response['message']


def test_cleanup_file_not_found(mocker, mock_context):
    """Test cleanup when file is already gone (idempotent)."""
    mock_s3 = mocker.patch('src.cleanup.app.s3_client')
    
    error_response = {'Error': {'Code': 'NoSuchKey'}}
    mock_s3.delete_object.side_effect = ClientError(error_response, 'DeleteObject')
    
    event = {"bucket": "fake-bucket", "file_key": "fake-file.csv"}
    response = lambda_handler(event, mock_context)

    assert response['status'] == 'COMPLETED'
    assert response['cleanup_outcome'] == 'ALREADY_GONE'


def test_cleanup_s3_error(mocker, mock_context):
    """Test cleanup when S3 deletion fails."""
    mock_s3 = mocker.patch('src.cleanup.app.s3_client')
    
    error_response = {'Error': {'Code': 'AccessDenied'}}
    mock_s3.delete_object.side_effect = ClientError(error_response, 'DeleteObject')
    
    event = {"bucket": "fake-bucket", "file_key": "fake-file.csv"}
    response = lambda_handler(event, mock_context)

    assert response['status'] == 'ERROR'