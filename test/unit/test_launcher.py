import pytest
import requests
from src.launcher.app import lambda_handler

def test_lambda_handler_success(mocker, mock_s3_event):
    """Test successful pipeline trigger."""
    # 1. Mock the secret fetch
    mocker.patch('src.launcher.app.get_db_token', return_value='fake-token-123')
    
    # 2. Mock the requests.post call to Databricks
    mock_post = mocker.patch('src.launcher.app.requests.post')
    mock_post.return_value.status_code = 200
    mock_post.return_value.json.return_value = {"update_id": "update-999"}

    # 3. Run the handler
    response = lambda_handler(mock_s3_event, None)

    # 4. Assertions
    assert response['statusCode'] == 200
    assert response['update_id'] == 'update-999'
    assert response['status'] == 'STARTED'
    assert response['bucket'] == 'fake-landing-bucket'
    assert response['file_key'] == 'input/airline_data.csv'
    mock_post.assert_called_once()


def test_lambda_handler_malformed_event(mocker):
    """Test handler with malformed S3 event."""
    mocker.patch('src.launcher.app.get_db_token', return_value='fake-token-123')
    
    bad_event = {"Records": []}  # Missing S3 data
    response = lambda_handler(bad_event, None)

    assert response['statusCode'] == 400
    assert response['status'] == 'ERROR'
    assert 'Malformed S3 Event' in response['message']


def test_lambda_handler_auth_failure(mocker, mock_s3_event):
    """Test handler when token retrieval fails."""
    mocker.patch('src.launcher.app.get_db_token', return_value=None)
    
    response = lambda_handler(mock_s3_event, None)

    assert response['statusCode'] == 500
    assert response['status'] == 'ERROR'
    assert 'Authentication Failure' in response['message']


def test_lambda_handler_databricks_api_error(mocker, mock_s3_event):
    """Test handler when Databricks API returns error."""
    mocker.patch('src.launcher.app.get_db_token', return_value='fake-token-123')
    
    mock_post = mocker.patch('src.launcher.app.requests.post')
    mock_post.return_value.status_code = 400
    mock_post.return_value.text = "Invalid pipeline configuration"

    response = lambda_handler(mock_s3_event, None)

    assert response['statusCode'] == 400
    assert response['status'] == 'ERROR'
    assert 'Databricks API Error' in response['message']


def test_lambda_handler_timeout(mocker, mock_s3_event):
    """Test handler when Databricks API call times out."""
    mocker.patch('src.launcher.app.get_db_token', return_value='fake-token-123')
    
    mock_post = mocker.patch('src.launcher.app.requests.post')
    mock_post.side_effect = requests.exceptions.Timeout("Connection timeout")

    response = lambda_handler(mock_s3_event, None)

    assert response['statusCode'] == 504
    assert response['status'] == 'ERROR'
    assert 'API Timeout' in response['message']


def test_lambda_handler_network_error(mocker, mock_s3_event):
    """Test handler when network error occurs."""
    mocker.patch('src.launcher.app.get_db_token', return_value='fake-token-123')
    
    mock_post = mocker.patch('src.launcher.app.requests.post')
    mock_post.side_effect = requests.exceptions.ConnectionError("Network unreachable")

    response = lambda_handler(mock_s3_event, None)

    assert response['statusCode'] == 502
    assert response['status'] == 'ERROR'
    assert 'Network Failure' in response['message']