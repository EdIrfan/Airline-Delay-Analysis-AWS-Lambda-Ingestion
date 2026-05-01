import pytest
import requests
from src.launcher.app import lambda_handler

def test_lambda_handler_success(mocker, mock_s3_event, mock_context):
    """Test successful pipeline trigger."""
    # 1. Mock the secret fetch
    mocker.patch('src.launcher.app.get_db_token', return_value='fake-token-123')

    # 2. Mock the GET call to fetch current pipeline configuration
    mock_get = mocker.patch('src.launcher.app.requests.get')
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = {
        "pipeline_id": "f4ed740f-01c3-4cac-8d09-939815359b64",
        "configuration": {
            "pipeline.env": "old-value"
        }
    }

    # 3. Mock the PUT call to update pipeline configuration
    mock_put = mocker.patch('src.launcher.app.requests.put')
    mock_put.return_value.status_code = 200

    # 4. Mock the POST call to trigger job
    mock_post = mocker.patch('src.launcher.app.requests.post')
    mock_post.return_value.status_code = 200
    mock_post.return_value.json.return_value = {"run_id": "update-999"}

    # 5. Run the handler
    response = lambda_handler(mock_s3_event, mock_context)

    # 6. Assertions
    assert response['statusCode'] == 200
    assert response['run_id'] == 'update-999'
    assert response['status'] == 'STARTED'
    assert response['bucket'] == 'fake-landing-bucket'
    assert response['file_key'] == 'input/airline_data.csv'
    mock_get.assert_called_once()
    mock_put.assert_called_once()
    mock_post.assert_called_once()


def test_lambda_handler_malformed_event(mocker, mock_context):
    """Test handler with malformed S3 event."""
    mocker.patch('src.launcher.app.get_db_token', return_value='fake-token-123')
    
    bad_event = {"detail": {}}  # Missing bucket/object data
    response = lambda_handler(bad_event, mock_context)

    assert response['statusCode'] == 400
    assert response['status'] == 'ERROR'
    assert 'Malformed S3 Event' in response['message']


def test_lambda_handler_auth_failure(mocker, mock_s3_event, mock_context):
    """Test handler when token retrieval fails."""
    mocker.patch('src.launcher.app.get_db_token', return_value=None)
    
    response = lambda_handler(mock_s3_event, mock_context)

    assert response['statusCode'] == 500
    assert response['status'] == 'ERROR'
    assert 'Authentication Failure' in response['message']


def test_lambda_handler_databricks_api_error(mocker, mock_s3_event, mock_context):
    """Test handler when Databricks API returns error."""
    mocker.patch('src.launcher.app.get_db_token', return_value='fake-token-123')

    mock_get = mocker.patch('src.launcher.app.requests.get')
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = {"configuration": {}}

    mock_put = mocker.patch('src.launcher.app.requests.put')
    mock_put.return_value.status_code = 200

    mock_post = mocker.patch('src.launcher.app.requests.post')
    mock_post.return_value.status_code = 400
    mock_post.return_value.text = "Invalid pipeline configuration"

    response = lambda_handler(mock_s3_event, mock_context)

    assert response['statusCode'] == 400
    assert response['status'] == 'ERROR'
    assert 'Databricks API Error' in response['message']


def test_lambda_handler_timeout(mocker, mock_s3_event, mock_context):
    """Test handler when Databricks API call times out."""
    mocker.patch('src.launcher.app.get_db_token', return_value='fake-token-123')

    mock_get = mocker.patch('src.launcher.app.requests.get')
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = {"configuration": {}}

    mock_put = mocker.patch('src.launcher.app.requests.put')
    mock_put.return_value.status_code = 200

    mock_post = mocker.patch('src.launcher.app.requests.post')
    mock_post.side_effect = requests.exceptions.Timeout("Connection timeout")

    response = lambda_handler(mock_s3_event, mock_context)

    assert response['statusCode'] == 504
    assert response['status'] == 'ERROR'
    assert 'API Timeout' in response['message']


def test_lambda_handler_network_error(mocker, mock_s3_event, mock_context):
    """Test handler when network error occurs."""
    mocker.patch('src.launcher.app.get_db_token', return_value='fake-token-123')

    mock_get = mocker.patch('src.launcher.app.requests.get')
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = {"configuration": {}}

    mock_put = mocker.patch('src.launcher.app.requests.put')
    mock_put.return_value.status_code = 200

    mock_post = mocker.patch('src.launcher.app.requests.post')
    mock_post.side_effect = requests.exceptions.ConnectionError("Network unreachable")

    response = lambda_handler(mock_s3_event, mock_context)

    assert response['statusCode'] == 502
    assert response['status'] == 'ERROR'
    assert 'Network Failure' in response['message']


def test_lambda_handler_pipeline_fetch_failure(mocker, mock_s3_event, mock_context):
    """Test handler when fetching pipeline configuration fails."""
    mocker.patch('src.launcher.app.get_db_token', return_value='fake-token-123')

    mock_get = mocker.patch('src.launcher.app.requests.get')
    mock_get.return_value.status_code = 404
    mock_get.return_value.text = '{"error_code": "NOT_FOUND", "message": "Pipeline not found"}'

    response = lambda_handler(mock_s3_event, mock_context)

    assert response['statusCode'] == 404
    assert response['status'] == 'ERROR'
    assert 'Failed to fetch pipeline configuration' in response['message']


def test_lambda_handler_pipeline_config_update_failure(mocker, mock_s3_event, mock_context):
    """Test handler when pipeline configuration PUT fails."""
    mocker.patch('src.launcher.app.get_db_token', return_value='fake-token-123')

    mock_get = mocker.patch('src.launcher.app.requests.get')
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = {"configuration": {}}

    mock_put = mocker.patch('src.launcher.app.requests.put')
    mock_put.return_value.status_code = 400
    mock_put.return_value.text = '{"error_code": "INVALID_PARAMETER_VALUE", "message": "Changing a UC pipeline to a HMS pipeline is not allowed."}'

    response = lambda_handler(mock_s3_event, mock_context)

    assert response['statusCode'] == 400
    assert response['status'] == 'ERROR'
    assert 'Pipeline configuration update failed' in response['message']