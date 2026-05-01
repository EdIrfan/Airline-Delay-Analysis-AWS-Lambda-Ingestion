import pytest
import requests
from src.checker.app import lambda_handler

def test_checker_success(mocker, mock_context):
    """Test checker when pipeline completes successfully."""
    mocker.patch('src.checker.app.get_db_token', return_value='fake-token-123')
    
    # Mock Databricks status response
    mock_get = mocker.patch('src.checker.app.requests.get')
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = {
        "update": {"state": "COMPLETED"}
    }

    event = {"update_id": "update-999", "bucket": "b", "file_key": "f"}
    response = lambda_handler(event, mock_context)

    assert response['status'] == 'SUCCESS'


def test_checker_missing_update_id(mocker, mock_context):
    """Test checker when update_id is missing."""
    mocker.patch('src.checker.app.get_db_token', return_value='fake-token-123')
    
    event = {"bucket": "b", "file_key": "f"}
    response = lambda_handler(event, mock_context)

    assert response['status'] == 'ERROR'
    assert 'update_id' in response['message']


def test_checker_auth_failure(mocker, mock_context):
    """Test checker when token retrieval fails."""
    mocker.patch('src.checker.app.get_db_token', return_value=None)
    
    event = {"update_id": "update-999", "bucket": "b", "file_key": "f"}
    response = lambda_handler(event, mock_context)

    assert response['status'] == 'ERROR'
    assert 'Authentication' in response['message']


def test_checker_pipeline_failed(mocker, mock_context):
    """Test checker when pipeline fails."""
    mocker.patch('src.checker.app.get_db_token', return_value='fake-token-123')
    
    mock_get = mocker.patch('src.checker.app.requests.get')
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = {
        "update": {"state": "FAILED"}
    }

    event = {"update_id": "update-999", "bucket": "b", "file_key": "f"}
    response = lambda_handler(event, mock_context)

    assert response['status'] == 'FAILED'


def test_checker_still_running(mocker, mock_context):
    """Test checker when pipeline is still running."""
    mocker.patch('src.checker.app.get_db_token', return_value='fake-token-123')
    
    mock_get = mocker.patch('src.checker.app.requests.get')
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = {
        "update": {"state": "RUNNING"}
    }

    event = {"update_id": "update-999", "bucket": "b", "file_key": "f"}
    response = lambda_handler(event, mock_context)

    assert response['status'] == 'RUNNING'