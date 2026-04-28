import pytest
from src.checker.app import lambda_handler

def test_checker_success(mocker):
    mocker.patch('src.checker.app.get_db_token', return_value='fake-token-123')
    
    # Mock Databricks status response
    mock_get = mocker.patch('requests.get')
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = {
        "update": {"state": "COMPLETED"}
    }

    event = {"update_id": "update-999", "bucket": "b", "file_key": "f"}
    response = lambda_handler(event, None)

    assert response['status'] == 'SUCCESS'