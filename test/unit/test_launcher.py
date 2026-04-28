import pytest
from src.launcher.app import lambda_handler

def test_lambda_handler_success(mocker, mock_s3_event):
    # 1. Mock the secret fetch
    mocker.patch('src.launcher.app.get_db_token', return_value='fake-token-123')
    
    # 2. Mock the requests.post call to Databricks
    mock_post = mocker.patch('requests.post')
    mock_post.return_value.status_code = 200
    mock_post.return_value.json.return_value = {"update_id": "update-999"}

    # 3. Run the handler
    response = lambda_handler(mock_s3_event, None)

    # 4. Assertions
    assert response['statusCode'] == 200
    assert response['update_id'] == 'update-999'
    assert response['status'] == 'STARTED'
    mock_post.assert_called_once() # Ensure we actually hit the API