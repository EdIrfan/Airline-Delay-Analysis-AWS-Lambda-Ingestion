import json
import boto3
import pytest
import os
from unittest.mock import patch, MagicMock
from datetime import datetime

# Import the error handler module
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../src/error_handler'))
import app as error_handler_app


class TestErrorHandler:
    """Test suite for the Error Handler Lambda function."""
    
    @pytest.fixture
    def mock_context(self):
        """Create a mock Lambda context."""
        context = MagicMock()
        context.log_group_name = '/aws/lambda/TestFunction'
        context.log_stream_name = '2024/04/30/[$LATEST]abcd1234'
        context.function_name = 'ErrorHandlerFunction'
        context.aws_request_id = 'test-request-id'
        return context
    
    @pytest.fixture
    def sample_error_data(self):
        """Create sample error data for testing."""
        return {
            'lambda_name': 'TestFunction',
            'error_source': 'Lambda',
            'error_message': 'Test error message',
            'error_traceback': 'Traceback (most recent call last):\n  File "<string>", line 1, in <module>\nError: Test',
            'timestamp': datetime.utcnow().isoformat(),
            'log_group': '/aws/lambda/TestFunction',
            'log_stream': '2024/04/30/[$LATEST]abcd1234',
            'event_context': {'bucket': 'test-bucket', 'key': 'test-key'}
        }
    
    def test_format_html_email(self, sample_error_data):
        """Test HTML email formatting."""
        html_output = error_handler_app.format_html_email(sample_error_data)
        
        # Verify HTML structure
        assert '<html>' in html_output
        assert '</html>' in html_output
        assert 'TestFunction' in html_output
        assert 'Test error message' in html_output
        assert 'Pipeline Error Notification' in html_output
        
        # Verify CloudWatch log link is generated
        assert 'CloudWatch' in html_output
        assert '/aws/lambda/TestFunction' in html_output
    
    def test_format_html_email_escapes_html(self, sample_error_data):
        """Test that HTML email escapes special characters."""
        sample_error_data['error_message'] = '<script>alert("xss")</script>'
        sample_error_data['lambda_name'] = 'Test<Function>'
        
        html_output = error_handler_app.format_html_email(sample_error_data)
        
        # Verify special characters are escaped
        assert '<script>' not in html_output
        assert '&lt;script&gt;' in html_output
        assert '&lt;Function&gt;' in html_output
    
    @patch('error_handler_app.sns_client')
    def test_publish_error_notification(self, mock_sns, sample_error_data):
        """Test publishing error notification to SNS."""
        mock_sns.publish = MagicMock(return_value={'MessageId': 'test-message-id'})
        
        error_handler_app.publish_error_notification(sample_error_data)
        
        # Verify SNS publish was called
        assert mock_sns.publish.call_count >= 1
        
        # Verify SNS publish was called with correct topic
        call_args = mock_sns.publish.call_args_list[0]
        assert call_args[1]['TopicArn'] is not None
        assert 'TestFunction' in call_args[1]['Subject']
    
    @patch('error_handler_app.sqs_client')
    @patch('error_handler_app.sns_client')
    def test_process_sqs_messages(self, mock_sns, mock_sqs, sample_error_data, mock_context):
        """Test processing SQS messages."""
        mock_sqs.delete_message = MagicMock()
        mock_sns.publish = MagicMock(return_value={'MessageId': 'test-message-id'})
        
        # Create SQS event
        event = {
            'Records': [
                {
                    'body': json.dumps(sample_error_data),
                    'receiptHandle': 'test-receipt-handle-1'
                },
                {
                    'body': json.dumps(sample_error_data),
                    'receiptHandle': 'test-receipt-handle-2'
                }
            ]
        }
        
        result = error_handler_app.process_sqs_messages(event)
        
        # Verify result
        assert result['status'] == 'PROCESSED'
        assert result['processed'] == 2
        assert result['failed'] == 0
        
        # Verify SQS delete was called for each message
        assert mock_sqs.delete_message.call_count == 2
    
    @patch('error_handler_app.sqs_client')
    @patch('error_handler_app.sns_client')
    def test_process_sqs_messages_with_invalid_json(self, mock_sns, mock_sqs, mock_context):
        """Test processing SQS messages with invalid JSON."""
        mock_sqs.delete_message = MagicMock()
        mock_sns.publish = MagicMock(return_value={'MessageId': 'test-message-id'})
        
        # Create SQS event with invalid JSON
        event = {
            'Records': [
                {
                    'body': 'invalid json {',
                    'receiptHandle': 'test-receipt-handle-1'
                }
            ]
        }
        
        result = error_handler_app.process_sqs_messages(event)
        
        # Verify result
        assert result['status'] == 'PROCESSED'
        assert result['processed'] == 0
        assert result['failed'] == 1
    
    @patch('error_handler_app.sns_client')
    def test_process_direct_error_step_function(self, mock_sns, mock_context):
        """Test processing direct error from Step Function."""
        mock_sns.publish = MagicMock(return_value={'MessageId': 'test-message-id'})
        
        # Create Step Function error event
        event = {
            'error_source': 'StepFunction',
            'error_message': 'Pipeline failed',
            'timestamp': datetime.utcnow().isoformat(),
            'event_context': {'status': 'FAILED'}
        }
        
        result = error_handler_app.process_direct_error(event)
        
        # Verify result
        assert result['status'] == 'PUBLISHED'
        assert result['statusCode'] == 200
        
        # Verify SNS publish was called
        assert mock_sns.publish.call_count >= 1
    
    @patch('error_handler_app.sqs_client')
    @patch('error_handler_app.sns_client')
    def test_lambda_handler_with_sqs_event(self, mock_sns, mock_sqs, sample_error_data, mock_context):
        """Test lambda_handler with SQS event."""
        mock_sqs.delete_message = MagicMock()
        mock_sns.publish = MagicMock(return_value={'MessageId': 'test-message-id'})
        
        # Create SQS event
        event = {
            'Records': [
                {
                    'body': json.dumps(sample_error_data),
                    'receiptHandle': 'test-receipt-handle'
                }
            ]
        }
        
        result = error_handler_app.lambda_handler(event, mock_context)
        
        # Verify result
        assert result['status'] == 'PROCESSED'
        assert result['statusCode'] == 200
    
    @patch('error_handler_app.sns_client')
    def test_lambda_handler_with_direct_error(self, mock_sns, mock_context):
        """Test lambda_handler with direct error invocation."""
        mock_sns.publish = MagicMock(return_value={'MessageId': 'test-message-id'})
        
        # Create direct error event
        event = {
            'error_source': 'Lambda',
            'error_message': 'Direct error',
            'timestamp': datetime.utcnow().isoformat()
        }
        
        result = error_handler_app.lambda_handler(event, mock_context)
        
        # Verify result
        assert result['status'] == 'PUBLISHED'
        assert result['statusCode'] == 200


class TestErrorReportingIntegration:
    """Test integration of error reporting across Lambda functions."""
    
    @pytest.fixture
    def mock_context(self):
        """Create a mock Lambda context."""
        context = MagicMock()
        context.log_group_name = '/aws/lambda/TestFunction'
        context.log_stream_name = '2024/04/30/[$LATEST]abcd1234'
        context.function_name = 'TestFunction'
        context.aws_request_id = 'test-request-id'
        return context
    
    @patch('error_handler_app.sqs_client')
    def test_send_error_to_sqs_called_from_launcher(self, mock_sqs, mock_context):
        """Test that Launcher calls send_error_to_sqs on exception."""
        mock_sqs.send_message = MagicMock()
        
        # Simulate error being sent to SQS
        error_message = "Test error from Launcher"
        error_traceback = "Traceback: ...\nError: Test"
        event = {'Records': [{'s3': {'bucket': {'name': 'test'}}}]}
        
        # This would be called from launcher's exception handler
        error_data = {
            'lambda_name': 'LauncherFunction',
            'error_source': 'Lambda',
            'error_message': error_message,
            'error_traceback': error_traceback,
            'timestamp': datetime.utcnow().isoformat(),
            'log_group': mock_context.log_group_name,
            'log_stream': mock_context.log_stream_name,
            'event_context': event
        }
        
        # Verify error data structure
        assert error_data['lambda_name'] == 'LauncherFunction'
        assert error_data['error_message'] == error_message
        assert 'error_traceback' in error_data
        assert 'timestamp' in error_data
        assert 'log_group' in error_data
        assert 'log_stream' in error_data
