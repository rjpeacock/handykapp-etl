import pytest
import importlib

rp_module = importlib.import_module("processors.record_processor")


def test_transform_single_record_returns_none_on_error(mock_logger):
    def failing_transformer(record):
        raise ValueError("Transform failed")
    
    result = rp_module.transform_single_record(
        {"data": "test"}, failing_transformer, "test_file.csv", mock_logger
    )
    
    assert result is None
    mock_logger.error.assert_called_once()


def test_transform_single_record_returns_result_on_success(mock_logger):
    def successful_transformer(record):
        return ["race1", "race2"]
    
    result = rp_module.transform_single_record(
        {"data": "test"}, successful_transformer, "test_file.csv", mock_logger
    )
    
    assert result == ["race1", "race2"]
