import pytest
from unittest.mock import patch
from etl_pipeline import extract_data

@patch('etl_pipeline.requests.get')
def test_extract_data_success(mock_get):
    # Mock da resposta da API
    mock_response = mock_get.return_value
    mock_response.status_code = 200
    mock_response.json.return_value = {"current_weather": {"temperature": 32.5}}
    
    data = extract_data("http://fakeurl.com")
    
    assert "current_weather" in data
    assert data["current_weather"]["temperature"] == 32.5

@patch('etl_pipeline.requests.get')
def test_extract_data_failure(mock_get):
    # Simula um erro de conexão
    mock_get.side_effect = Exception("Erro de Conexão")
    
    with pytest.raises(Exception):
        extract_data("http://fakeurl.com")