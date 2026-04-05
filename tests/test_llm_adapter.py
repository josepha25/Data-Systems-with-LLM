import pytest
from unittest.mock import MagicMock, patch
from modules.llm_adapter import LLMAdapter

MOCK_SCHEMA = {
    "employees": [
        {"name": "id"}, {"name": "name"},
        {"name": "department"}, {"name": "salary"}
    ]
}


@pytest.fixture
def adapter():
    with patch("modules.llm_adapter.anthropic.Anthropic"):
        adapter = LLMAdapter()
        return adapter


def make_mock_response(text: str):
    mock_content = MagicMock()
    mock_content.text = text
    mock_message = MagicMock()
    mock_message.content = [mock_content]
    return mock_message


# --- translate ---

def test_translate_returns_sql(adapter):
    adapter.client.messages.create.return_value = make_mock_response(
        "SQL: SELECT * FROM employees\nEXPLANATION: Returns all employees."
    )
    result = adapter.translate("Show all employees", MOCK_SCHEMA)
    assert result["success"] is True
    assert "SELECT" in result["sql"].upper()

def test_translate_returns_explanation(adapter):
    adapter.client.messages.create.return_value = make_mock_response(
        "SQL: SELECT * FROM employees\nEXPLANATION: Returns all employees."
    )
    result = adapter.translate("Show all employees", MOCK_SCHEMA)
    assert result["explanation"] != ""

def test_translate_empty_query(adapter):
    result = adapter.translate("", MOCK_SCHEMA)
    assert result["success"] is False
    assert result["error"] != ""

def test_translate_unparseable_response(adapter):
    adapter.client.messages.create.return_value = make_mock_response(
        "I don't know how to answer that."
    )
    result = adapter.translate("Show all employees", MOCK_SCHEMA)
    assert result["success"] is False

def test_translate_api_error(adapter):
    adapter.client.messages.create.side_effect = Exception("API error")
    result = adapter.translate("Show all employees", MOCK_SCHEMA)
    assert result["success"] is False
    assert "API error" in result["error"]

def test_translate_returns_select_only(adapter):
    adapter.client.messages.create.return_value = make_mock_response(
        "SQL: SELECT name FROM employees\nEXPLANATION: Returns names."
    )
    result = adapter.translate("Show employee names", MOCK_SCHEMA)
    assert result["sql"].upper().startswith("SELECT")