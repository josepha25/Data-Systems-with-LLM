import pytest
from modules.sql_validator import SQLValidator

# Mock schema: validator needs to know what tables/columns exist
MOCK_SCHEMA = {
    "employees": ["id", "name", "department", "hire_date", "salary"],
    "sales": ["id", "sale_id", "product_id", "quantity", "revenue"],
}


@pytest.fixture
def validator():
    return SQLValidator(schema=MOCK_SCHEMA)


# --- Query type tests ---

def test_allows_select(validator):
    assert validator.validate("SELECT * FROM employees") is True

def test_rejects_insert(validator):
    assert validator.validate("INSERT INTO employees VALUES (1, 'Alice', 'Eng', '2020-01-01', 95000)") is False

def test_rejects_drop(validator):
    assert validator.validate("DROP TABLE employees") is False

def test_rejects_update(validator):
    assert validator.validate("UPDATE employees SET name='Bob' WHERE id=1") is False

def test_rejects_delete(validator):
    assert validator.validate("DELETE FROM employees WHERE id=1") is False


# --- Table validation tests ---

def test_rejects_unknown_table(validator):
    assert validator.validate("SELECT * FROM ghost_table") is False

def test_allows_known_table(validator):
    assert validator.validate("SELECT * FROM employees") is True


# --- Column validation tests ---

def test_rejects_unknown_column(validator):
    assert validator.validate("SELECT salary_fake FROM employees") is False

def test_allows_star(validator):
    assert validator.validate("SELECT * FROM employees") is True

def test_allows_known_column(validator):
    assert validator.validate("SELECT department FROM employees") is True


# --- Edge cases ---

def test_rejects_empty_query(validator):
    assert validator.validate("") is False

def test_rejects_semicolon_injection(validator):
    assert validator.validate("SELECT * FROM employees; DROP TABLE employees") is False

def test_error_message_set_on_failure(validator):
    validator.validate("SELECT * FROM ghost_table")
    assert validator.last_error != ""

def test_error_message_mentions_unknown_table(validator):
    validator.validate("SELECT * FROM ghost_table")
    assert "ghost_table" in validator.last_error.lower()

def test_error_message_mentions_unknown_column(validator):
    validator.validate("SELECT salary_fake FROM employees")
    assert "salary_fake" in validator.last_error.lower()

def test_no_error_on_valid_query(validator):
    validator.validate("SELECT * FROM employees")
    assert validator.last_error == ""