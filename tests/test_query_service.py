import pytest
import os
import sqlite3
from modules.query_service import QueryService
from modules.csv_loader import CSVLoader
import pandas as pd

TEST_DB = "test_query.db"


@pytest.fixture(autouse=True)
def cleanup():
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)
    yield
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)


@pytest.fixture
def service():
    # Load sample data into test DB
    loader = CSVLoader(TEST_DB)
    import tempfile, csv
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        writer = csv.writer(f)
        writer.writerow(["name", "department", "salary"])
        writer.writerow(["Alice", "Engineering", 95000])
        writer.writerow(["Bob", "Marketing", 72000])
        writer.writerow(["Carol", "Engineering", 98000])
        tmp_path = f.name
    loader.load(tmp_path, table_name="employees")
    os.remove(tmp_path)
    return QueryService(TEST_DB)


# --- execute ---

def test_valid_select_all(service):
    result = service.execute("SELECT * FROM employees")
    assert result["success"] is True
    assert len(result["rows"]) == 3

def test_valid_select_column(service):
    result = service.execute("SELECT name FROM employees")
    assert result["success"] is True
    assert "name" in result["columns"]

def test_returns_correct_columns(service):
    result = service.execute("SELECT name, department FROM employees")
    assert result["columns"] == ["name", "department"]

def test_rejects_unknown_table(service):
    result = service.execute("SELECT * FROM ghost_table")
    assert result["success"] is False
    assert result["error"] != ""

def test_rejects_insert(service):
    result = service.execute("INSERT INTO employees VALUES (1, 'Eve', 'HR', 60000)")
    assert result["success"] is False

def test_rejects_drop(service):
    result = service.execute("DROP TABLE employees")
    assert result["success"] is False

def test_rejects_empty_query(service):
    result = service.execute("")
    assert result["success"] is False

def test_error_field_on_failure(service):
    result = service.execute("SELECT * FROM ghost_table")
    assert "error" in result
    assert result["error"] != ""

def test_rows_empty_on_failure(service):
    result = service.execute("SELECT * FROM ghost_table")
    assert result["rows"] == []

# --- get_tables ---

def test_get_tables(service):
    tables = service.get_tables()
    assert "employees" in tables

# --- get_schema ---

def test_get_schema(service):
    schema = service.get_schema("employees")
    names = [col["name"] for col in schema]
    assert "name" in names
    assert "department" in names