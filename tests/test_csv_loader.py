import pytest
import os
import pandas as pd
from modules.csv_loader import CSVLoader

TEST_DB = "test_csv.db"
TEST_CSV = "test_data.csv"


@pytest.fixture(autouse=True)
def cleanup():
    """Remove test files before and after each test."""
    for f in [TEST_DB, TEST_CSV]:
        if os.path.exists(f):
            os.remove(f)
    yield
    for f in [TEST_DB, TEST_CSV]:
        if os.path.exists(f):
            os.remove(f)


@pytest.fixture
def loader():
    return CSVLoader(TEST_DB)


@pytest.fixture
def sample_csv():
    df = pd.DataFrame({
        "name": ["Alice", "Bob"],
        "department": ["Engineering", "Marketing"],
        "salary": [95000, 72000]
    })
    df.to_csv(TEST_CSV, index=False)
    return TEST_CSV


# --- load ---

def test_load_returns_correct_table_name(loader, sample_csv):
    result = loader.load(sample_csv, table_name="employees")
    assert result["table_name"] == "employees"

def test_load_returns_correct_row_count(loader, sample_csv):
    result = loader.load(sample_csv, table_name="employees")
    assert result["rows_inserted"] == 2

def test_load_action_is_created(loader, sample_csv):
    result = loader.load(sample_csv, table_name="employees")
    assert result["action"] == "created"

def test_load_action_is_appended(loader, sample_csv):
    loader.load(sample_csv, table_name="employees")
    result = loader.load(sample_csv, table_name="employees")
    assert result["action"] == "appended"

def test_load_file_not_found(loader):
    with pytest.raises(FileNotFoundError):
        loader.load("nonexistent.csv")

def test_load_default_table_name(loader, sample_csv):
    result = loader.load(sample_csv)
    assert result["table_name"] == "test_data"

def test_load_columns_normalized(loader, sample_csv):
    loader.load(sample_csv, table_name="employees")
    from modules.schema_manager import SchemaManager
    sm = SchemaManager(TEST_DB)
    cols = sm.get_columns("employees")
    assert "name" in cols
    assert "department" in cols