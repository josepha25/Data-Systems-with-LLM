import pytest
import sqlite3
import os
import pandas as pd
from modules.schema_manager import SchemaManager

TEST_DB = "test_schema.db"


@pytest.fixture(autouse=True)
def cleanup():
    """Remove test database before and after each test."""
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)
    yield
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)


@pytest.fixture
def manager():
    return SchemaManager(TEST_DB)


@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "name": ["Alice", "Bob"],
        "department": ["Engineering", "Marketing"],
        "salary": [95000, 72000]
    })


# --- get_tables ---

def test_get_tables_empty(manager):
    assert manager.get_tables() == []

def test_get_tables_after_create(manager, sample_df):
    manager.resolve_table("employees", sample_df)
    assert "employees" in manager.get_tables()


# --- get_schema ---

def test_get_schema_unknown_table(manager):
    assert manager.get_schema("ghost") == []

def test_get_schema_has_id_column(manager, sample_df):
    manager.resolve_table("employees", sample_df)
    schema = manager.get_schema("employees")
    names = [col["name"] for col in schema]
    assert "id" in names

def test_get_schema_has_correct_columns(manager, sample_df):
    manager.resolve_table("employees", sample_df)
    schema = manager.get_schema("employees")
    names = [col["name"] for col in schema]
    assert "name" in names
    assert "department" in names
    assert "salary" in names


# --- get_columns ---

def test_get_columns(manager, sample_df):
    manager.resolve_table("employees", sample_df)
    cols = manager.get_columns("employees")
    assert "name" in cols
    assert "id" in cols


# --- resolve_table ---

def test_resolve_table_creates_new(manager, sample_df):
    action = manager.resolve_table("employees", sample_df)
    assert action == "created"

def test_resolve_table_appends_matching(manager, sample_df):
    manager.resolve_table("employees", sample_df)
    action = manager.resolve_table("employees", sample_df)
    assert action == "appended"

def test_resolve_table_conflict_creates_v2(manager, sample_df):
    manager.resolve_table("employees", sample_df)
    different_df = pd.DataFrame({
        "name": ["Carol"],
        "age": [30]
    })
    manager.resolve_table("employees", different_df)
    assert "employees_v2" in manager.get_tables()


# --- type inference ---

def test_integer_type_inference(manager):
    df = pd.DataFrame({"count": [1, 2, 3]})
    manager.resolve_table("counts", df)
    schema = manager.get_schema("counts")
    col = next(c for c in schema if c["name"] == "count")
    assert col["type"] == "INTEGER"

def test_text_type_inference(manager):
    df = pd.DataFrame({"name": ["Alice"]})
    manager.resolve_table("names", df)
    schema = manager.get_schema("names")
    col = next(c for c in schema if c["name"] == "name")
    assert col["type"] == "TEXT"