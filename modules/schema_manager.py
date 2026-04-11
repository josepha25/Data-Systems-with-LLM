import sqlite3
import pandas as pd
import logging

# log errors to a file
logging.basicConfig(filename="error_log.txt", level=logging.ERROR,
                    format="%(asctime)s - %(levelname)s - %(message)s")

# maps pandas types to SQLite types
DTYPE_MAP = {
    "int64": "INTEGER",
    "float64": "REAL",
    "bool": "INTEGER",
    "object": "TEXT",
    "datetime64[ns]": "TEXT",
}


class SchemaManager:
    # keeps track of the database structure

    def __init__(self, db_path: str):
        self.db_path = db_path

    def get_tables(self) -> list:
        # returns all table names
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            return [row[0] for row in cursor.fetchall()]
        finally:
            conn.close()

    def get_schema(self, table_name: str) -> list:
        # returns columns and types for a table
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute(f"PRAGMA table_info({table_name})")
            rows = cursor.fetchall()
            if not rows:
                return []
            return [
                {"name": r[1], "type": r[2], "notnull": bool(r[3]), "pk": bool(r[5])}
                for r in rows
            ]
        finally:
            conn.close()

    def get_all_schemas(self) -> dict:
        # returns schemas for all tables
        return {t: self.get_schema(t) for t in self.get_tables()}

    def get_columns(self, table_name: str) -> list:
        # returns just the column names
        return [col["name"] for col in self.get_schema(table_name)]

    def resolve_table(self, table_name: str, df: pd.DataFrame) -> str:
        # decides whether to create a new table or append to existing one
        existing_tables = self.get_tables()

        if table_name not in existing_tables:
            self._create_table(table_name, df)
            return "created"

        existing_schema = self.get_schema(table_name)
        if self._schemas_match(existing_schema, df):
            return "appended"

        # schema conflict, create a new table with _v2
        new_name = f"{table_name}_v2"
        logging.error(
            f"Schema conflict for table '{table_name}'. "
            f"Creating new table '{new_name}'."
        )
        self._create_table(new_name, df)
        return "created"

    def _infer_sqlite_type(self, dtype) -> str:
        # converts pandas dtype to SQLite type
        return DTYPE_MAP.get(str(dtype), "TEXT")

    def _create_table(self, table_name: str, df: pd.DataFrame):
        # creates a table with an auto id column plus columns from the CSV
        col_defs = ["id INTEGER PRIMARY KEY AUTOINCREMENT"]
        for col, dtype in zip(df.columns, df.dtypes):
            sqlite_type = self._infer_sqlite_type(dtype)
            col_defs.append(f"{col} {sqlite_type}")

        ddl = f"CREATE TABLE IF NOT EXISTS {table_name} (\n  "
        ddl += ",\n  ".join(col_defs)
        ddl += "\n);"

        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute(ddl)
            conn.commit()
        finally:
            conn.close()

    def _schemas_match(self, existing: list, df: pd.DataFrame) -> bool:
        # checks if the CSV columns match the existing table
        existing_cols = {
            col["name"]: col["type"]
            for col in existing
            if col["name"] != "id"
        }
        csv_cols = {
            col: self._infer_sqlite_type(dtype)
            for col, dtype in zip(df.columns, df.dtypes)
        }
        return existing_cols == csv_cols