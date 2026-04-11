import pandas as pd
import sqlite3
import os
from modules.schema_manager import SchemaManager


class CSVLoader:
    # reads a CSV and puts the data into the database

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.schema_manager = SchemaManager(db_path)

    def load(self, csv_path: str, table_name: str = None) -> dict:
        # loads a CSV file into the database
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        # read and normalize column names
        df = pd.read_csv(csv_path)
        df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

        # use filename as table name if not specified
        if table_name is None:
            table_name = os.path.splitext(os.path.basename(csv_path))[0].lower()

        action = self.schema_manager.resolve_table(table_name, df)
        rows_inserted = self._insert_rows(df, table_name)

        return {
            "table_name": table_name,
            "rows_inserted": rows_inserted,
            "action": action
        }

    def _insert_rows(self, df: pd.DataFrame, table_name: str) -> int:
        # inserts rows using parameterized SQL, no df.to_sql()
        columns = ", ".join(df.columns)
        placeholders = ", ".join(["?" for _ in df.columns])
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            rows = [tuple(row) for row in df.itertuples(index=False, name=None)]
            cursor.executemany(sql, rows)
            conn.commit()
            return cursor.rowcount
        finally:
            conn.close()