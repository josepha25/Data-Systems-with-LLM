import sqlite3
from modules.schema_manager import SchemaManager
from modules.sql_validator import SQLValidator


class QueryService:
    # middleman between the CLI and the database

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.schema_manager = SchemaManager(db_path)

    def execute(self, sql: str) -> dict:
        # validates then runs a SQL query
        schema = self._get_schema_for_validator()
        validator = SQLValidator(schema=schema)

        # reject if invalid
        if not validator.validate(sql):
            return {
                "success": False,
                "error": validator.last_error,
                "rows": [],
                "columns": []
            }

        # run it
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(sql)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            conn.close()
            return {
                "success": True,
                "rows": rows,
                "columns": columns,
                "error": ""
            }
        except sqlite3.Error as e:
            return {
                "success": False,
                "error": str(e),
                "rows": [],
                "columns": []
            }

    def get_tables(self) -> list:
        # returns all table names
        return self.schema_manager.get_tables()

    def get_schema(self, table_name: str) -> list:
        # returns schema for one table
        return self.schema_manager.get_schema(table_name)

    def get_all_schemas(self) -> dict:
        # returns all schemas, used by the LLM adapter
        return self.schema_manager.get_all_schemas()

    def _get_schema_for_validator(self) -> dict:
        # builds schema in the format the validator expects
        return {
            table: self.schema_manager.get_columns(table)
            for table in self.schema_manager.get_tables()
        }