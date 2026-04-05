import sqlite3
from modules.schema_manager import SchemaManager
from modules.sql_validator import SQLValidator


class QueryService:
    """
    Orchestrates the query flow.
    CLI -> QueryService -> SQLValidator -> Database
    Does NOT call the LLM directly (that's the LLM Adapter's job).
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.schema_manager = SchemaManager(db_path)

    def execute(self, sql: str) -> dict:
        """
        Validate and execute a SQL query.

        Args:
            sql: SQL string to validate and execute.

        Returns:
            dict with keys:
                - success (bool)
                - rows (list of tuples) if success
                - columns (list of str) if success
                - error (str) if not success
        """
        # Build validator from current schema
        schema = self._get_schema_for_validator()
        validator = SQLValidator(schema=schema)

        # Validate first
        if not validator.validate(sql):
            return {
                "success": False,
                "error": validator.last_error,
                "rows": [],
                "columns": []
            }

        # Execute
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
        """Return all table names in the database."""
        return self.schema_manager.get_tables()

    def get_schema(self, table_name: str) -> list:
        """Return schema for a specific table."""
        return self.schema_manager.get_schema(table_name)

    def get_all_schemas(self) -> dict:
        """Return all schemas. Used by LLM Adapter."""
        return self.schema_manager.get_all_schemas()

    def _get_schema_for_validator(self) -> dict:
        """Build schema dict in the format SQLValidator expects."""
        return {
            table: self.schema_manager.get_columns(table)
            for table in self.schema_manager.get_tables()
        }