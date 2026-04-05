import re
import sqlparse
from sqlparse.tokens import Keyword, DML


class SQLValidator:
    """
    Validates SQL queries at the structure level.
    Only SELECT queries against known tables/columns are allowed.
    """

    def __init__(self, schema: dict):
        """
        Args:
            schema: dict mapping table_name -> list of column names
        """
        self.schema = {k.lower(): [c.lower() for c in v] for k, v in schema.items()}
        self.last_error = ""

    def validate(self, sql: str) -> bool:
        """
        Validate a SQL query.
        Returns True if safe and valid, False otherwise.
        Sets self.last_error with a descriptive message on failure.
        """
        self.last_error = ""

        # Reject empty queries
        if not sql or not sql.strip():
            self.last_error = "Empty query."
            return False

        # Reject multi-statement queries (semicolon injection)
        clean = sql.strip().rstrip(";")
        if ";" in clean:
            self.last_error = "Multiple statements are not allowed."
            return False

        parsed = sqlparse.parse(clean)
        if not parsed:
            self.last_error = "Could not parse query."
            return False

        statement = parsed[0]

        # Check query type — only SELECT allowed
        query_type = self._get_query_type(statement)
        if query_type not in ("SELECT",):
            self.last_error = f"Query type '{query_type}' is not allowed. Only SELECT is permitted."
            return False

        # Check tables exist
        tables = self._extract_tables(clean)
        for table in tables:
            if table not in self.schema:
                self.last_error = f"unknown table: '{table}'."
                return False

        # Check columns exist (skip * wildcards)
        columns = self._extract_columns(statement)
        all_valid_columns = {col for cols in self.schema.values() for col in cols}
        for col in columns:
            if col not in all_valid_columns:
                self.last_error = f"unknown column: '{col}'."
                return False

        return True

    def _get_query_type(self, statement) -> str:
        for token in statement.tokens:
            if token.ttype is DML:
                return token.normalized.upper()
        return "UNKNOWN"

    def _extract_tables(self, sql: str) -> list:
        """Extract table names from FROM and JOIN clauses."""
        pattern = r'\b(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*)'
        matches = re.findall(pattern, sql, re.IGNORECASE)
        return [m.lower() for m in matches]

    def _extract_columns(self, statement) -> list:
        """
        Extract column names from SELECT clause.
        Returns empty list if SELECT * is used.
        """
        columns = []
        select_seen = False

        for token in statement.flatten():
            if token.ttype is DML and token.normalized.upper() == "SELECT":
                select_seen = True
                continue
            if select_seen:
                if token.ttype is Keyword:
                    break
                val = token.value.strip().lower()
                if val and val not in ("*", ",", " ", ""):
                    if re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', val):
                        columns.append(val)

        return columns