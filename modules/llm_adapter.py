import os
import anthropic
from dotenv import load_dotenv

load_dotenv()


class LLMAdapter:
    # sends user question to Claude and gets SQL back

    def __init__(self):
        # load API key
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            raise ValueError("ANTHROPIC_API_KEY not found in environment.")
        self.client = anthropic.Anthropic(api_key=api_key)

    def translate(self, user_query: str, schema: dict) -> dict:
        # takes a plain english question and returns SQL
        if not user_query or not user_query.strip():
            return {"success": False, "sql": "", "explanation": "", "error": "Empty query."}

        prompt = self._build_prompt(user_query, schema)

        try:
            message = self.client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=1024,
                messages=[{"role": "user", "content": prompt}]
            )
            raw = message.content[0].text
            return self._parse_response(raw)

        except Exception as e:
            return {"success": False, "sql": "", "explanation": "", "error": str(e)}

    def _build_prompt(self, user_query: str, schema: dict) -> str:
        # builds the prompt with the database schema so Claude knows what tables exist
        schema_description = ""
        for table, columns in schema.items():
            col_names = ", ".join(
                col["name"] if isinstance(col, dict) else col
                for col in columns
            )
            schema_description += f"- {table} ({col_names})\n"

        return f"""You are an AI assistant that converts natural language questions into SQLite SQL queries.

The database contains the following tables:
{schema_description}
User question: "{user_query}"

Your task:
1. Generate a valid SQLite SELECT query that answers the question.
2. Only use tables and columns that exist in the schema above.
3. Only generate SELECT queries, never INSERT, UPDATE, DELETE, or DROP.
4. Provide a short explanation of what the query does.

Respond in exactly this format:
SQL: <your sql query here>
EXPLANATION: <your explanation here>"""

    def _parse_response(self, raw: str) -> dict:
        # pulls the SQL and explanation out of Claude's response
        sql = ""
        explanation = ""

        for line in raw.strip().splitlines():
            if line.startswith("SQL:"):
                sql = line[4:].strip()
            elif line.startswith("EXPLANATION:"):
                explanation = line[12:].strip()

        if not sql:
            return {
                "success": False,
                "sql": "",
                "explanation": "",
                "error": f"Could not parse SQL from response: {raw}"
            }

        return {"success": True, "sql": sql, "explanation": explanation, "error": ""}