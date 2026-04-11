from modules.csv_loader import CSVLoader
from modules.query_service import QueryService
from modules.llm_adapter import LLMAdapter


def print_results(result: dict):
    """Pretty print query results as a table."""
    if not result["success"]:
        print(f"\n  Error: {result['error']}\n")
        return

    rows = result["rows"]
    columns = result["columns"]

    if not rows:
        print("\n  No results found.\n")
        return


    col_widths = [len(col) for col in columns]
    for row in rows:
        for i, val in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(val)))

    header = " | ".join(col.ljust(col_widths[i]) for i, col in enumerate(columns))
    separator = "-+-".join("-" * w for w in col_widths)
    print(f"\n  {header}")
    print(f"  {separator}")

    for row in rows:
        line = " | ".join(str(val).ljust(col_widths[i]) for i, val in enumerate(row))
        print(f"  {line}")

    print(f"\n  {len(rows)} row(s) returned.\n")


def run_cli(db_path: str = "database.db"):
    """Main CLI loop."""
    query_service = QueryService(db_path)
    csv_loader = CSVLoader(db_path)
    llm_adapter = None

    print("\n" + "="*50)
    print("  Data Systems with LLM Interface")
    print("="*50)
    print("  Commands:")
    print("  load <path>     - Load a CSV file")
    print("  tables          - List all tables")
    print("  schema <table>  - Show table schema")
    print("  sql <query>     - Run a SQL query directly")
    print("  ask <question>  - Ask in natural language")
    print("  exit            - Exit the program")
    print("="*50 + "\n")

    while True:
        try:
            user_input = input(">> ").strip()
        except (KeyboardInterrupt, EOFError):
            print("\nGoodbye!")
            break

        if not user_input:
            continue


        if user_input.lower() == "exit":
            print("Goodbye!")
            break


        elif user_input.lower().startswith("load "):
            path = user_input[5:].strip()
            try:
                result = csv_loader.load(path)
                print(f"\n  Loaded '{result['table_name']}' — "
                      f"{result['rows_inserted']} rows ({result['action']}).\n")
            except FileNotFoundError:
                print(f"\n  Error: File not found: {path}\n")
            except Exception as e:
                print(f"\n  Error: {e}\n")


        elif user_input.lower() == "tables":
            tables = query_service.get_tables()
            if tables:
                print("\n  Tables:")
                for t in tables:
                    print(f"    - {t}")
                print()
            else:
                print("\n  No tables found. Load a CSV first.\n")


        elif user_input.lower().startswith("schema "):
            table = user_input[7:].strip()
            schema = query_service.get_schema(table)
            if schema:
                print(f"\n  Schema for '{table}':")
                for col in schema:
                    pk = " (PK)" if col["pk"] else ""
                    print(f"    - {col['name']}: {col['type']}{pk}")
                print()
            else:
                print(f"\n  Table '{table}' not found.\n")


        elif user_input.lower().startswith("sql "):
            sql = user_input[4:].strip()
            result = query_service.execute(sql)
            print_results(result)


        elif user_input.lower().startswith("ask "):
            question = user_input[4:].strip()


            if llm_adapter is None:
                try:
                    llm_adapter = LLMAdapter()
                except ValueError as e:
                    print(f"\n  Error: {e}\n")
                    continue

            schema = query_service.get_all_schemas()
            if not schema:
                print("\n  No tables found. Load a CSV first.\n")
                continue

            print("\n  Translating your question...")
            translation = llm_adapter.translate(question, schema)

            if not translation["success"]:
                print(f"\n  Could not translate: {translation['error']}\n")
                continue

            print(f"  Generated SQL: {translation['sql']}")
            print(f"  Explanation:   {translation['explanation']}")

            result = query_service.execute(translation["sql"])
            print_results(result)

        else:
            print("\n  Unknown command. Type 'exit' to quit.\n")


if __name__ == "__main__":
    run_cli()