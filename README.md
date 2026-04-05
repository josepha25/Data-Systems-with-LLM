# Data-Systems-with-LLM

# Data Systems with LLM Interface

A modular system that loads structured data into SQLite and allows users to query it using natural language, powered by Claude AI.

## System Overview

The system has two independent flows:

**Ingestion flow:** CLI → CSV Loader → Schema Manager → SQLite

**Query flow:** CLI → Query Service → SQL Validator → SQLite

**Natural language flow:** CLI → Query Service → LLM Adapter → SQL Validator → SQLite

### Modules

- **CSV Loader** - Reads CSV files and inserts data into SQLite using raw SQL
- **Schema Manager** - Understands database structure, infers types, handles schema conflicts
- **SQL Validator** - Validates queries before execution (SELECT only, known tables/columns)
- **Query Service** - Orchestrates the query flow, CLI never touches the DB directly
- **LLM Adapter** - Translates natural language to SQL using Claude, output treated as untrusted
- **CLI** - Entry point only, all DB access goes through Query Service

## How to Run

### 1. Clone the repo
```bash
git clone https://github.com/josepha25/Data-Systems-with-LLM.git
cd Data-Systems-with-LLM
```

### 2. Set up environment
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3. Add your API key
```bash
cp .env.example .env
# Edit .env and add your Anthropic API key
```

### 4. Run the CLI
```bash
python3 -m modules.cli
```

### Available Commands

## How to Run Tests
```bash
pytest -v
```

## Design Decisions

- **CLI has no direct DB access** - enforces separation of concerns and makes the system testable
- **SQL Validator runs before every query** - LLM output is treated as untrusted input
- **Schema Manager is independent** - used by both ingestion and query flows
- **Tests written before implementation** - especially for the SQL Validator per project spec
- **No df.to_sql()** - schema creation and data insertion implemented manually for learning purposes