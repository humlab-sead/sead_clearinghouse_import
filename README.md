
##  SEAD Clearinghouse Import
This folder contains `python` scripts that create, upload and process CSV import files for the SEAD ClearingHouse database. The source data must be a complete data submission prepared as an Excel file (e.g., previously imported `Dendro archaeology/building` and `Ceramics` submissions).

### Install

You will need Python ^3.13 (install with uv) and uv on your local machine.

Install uv if you haven't already:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Clone the SEAD Clearinghouse import repository into a new folder and setup the local environment:

```bash
git clone git@github.com:humlab-sead/sead_clearinghouse_import
cd sead_clearinghouse_import
uv sync
```
### Usage

```bash
 λ PYTHONPATH=. python importer/scripts/import_excel.py --help
Usage: import_excel.py [OPTIONS] CONFIG_FILENAME FILENAME

  Imports a new SEAD data submission to the SEAD ClearingHouse database. The
  source data is an Excel file that is processed and converted to CSV format.

  The content of the Excel file is processed and stored in CSV files that
  conform to the clearinghouse data import schema.

  The Excel file must satisfy the following requirements:
    - The file must be in the Excel 2007+ format (xlsx)
    - The file must contain a sheet named as in SEAD for each table in the submission.

Options:
  --options-filename TEXT         Name of options file to use (alternative to
                                  CLI options).
  -t, --data-types TEXT           Types of data (short description)
  -n, --name TEXT                 Unique name of submission (use CR name)
                                  [required]
  --output-folder TEXT            Output folder  [required]
  -h, --host TEXT                 Target database server
  -d, --database TEXT             Database name
  -u, --user TEXT                 Database user
  -p, --port INTEGER              Server port number.
  --skip                          Skip the import (do nothing)
  --id INTEGER                    Replace existing submission.
  --table-names TEXT              Only load specified tables.
  --log-folder TEXT               Where log files are stored.
  --check-only                    Only check if file seems OK.
  --register / --no-register      Register file in the database.
  --explode / --no-explode        Explode CSV into public tables.
  --timestamp / --no-timestamp    Add timestamp to target CSV filename.
  --dump-to-csv / --no-dump-to-csv
                                  Store (policy-updated) submission data as
                                  CSV files in output folder.
  --help                          Show this message and exit.
```

### Configuration

The import program reads configuration in the following order of priority:

1. Command line arguments have the highest priority and supercedes other configurations.
2. Command line arguments found in options file (--options-filename FILENAME)
3. Environment variables having prefix "SEAD_IMPORT_ABC_DEF" corresponding to setting "options"[abc].def.
4. Values in "options" section in YAML configuration file (argument)

## Testing

The test suite is organized into **unit tests** (fast, no database required) and **integration tests** (require database connection).

### Test Structure

```
tests/
├── unit/              # Pure unit tests (100+ tests, ~0.3s)
│   ├── test_config.py
│   ├── test_csv_dispatcher.py
│   ├── test_csv_dispatcher_edge_cases.py
│   ├── test_fixtures.py
│   ├── test_metadata_edge_cases.py
│   ├── test_policy.py
│   ├── test_specification_edge_cases.py
│   ├── test_to_csv.py
│   └── test_utility.py
├── integration/       # Integration tests (require DB)
│   ├── test_metadata.py
│   ├── test_process.py
│   └── test_submission.py
├── conftest.py        # Pytest configuration and fixtures
└── builders.py        # Factory functions for test data
```

### Running Tests

```bash
# Run only unit tests (fast, no DB required)
make test
# or
pytest tests/unit/

# Run integration tests (requires database)
make test-integration
# or
pytest tests/integration/

# Run all tests
make full-test
# or
pytest tests/

# Run with coverage report
make test-coverage        # Unit tests only
make test-coverage-full   # All tests

# Open coverage report
open htmlcov/index.html
```

### Writing Tests

**Unit Tests** (tests/unit/):
- Use fixtures from `tests/fixtures.py` for common test data
- Use builders from `tests/builders.py` for custom schemas
- No database required - use `MockSchemaService`
- Fast execution (milliseconds per test)

**Integration Tests** (tests/integration/):
- Mark with `@pytest.mark.integration` decorator
- Use minimal fixtures from `tests/builders.py` for fast execution
- Require live database connection only when needed
- Test complete workflows

See test files for detailed examples and patterns.

## Development

### Code Quality

```bash
# Format code
make tidy          # black + isort

# Lint code
make lint          # pylint + ruff

# Type checking (if configured)
make typecheck     # mypy
```

### Package Management

This project uses `uv` for dependency management:

```bash
# Install dependencies
uv sync --all-extras

# Add a dependency
uv add <package>

# Add a dev dependency
uv add --dev <package>

# Update dependencies
uv sync --upgrade
```


### Examples

Check for errors in file (don't create CSV):

```bash
λ PYTHONPATH=. python importer/scripts/import_excel.py config.yml data/input/building_dendro_2023-12_import.xlsx --check-only --data-types dendrochronology --name "dendro_2023_12" --output-folder output/
```

Generate CSV and submit to clearinghouse:

```bash
λ PYTHONPATH=. python importer/scripts/import_excel.py config.yml data/input/building_dendro_2023-12_import.xlsx --data-types dendrochronology --name "dendro_2023_12" --output-folder output/ --register --explode
```
