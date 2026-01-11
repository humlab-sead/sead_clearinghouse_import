
##  SEAD Clearinghouse Import
This folder contains `python` scripts that creates, uploads and processes an "CH complient" XML import file. The file must be a complete data submission prepared as an Excel file i.e. a file such as previously imported `Dendro archeologhy/buildning` and `Ceramics` submissions.

### Install
This program uses `tidlib` to cleanup the resulting XML which you can install using apt:

```bash
sudo apt-get install tidy
```

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
  source data is either an Excel file or an XML file that has previously been
  generated with this program.

  The content of the Excel file is processed and stored in an XML file that
  conforms to the clearinghouse data import schema.

  The Excel file must satisfy the following requirements:
    - The file must be in the Excel 2007+ format (xlsx)
    - The file must contain a sheet named as in SEAD' for each table in the submission.

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
  --xml-filename TEXT             Name of existing XML file to use.
  --log-folder TEXT               Name of existing XML file to use.
  --check-only                    Only check if file seems OK.
  --register / --no-register      Register file in the database.
  --explode / --no-explode        Explode XML into public tables.
  --tidy-xml / --no-tidy-xml      Run XML formatting tool on document.
  --timestamp / --no-timestamp    Add timestamp to target XML filename.
  --transfer-format TEXT          Specify format to use in upload (XML or
                                  CSV).
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
├── unit/              # Pure unit tests (40+ tests, ~0.2s)
│   ├── test_config.py
│   ├── test_fixtures.py
│   ├── test_policy.py
│   ├── test_to_csv.py
│   ├── test_utility.py
│   └── test_xml.py
├── integration/       # Integration tests (require DB)
│   ├── test_metadata.py
│   ├── test_process.py
│   └── test_submission.py
├── fixtures.py        # Reusable test fixtures
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
- Use full CSV fixtures from `tests/test_data/`
- Require live database connection
- Test complete workflows

See [tests/unit/README.md](tests/unit/README.md) and [tests/integration/README.md](tests/integration/README.md) for detailed guidelines.

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

Check for errors in file (don't create XML):

```bash
λ PYTHONPATH=. python importer/scripts/import-excel.py data/input/building_dendro_2023-12_import.xlsx --check-only --data-types dendrochronology
```

Generate XML and submit to clearinghouse:

```bash
λ PYTHONPATH=. python importer/scripts/import-excel.py data/input/building_dendro_2023-12_import.xlsx --data-types dendrochronology
```
