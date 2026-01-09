# AI Agent Instructions - SEAD Clearinghouse Import

## Project Purpose
Python system that transforms Excel data submissions into XML/CSV formats conforming to the SEAD ClearingHouse database schema, then uploads and processes them into PostgreSQL staging/public tables.

## Architecture Overview

### Core Workflow (3-stage pipeline)
1. **Load & Transform**: Excel → `Submission` object (via policies) → validated data
2. **Dispatch**: `Submission` → XML/CSV (via `IDispatcher` implementations)  
3. **Upload & Process**: Files → DB staging tables → explode to public schema

### Key Components
- **`Submission`** ([submission.py](importer/submission.py)): Wrapper for Excel data tables loaded as pandas DataFrames
- **`Metadata`** ([metadata.py](importer/metadata.py)): Database schema metadata (tables, columns, FKs, PKs) queried from PostgreSQL `information_schema`
- **`Policies`** ([policies.py](importer/policies.py)): Auto-registered data transformation rules applied to submissions (see Registry pattern below)
- **`Specifications`** ([specification.py](importer/specification.py)): Validation rules for data integrity
- **`ImportService`** ([process.py](importer/process.py)): Orchestrates the full workflow

### Data Model Conventions
- **`system_id`**: Internal temporary ID used during submission (required on all tables, must be unique per table)
- **`pk_name`**: The actual public database primary key (e.g., `site_id`, `dataset_id`)
- **New vs Existing Records**: Rows with null `pk_name` are new; non-null means updating existing records
- **Lookup Tables**: Small reference tables (e.g., `tbl_sample_types`) managed specially by policies

## Critical Patterns

### Registry Pattern
Policies and Specifications auto-register using decorators:
```python
@UpdatePolicies.register()
class AddPrimaryKeyColumnIfMissingPolicy(PolicyBase):
    def update(self) -> None:
        # Transformation logic here
```
- Policies execute in **priority order** (configured in `config.yml`)
- Can be disabled per-policy in config: `policies.<policy_id>.disabled: true`
- Policy IDs are snake_case class names (e.g., `add_primary_key_column_if_missing_policy`)

### Configuration Injection
Uses custom dependency injection via `ConfigValue` (see [inject.py](importer/configuration/inject.py)):
```python
class Options:
    ignore_columns: list[str] = ConfigValue("options:ignore_columns").resolve()
```
**Priority order**: CLI args → options file → environment vars (`SEAD_IMPORT_*`) → YAML config

Configuration files: 
- Project-wide: [config.yml](config.yml)
- Data-specific: [data/config.yml](data/config.yml)

## Developer Workflows

### Environment Setup
```bash
# Install uv if not already installed
curl -LsSf https://astral.sh/uv/install.sh | sh

# Clone and setup
git clone git@github.com:humlab-sead/sead_clearinghouse_import
cd sead_clearinghouse_import
uv sync  # or make install
```

Requirements:
- Python 3.12 (use pyenv)
- uv for dependency management
- External dependency: `tidy` CLI tool for XML formatting (`sudo apt-get install tidy`)
- Database credentials in `.env` or passed via CLI

### Running Import
```bash
# Standard workflow with all stages
PYTHONPATH=. python importer/scripts/import_excel.py \
  config.yml data/input/file.xlsx \
  --name "submission_name" \
  --output-folder output/ \
  --register --explode

# Check-only mode (validation without XML generation)
PYTHONPATH=. python importer/scripts/import_excel.py config.yml file.xlsx --check-only

# Using existing submission ID
PYTHONPATH=. python importer/scripts/import_excel.py config.yml 123 --name "existing"
```

### Testing
```bash
make test           # Fast tests (excludes @pytest.mark.long_running)
make full-test      # All tests including long-running
make test-coverage  # With HTML coverage report
```

### Code Quality
```bash
make tidy          # black + isort (line-length=120)
make lint          # pylint across importer/ and tests/
```

## Common Gotchas

### Excel File Requirements
- Must be `.xlsx` format (Excel 2007+)
- Each table requires a sheet with exact name matching metadata `excel_sheet` field
- Column names must match database schema (case-sensitive)
- Required columns: `system_id` (can be added by policy) + `pk_name` for the table

### Policy Execution
- Policies modify `Submission.data_tables` in-place
- Order matters! Check `config.yml` for priority settings
- Common policies:
  - `AddPrimaryKeyColumnIfMissingPolicy`: Adds PK column with nulls (assumes all new records)
  - `IfForeignKeyValueIsMissingAddIdentityMappingToForeignKeyTable`: Auto-creates referenced lookup records
  - `DropIgnoredColumns`: Removes columns matching patterns in `ignore_columns` config

### Foreign Key Management
- FK references use `system_id` during submission, not public PK values
- FK table must be included in submission or already exist in database
- Missing FK references can trigger auto-creation of lookup entries (depending on policy config)

## File Locations

### Input/Output
- Excel inputs: `data/<domain>/` (e.g., `data/dendro/`, `data/ceramics/`)
- Generated XML/CSV: `output/` (timestamped filenames by default)
- Logs: `logs/` (YAML format with timestamp prefix)

### Key Source Modules
- Entry point: [importer/scripts/import_excel.py](importer/scripts/import_excel.py)
- Dispatchers: [importer/dispatchers/](importer/dispatchers/) (XML generation logic)
- Uploaders: [importer/uploader/](importer/uploader/) (DB interaction)
- Tests: [tests/](tests/) (includes test submissions in `tests/submissions/`)

## Code Style Specifics
- Black formatting: 120 char line length, skip string normalization
- Use `loguru` for logging (not stdlib logging)
- Dataclasses preferred for config/options objects
- Type hints required (Python 3.12 syntax)
- Pandas DataFrames are primary data structure for table manipulation

## When Modifying Code

### Adding New Policies
1. Create class inheriting from `PolicyBase` in [policies.py](importer/policies.py)
2. Decorate with `@UpdatePolicies.register()`
3. Implement `update(self) -> None` method
4. Add priority in [config.yml](config.yml) under `policies.<policy_id>.priority`
5. Use `self.log(table_name, message)` for tracking changes

### Adding New Specifications
1. Create class inheriting from `SpecificationBase` in [specification.py](importer/specification.py)
2. Decorate with `@SpecificationRegistry.register()`
3. Implement `is_satisfied_by(self, submission: Submission, table_name: str) -> None`
4. Use `self.error()`, `self.warn()`, or `self.info()` to report issues

### Modifying Metadata Loading
- Metadata is loaded from PostgreSQL `information_schema` and cached
- Schema is defined in `clearing_house.clearinghouse_import_tables` and related views
- Changes to database schema require corresponding updates to metadata queries

## Debugging Tips

### Enable Detailed Logging
```python
# In scripts, logging is configured via config.yml
# Set level to DEBUG in config.yml:
logging:
  handlers:
    - sink: "sys.stdout"
      level: "DEBUG"
```

### Inspecting Submission Data
```python
# After loading, dump to CSV to inspect intermediate state
submission.to_csv(output_folder)
```

### Understanding Policy Execution Order
Check the priority values in [config.yml](config.yml) - lower numbers execute first.

## Package Management (uv)

This project uses `uv` instead of Poetry:
- Lock file: `uv.lock`
- Virtual environment: `.venv/`
- Add dependency: `uv add <package>`
- Remove dependency: `uv remove <package>`
- Update all: `uv sync --upgrade`

See [MIGRATION_UV.md](MIGRATION_UV.md) for details on the Poetry → uv migration.
