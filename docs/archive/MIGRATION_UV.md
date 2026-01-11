# Migration from Poetry to uv

This document summarizes the migration from Poetry to uv for the SEAD Clearinghouse Import project.

## What Changed

### Package Management
- **Before**: Poetry (`poetry.lock`)
- **After**: uv (`uv.lock`)

### Configuration Files

#### pyproject.toml
- Converted from Poetry's `[tool.poetry]` format to standard PEP 621 `[project]` format
- Changed build backend from `poetry-core` to `hatchling`
- Added `[tool.hatch.build.targets.wheel]` to specify package location
- Converted dependency specifications:
  - `python = "3.13.*"` → `requires-python = ">=3.13"`
  - `package = "^1.2.3"` → `package>=1.2.3"`
- Changed `psycopg2` to `psycopg2-binary` to avoid compilation issues
- **Updated**: Upgraded from `psycopg2-binary` to `psycopg[binary]>=3.1.0` (psycopg3)

#### Makefile
Updated all Poetry commands to uv equivalents:
- `poetry install` → `uv sync --all-extras`
- `poetry run <cmd>` → `uv run <cmd>`
- `poetry build` → `uv build`
- `poetry publish` → `uv publish`
- `poetry version patch` → `uv version patch`
- `poetry cache clear` → `uv cache clean`
- `poetry export` → `uv pip compile`

#### .gitignore
Updated to ignore uv-specific files:
- Added `.venv/`
- Added `uv.lock`
- Added `.python-version`

### Documentation Updates
- Updated README.md installation instructions
- Updated .github/copilot-instructions.md

## Command Equivalents

| Poetry Command | uv Equivalent |
|---------------|---------------|
| `poetry install` | `uv sync` or `make install` |
| `poetry add <package>` | `uv add <package>` |
| `poetry remove <package>` | `uv remove <package>` |
| `poetry run <command>` | `uv run <command>` |
| `poetry shell` | `source .venv/bin/activate` |
| `poetry lock` | `uv lock` |
| `poetry update` | `uv sync --upgrade` |

## Developer Workflow Changes

### Initial Setup
```bash
# Install uv if not already installed
curl -LsSf https://astral.sh/uv/install.sh | sh

# Clone and setup
git clone git@github.com:humlab-sead/sead_clearinghouse_import
cd sead_clearinghouse_import
uv sync  # or make install
```

### Running Tests
```bash
make test           # Fast tests
make full-test      # All tests
make test-coverage  # With coverage
```

### Code Quality
```bash
make tidy          # black + isort
make lint          # pylint
```

### Running Scripts
```bash
# Using uv run directly
uv run python importer/scripts/import_excel.py --help

# Or using the virtual environment
source .venv/bin/activate
python importer/scripts/import_excel.py --help
```

## Benefits of uv

1. **Speed**: Much faster dependency resolution and installation
2. **Drop-in replacement**: Works with existing PEP 621 pyproject.toml
3. **No additional configuration**: Uses standard Python packaging formats
4. **Better caching**: More efficient disk usage
5. **Active development**: Maintained by Astral (makers of Ruff)

## Migration Checklist

- [x] Convert pyproject.toml to PEP 621 format
- [x] Update build backend to hatchling
- [x] Update Makefile commands
- [x] Update README.md
- [x] Update .gitignore
- [x] Update .github/copilot-instructions.md
- [x] Generate uv.lock file
- [x] Verify make targets work
- [x] Test that dev tools work (pytest, black, pylint)

## Rollback (if needed)

If you need to rollback to Poetry:
1. `git checkout main -- pyproject.toml Makefile README.md .gitignore`
2. `poetry install`
3. Delete `.venv` and `uv.lock` if created by uv

## Notes

- The old `poetry.lock` file is still in the repository but is no longer used
- uv creates its virtual environment in `.venv/` by default (same as Poetry)
- All existing make targets continue to work with the same syntax
