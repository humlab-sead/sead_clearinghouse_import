# SEAD Clearinghouse Import - Archival Notice

**Status**: 🗃️ **ARCHIVED** (2026-01-12)

This repository has been **superseded** by the SEAD ingester in the Shape Shifter project.

## Migration Status

All code, tests, and documentation have been migrated to:
**https://github.com/humlab-sead/sead_shape_shifter** → `ingesters/sead/`

### What Was Migrated

✅ **Code** (11 modules):
- Core modules: `metadata.py`, `policies.py`, `specification.py`, `submission.py`, `process.py`, `repository.py`, `utility.py`
- Dispatchers: `dispatchers/to_csv.py`
- Uploaders: `uploader/csv_uploader.py`
- Main ingester: `ingester.py` (new - implements `Ingester` protocol)

✅ **Tests** (20 test files):
- Unit tests: `tests/unit/` (12 files) → `ingesters/sead/tests/unit/`
- Integration tests: `tests/integration/` (4 files) → `ingesters/sead/tests/integration/`
- Test infrastructure: `conftest.py`, `builders.py`, `fixtures.py`, `utility.py`
- Test data: `tests/test_data/` → `ingesters/sead/tests/test_data/`

✅ **Documentation**:
- Architecture guide: `docs/ARCHITECTURE.md` → `ingesters/sead/ARCHITECTURE.md`
- User guide: `README.md` content merged into `ingesters/sead/README.md`
- Integration guide: Content merged into Shape Shifter documentation

### New Location in Shape Shifter

```
sead_shape_shifter/
└── ingesters/
    └── sead/                    # SEAD Clearinghouse Ingester
        ├── dispatchers/         # CSV generation
        ├── uploader/            # Database upload
        ├── tests/               # ✨ NEW: Full test suite
        │   ├── unit/            # Unit tests (fast, no DB)
        │   ├── integration/     # Integration tests
        │   └── test_data/       # Test fixtures
        ├── ingester.py          # Main ingester (protocol implementation)
        ├── metadata.py
        ├── policies.py
        ├── specification.py
        ├── submission.py
        ├── process.py
        ├── repository.py
        ├── utility.py
        ├── README.md            # ✨ NEW: Complete user guide
        └── ARCHITECTURE.md      # ✨ NEW: Architecture documentation
```

## Why Archived?

The SEAD ingester functionality has been integrated into the **Shape Shifter** project as a plugin-based ingester:

1. **Better Architecture**: Protocol-based ingester system with dynamic discovery
2. **Reusability**: Ingester can be used independently or via Shape Shifter API
3. **Maintainability**: All SEAD-specific code in one self-contained module
4. **Testing**: Tests now live with implementation for better cohesion
5. **Documentation**: Comprehensive user and architecture docs alongside code

## Migration for Users

### Old Usage (sead_clearinghouse_import)

```bash
cd sead_clearinghouse_import
PYTHONPATH=. python importer/scripts/import_excel.py \
  config.yml data.xlsx \
  --name "submission" \
  --data-types "dendro" \
  --register --explode
```

### New Usage (Shape Shifter)

```bash
cd sead_shape_shifter

# Via CLI
python -m backend.app.scripts.ingest ingest sead data.xlsx \
  --submission-name "submission" \
  --data-types "dendro" \
  --config config.json \
  --register --explode

# Or via Python API
from ingesters.sead.ingester import SeadIngester
from backend.app.ingesters.protocol import IngesterConfig

config = IngesterConfig(
    host="localhost",
    port=5432,
    dbname="sead_staging",
    user="sead_user",
    submission_name="submission",
    data_types="dendro",
)
ingester = SeadIngester(config)
result = ingester.ingest("data.xlsx")
```

## What Remains in This Repository (for historical reference)

- Original development history
- Legacy scripts and utilities
- Configuration examples (`configs/`, `data/`)
- CSV output examples (`csv_files/`)
- Historical documentation (`docs/archive/`)

## For Developers

If you need to work with SEAD data imports, please use the **Shape Shifter** project:

### Setup
```bash
git clone git@github.com:humlab-sead/sead_shape_shifter.git
cd sead_shape_shifter
make install
```

### Run Tests
```bash
# All SEAD ingester tests
uv run pytest ingesters/sead/tests/

# Unit tests only (fast)
uv run pytest ingesters/sead/tests/unit/

# Integration tests (requires DB)
uv run pytest ingesters/sead/tests/integration/
```

### Documentation
- User Guide: `ingesters/sead/README.md`
- Architecture: `ingesters/sead/ARCHITECTURE.md`
- Protocol: `backend/app/ingesters/README.md`
- Main Project: `README.md`

## Timeline

- **2020-2024**: Active development in `sead_clearinghouse_import`
- **Jan 2026**: Migration to Shape Shifter as ingester plugin
- **Jan 12, 2026**: Repository archived

## Questions?

For questions about:
- **SEAD ingester functionality**: See Shape Shifter repository
- **Migration support**: Create issue in Shape Shifter
- **Historical questions**: This archived repository remains available for reference

---

**Repository**: https://github.com/humlab-sead/sead_clearinghouse_import  
**Superseded by**: https://github.com/humlab-sead/sead_shape_shifter (ingesters/sead/)  
**Archived**: 2026-01-12
