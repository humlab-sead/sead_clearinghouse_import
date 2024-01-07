
##  SEAD Clearinghouse Import
This folder contains `python` scripts that creates, uploads and processes an "CH complient" XML import file. The file must be a complete data submission prepared as an Excel file i.e. a file such as previously imported `Dendro archeologhy/buildning` and `Ceramics` submissions.

### Install
This program uses `tidlib` to cleanup the resulting XML which you can install using apt:

```bash
sudo apt-get install tidy
```

You will need Python ^3.11 (install with) and Poetry on your local machine.

Clone the SEAD Clearinghouse import repository into a new folder and setup the local environment:

```bash
git clone git@github.com:humlab-sead/sead_clearinghouse_import
cd sead_clearinghouse_import
poetry install
```
### Usage

```bash
λ PYTHONPATH=. python importer/scripts/import-excel.py --help
Usage: import-excel.py [OPTIONS] FILENAME

  Imports an Excel file to the database. The Excel file is stored as an XML
  file conforming to the clearinghouse data import XML schema. The Excel file
  must satisfy the following requirements: - The file must be in the Excel
  2007+ format (xlsx) - The file must contain a sheet named `data_table_index'
  listing all tables in the submission having new or changed data. - The file
  must contain a sheet named as in SEADe' for each table in the submission.

Options:
  -t, --data-types TEXT  Types of data (short description)  [required]
  --output-folder TEXT   Output folder
  -h, --host TEXT        Target database server
  -d, --database TEXT    Database name
  -u, --user TEXT        Database user
  --port INTEGER         Server port number.
  --skip                 Skip the import (do nothing)
  --id INTEGER           Replace existing submission.
  --table-names TEXT     Only load specified tables.
  --xml-filename TEXT    Name of existing XML file to use.
  --log-folder TEXT      Name of existing XML file to use.
  --check-only           Only check if file seems OK.
  --help                 Show this message and exit.
```

### Configuration

The import expects that user's password is stored in environment variable "SEAD_CH_PASSWORD".

### Examples

Check for errors in file (don't create XML):

```bash
λ PYTHONPATH=. python importer/scripts/import-excel.py data/input/building_dendro_2023-12_import.xlsx --check-only --data-types dendrochronology
```

Generate XML and submit to clearinghouse:

```bash
λ PYTHONPATH=. python importer/scripts/import-excel.py data/input/building_dendro_2023-12_import.xlsx --data-types dendrochronology
```

2024-01-07 10:04:31.981 | Level 30 | importer.model.specification:log_messages:98 - WARNING type clash: tbl_dendro_dates.age_older integer<=>object
2024-01-07 10:04:31.981 | Level 30 | importer.model.specification:log_messages:98 - WARNING type clash: tbl_physical_samples.sample_name character varying<=>int64
2024-01-07 10:04:31.982 | Level 30 | importer.model.specification:log_messages:98 - WARNING! Column tbl_dendro_dates.season_id: ends with "_id" but NOT marked as PK/FK
2024-01-07 10:04:31.982 | Level 30 | importer.model.specification:log_messages:98 - WARNING tbl_dendro_dates has EXTRA DATA columns: error_uncertainty_id
2024-01-07 10:04:31.982 | Level 30 | importer.model.specification:log_messages:98 - WARNING tbl_sample_group_coordinates has EXTRA DATA columns: sample_group_name
2024-01-07 10:04:31.983 | Level 30 | importer.model.specification:log_messages:98 - WARNING tbl_sample_groups has EXTRA DATA columns: sample_group_id.1
2024-01-07 10:04:31.983 | Level 30 | importer.model.specification:log_messages:98 - WARNING tbl_site_references has EXTRA DATA columns: Projektnr
2024-01-07 10:04:31.983 | Level 40 | importer.model.specification:log_messages:98 - CRITICAL ERROR Column tbl_dendro_dates.age_older has non-numeric values: Winter X After X