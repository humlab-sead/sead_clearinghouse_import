
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
 λ PYTHONPATH=. python importer/scripts/import_excel.py --help
Usage: import_excel.py [OPTIONS] FILENAME

  FILENAME is either an Excel file, or a previously generated XML file.

  Imports an Excel file to the database. The content of the Excel file is
  processed and stored as an XML  file conforming to the clearinghouse data
  import XML schema. The Excel file
  must satisfy the following requirements: - The file must be in the Excel
  2007+ format (xlsx) -
  
Options:
  -t, --data-types TEXT         Types of data (short description)  [required]
  --output-folder TEXT          Output folder
  -h, --host TEXT               Target database server
  -d, --database TEXT           Database name
  -u, --user TEXT               Database user
  --port INTEGER                Server port number.
  --skip                        Skip the import (do nothing)
  --id INTEGER                  Replace existing submission.
  --table-names TEXT            Only load specified tables.
  --xml-filename TEXT           Name of existing XML file to use.
  --log-folder TEXT             Name of existing XML file to use.
  --check-only                  Only check if file seems OK.
  --register / --no-register    Register file in the database.
  --explode / --no-explode      Explode XML into public tables.
  --tidy-xml / --no-tidy-xml    Run XML formatting tool on document.
  --timestamp / --no-timestamp  Add timestamp to target XML filename.
  --help                        Show this message and exit.

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
  6038	Winter X
  6039	Winter X
  6040	Winter X
  6130	After X
  6131	After X

2024-01-07 10:04:31.982 | Level 30 | importer.model.specification:log_messages:98 - WARNING! Column tbl_dendro_dates.season_id: ends with "_id" but NOT marked as PK/FK
  Add FK constraint

2024-01-07 10:04:31.981 | Level 30 | importer.model.specification:log_messages:98 - WARNING type clash: tbl_physical_samples.sample_name character varying<=>int64
  Non-critical, should be OK

2024-01-07 10:04:31.982 | Level 30 | importer.model.specification:log_messages:98 - WARNING tbl_dendro_dates has EXTRA DATA columns: error_uncertainty_id
  Non-critical, should be OK

2024-01-07 10:04:31.982 | Level 30 | importer.model.specification:log_messages:98 - WARNING tbl_sample_group_coordinates has EXTRA DATA columns: sample_group_name
  Non-critical, should be OK

2024-01-07 10:04:31.983 | Level 30 | importer.model.specification:log_messages:98 - WARNING tbl_sample_groups has EXTRA DATA columns: sample_group_id.1
  Non-critical, should be OK

2024-01-07 10:04:31.983 | Level 30 | importer.model.specification:log_messages:98 - WARNING tbl_site_references has EXTRA DATA columns: Projektnr
  Non-critical, should be OK

2024-01-07 10:04:31.983 | Level 40 | importer.model.specification:log_messages:98 - CRITICAL ERROR Column tbl_dendro_dates.age_older has non-numeric values: Winter X After X
  Criticalerror, same cause as above
  6038	Winter X
  6039	Winter X
  6040	Winter X
  6130	After X
  6131	After X


ERROR   Non-nullable foreign key column tbl_dendro_dates.age_type_id                 has missing values
WARNING Non-nullable foreign key column tbl_physical_samples.sample_type_id          has missing values
ERROR   Non-nullable foreign key column tbl_sample_group_coordinates.sample_group_id has missing values == system_id?

Hej, tre mindre problem i nya dendrofilen: 

- tbl_dendro_dates.age_type_id är blank, ska förmodligen vara 1, är non-nullable i databasen.
- tbl_dendro_dates.age_older har icke-numeriska värden (Winter X. After X). fältet är numeriskt i databasen.
- tbl_physical_samples.sample_type_id saknar värden (non-nullable i databasen)
- tbl_sample_group_coordinates.sample_group_id (non-nullable FK), ska förmodligen ha samma värden som "system_id"


