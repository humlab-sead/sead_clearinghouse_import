
##  SEAD Clearinghouse Import
This folder contains `python` scripts that creates, uploads and processes an "CH complient" XML import file. The file must be a complete data submission prepared as an Excel file i.e. a file such as previously imported `Dendro archeologhy/buildning` and `Ceramics` submissions.

### Install
This program uses `tidlib` to cleanup the resulting XML which you can install using apt:

```bash
sudo apt-get install tidy
```

You will need Python ^3.12 (install with pyenv) and Poetry on your local machine.

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

The import program reads configuration in the following order of priority:

1. Command line arguments have the highest priority and supercedes other configurations.
2. Command line arguments found in options file (--options-filename FILENAME)
3. Environment variables having prefix "SEAD_IMPORT_ABC_DEF" corresponding to setting "options"[abc].def.
4. Values in "options" section in YAML configuration file (argument)


### Examples

Check for errors in file (don't create XML):

```bash
λ PYTHONPATH=. python importer/scripts/import-excel.py data/input/building_dendro_2023-12_import.xlsx --check-only --data-types dendrochronology
```

Generate XML and submit to clearinghouse:

```bash
λ PYTHONPATH=. python importer/scripts/import-excel.py data/input/building_dendro_2023-12_import.xlsx --data-types dendrochronology
```
