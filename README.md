
#  SEAD Clearinghouse Import
This folder contains `python` scripts that creates, uploads and processes an "CH complient" XML import file. The file must be a complete data submission prepared as an Excel file i.e. a file such as previously imported `Dendro archeologhy/buildning` and `Ceramics` submissions.

#### Install

Install Docker image (assumes that Docker is installed):
```bash
wget https://raw.githubusercontent.com/humlab-sead/sead_clearinghouse/master/import/Dockerfile
docker build -f Dockerfile -t ch/import:latest .
```
Install (i.e run) from source (assumes that python, pipenv and git is installed):
```bash
git clone https://github.com/humlab-sead/sead_clearinghouse.git
cd sead_clearinghouse/import
pipenv install && chmod +x import.sh
```

#### Usage
The import expects that user's password is stored in environment variable "SEAD_CH_PASSWORD". Before

```bash
usage: `run-command` [-h] --host DBHOST [--port PORT] --dbname DBNAME
                  [--dbuser DBUSER] --input-folder INPUT_FOLDER
                  --output-folder OUTPUT_FOLDER --data-filename DATA_FILENAME
                  [--meta-filename META_FILENAME]
                  [--xml-filename XML_FILENAME] [--id SUBMISSION_ID]
                  [--table-names TABLE_NAMES] --data-types DATA_TYPES [--skip]

optional arguments:
  -h, --help            show this help message and exit
  --host DBHOST         target database server
  --port PORT           server port number
  --dbname DBNAME       target database
  --dbuser DBUSER       target database username
  --input-folder INPUT_FOLDER
                        source folder where input files are stored
  --output-folder OUTPUT_FOLDER
                        target folder where result is stored
  --data-filename DATA_FILENAME
                        name of file that contains data
  --meta-filename META_FILENAME
                        name of file that contains meta-data
  --xml-filename XML_FILENAME
                        name of existing XML to use
  --id SUBMISSION_ID    overwrite (replace) existing submission id
  --table-names TABLE_NAMES
                        load specific tables only
  --data-types DATA_TYPES
                        types of data (short description)
  --skip                skip (do nothing)
```

Substitute `run_command` with either of the following commands depending on install method:

```bash
- docker run -rm -it ch/import:latest -v "your-input-folder":/input -v "your-output-folder":/output options...
- pipenv run python process.py options...
- `import.sh options`...
```
Flags `--input-folder` and `--output-folder` are ignored by Docker since it assumes `./input` and `./output` are mounted to proper folders.

