
#  SEAD Clearinghouse Import
This folder contains `python` scripts that creates, uploads and processes an "CH complient" XML import file. The file must be a complete data submission prepared as an Excel file i.e. a file such as previously imported `Dendro archeologhy/buildning` and `Ceramics` submissions.

#### Install

This program uses `tidlib` to cleanup the resulting XML:

```bash
sudo apt-get install tidy
```

It is also recommended to install `pyenv`:

```bash
sudo apt-get install -y make build-essential libssl-dev zlib1g-dev libbz2-dev \
    libreadline-dev libsqlite3-dev wget curl llvm libncurses5-dev libncursesw5-dev \
    xz-utils tk-dev libffi-dev liblzma-dev
curl -L https://raw.githubusercontent.com/pyenv/pyenv-installer/master/bin/pyenv-installer | bash
```
Then add the following to your `.bashrc`:

```bash
echo export PATH="/home/roger/.pyenv/bin:$PATH"
eval "$(pyenv init -)"
eval "$(pyenv virtualenv-init -)"
```

Install (local) Python version using `pyenv`:

```bash
pyenv install 3.7.5
pyenv global 3.7.5
python --version
```
Install `pipenv`:
```bash
pip install pipenv
```
Install (i.e run) from source (assumes that python, pipenv and git is installed):
```bash
git clone https://github.com/humlab-sead/sead_clearinghouse.git
cd sead_clearinghouse/import
pipenv install
```

Edit options in `runner.py` and then run:

```bash
python runner.py
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
- pipenv run python process.py options...
```

Flags `--input-folder` and `--output-folder` are ignored by Docker since it assumes `./input` and `./output` are mounted to proper folders.

Install Docker image (assumes that Docker is installed):

```bash
wget https://raw.githubusercontent.com/humlab-sead/sead_clearinghouse/master/import/Dockerfile
docker build -f Dockerfile -t ch/import:latest .
docker run -rm -it ch/import:latest -v "your-input-folder":/input -v "your-output-folder":/output options...
```

