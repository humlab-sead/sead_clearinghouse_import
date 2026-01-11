.DEFAULT_GOAL=lint
SHELL := /bin/bash
SOURCE_FOLDERS=importer tests
PACKAGE_FOLDER=importer

RUN_TIMESTAMP := $(shell /bin/date "+%Y-%m-%d-%H%M%S")

.PHONY: install
install:
	@uv sync --all-extras
	
fast-release: clean tidy build guard_clean_working_repository bump.patch tag publish

release: ready guard_clean_working_repository bump.patch tag  publish

ready: tools clean tidy full-test lint build

build: requirements.txt
	@uv build

publish:
	@uv publish

lint: tidy pylint ruff

ruff:
	@uv run ruff check $(SOURCE_FOLDERS) --fix

tidy: black isort

test: output-dir
	@echo "Running unit tests (fast, no DB required)..."
	@uv run pytest tests/unit/ --durations=0
	@rm -rf ./tests/output/*

test-unit: output-dir
	@echo "Running unit tests only..."
	@uv run pytest tests/unit/ -v
	@rm -rf ./tests/output/*

test-integration: output-dir
	@echo "Running integration tests (requires DB)..."
	@uv run pytest tests/integration/ --durations=0
	@rm -rf ./tests/output/*

pytest: output-dir
	@uv run pytest -m "not long_running" --durations=0 tests

test-coverage: output-dir
	@echo "Running tests with coverage report..."
	@uv run pytest tests/unit/ --cov=$(PACKAGE_FOLDER) --cov-report=html --cov-report=term
	@echo "Coverage report generated in htmlcov/index.html"
	@rm -rf ./tests/output/*

test-coverage-full: output-dir
	@echo "Running all tests with coverage report..."
	@uv run pytest tests/ --cov=$(PACKAGE_FOLDER) --cov-report=html --cov-report=term
	@echo "Coverage report generated in htmlcov/index.html"
	@rm -rf ./tests/output/*

full-test: output-dir
	@uv run pytest tests
	@rm -rf ./tests/output/*

long-test: output-dir
	@uv run pytest -m "long_running" --durations=0 tests
	@rm -rf ./tests/output/*

full-test-coverage: output-dir
	@mkdir -p ./tests/output
	@uv run pytest --cov=$(PACKAGE_FOLDER) --cov-report=html tests
	@rm -rf ./tests/output/*

output-dir:
	@mkdir -p ./tests/output ./logs

retest:
	@uv run pytest --durations=0 --last-failed tests

.ONESHELL: guard_clean_working_repository
guard_clean_working_repository:
	@status="$$(git status --porcelain)"
	@if [[ "$$status" != "" ]]; then
		echo "error: changes exists, please commit or stash them: "
		echo "$$status"
		exit 65
	fi

bump.patch: bump.version.patch sync.package.version
	@git add pyproject.toml requirements.txt penelope/__init__.py
	@git commit -m "Bump version patch"
	@git push

bump.version.patch:
	@uv version patch

.PHONY: tag
tag:
	@uv build
	@git push
	@git tag $(shell grep "^version \= " pyproject.toml | sed "s/version = //" | sed "s/\"//g") -a
	@git push origin --tags

.PHONY: pylint
pylint:
	@time uv run pylint $(SOURCE_FOLDERS)
	# @uv run mypy --version
	# @uv run mypy .

isort:
	@uv run isort --profile black --float-to-top --line-length 120 --py auto $(SOURCE_FOLDERS)

black: clean
	@uv run black --version
	@uv run black  $(SOURCE_FOLDERS)

clean:
	@rm -rf .pytest_cache build dist .eggs *.egg-info
	@rm -rf .coverage coverage.xml htmlcov report.xml .tox
	@find . -type d -name '__pycache__' -exec rm -rf {} +
	@find . -type d -name '*pytest_cache*' -exec rm -rf {} +
	@find . -type d -name '.mypy_cache' -exec rm -rf {} +
	@rm -rf tests/output

clean_cache:
	@uv cache clean

requirements.txt: uv.lock
	@uv pip compile pyproject.toml -o requirements.txt

.PHONY: help check install version
.PHONY: lint flake8 pylint pylint_by_file yapf black isort tidy pylint_diff_only
.PHONY: test retest pytest
.PHONY: ready build tag bump.patch release fast-release
.PHONY: clean clean_cache update

venus:
	# @tar czvf ./tmp/VENUS.$(RUN_TIMESTAMP).tar.gz ./tests/test_data/VENUS
	@uv run python -c 'from tests.pipeline.fixtures import create_test_data_bundles; create_test_data_bundles()'

help:
	@echo "Higher level recepies: "
	@echo " make ready            Makes ready for release (tools tidy test flake8 pylint)"
	@echo " make build            Updates tools, requirement.txt and build dist/wheel"
	@echo " make release          Bumps version (patch), pushes to origin and creates a tag on origin"
	@echo " make fast-release     Same as release but without lint and test"
	@echo " make test             Runs tests with code coverage"
	@echo " make retest           Runs failed tests with code coverage"
	@echo " make lint             Runs pylint and flake8"
	@echo " make tidy             Runs black and isort"
	@echo " make clean            Removes temporary files, caches, build files"
	@echo " make data             Downloads NLTK and SpaCy data"
	@echo "  "
	@echo "Lower level recepies: "
	@echo " make init             Install development tools and dependencies (dev recepie)"
	@echo " make tag              bump.patch + creates a tag on origin"
	@echo " make bump.patch       Bumps version (patch), pushes to origin"
	@echo " make pytest           Runs teets without code coverage"
	@echo " make pylint           Runs pylint"
	@echo " make pytest2          Runs pylint on a per-file basis"
	@echo " make flake8           Runs flake8 (black, flake8-pytest-style, mccabe, naming, pycodestyle, pyflakes)"
	@echo " make isort            Runs isort"
	@echo " make yapf             Runs yapf"
	@echo " make black            Runs black"