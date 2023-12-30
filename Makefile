.DEFAULT_GOAL=lint
SHELL := /bin/bash
SOURCE_FOLDERS=importer tests
PACKAGE_FOLDER=importer

RUN_TIMESTAMP := $(shell /bin/date "+%Y-%m-%d-%H%M%S")

fast-release: clean tidy build guard_clean_working_repository bump.patch tag publish

release: ready guard_clean_working_repository bump.patch tag  publish

ready: tools clean tidy full-test lint build

build: requirements.txt
	@poetry build

publish:
	@poetry publish

lint: tidy pylint flake8

tidy: black isort

test: output-dir
	@echo SKIPPING LONG RUNNING TESTS!
	@poetry run pytest -m "not long_running" --durations=0 tests
	@rm -rf ./tests/output/*

pytest: output-dir
	@poetry run pytest -m "not long_running" --durations=0 tests

test-coverage: output-dir
	@echo SKIPPING LONG RUNNING TESTS!
	@poetry run pytest -m "not long_running" --cov=$(PACKAGE_FOLDER) --cov-report=html tests
	@rm -rf ./tests/output/*

full-test: output-dir
	@poetry run pytest tests
	@rm -rf ./tests/output/*

long-test: output-dir
	@poetry run pytest -m "long_running" --durations=0 tests
	@rm -rf ./tests/output/*

full-test-coverage: output-dir
	@mkdir -p ./tests/output
	@poetry run pytest --cov=$(PACKAGE_FOLDER) --cov-report=html tests
	@rm -rf ./tests/output/*

output-dir:
	@mkdir -p ./tests/output ./logs

retest:
	@poetry run pytest --durations=0 --last-failed tests

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
	@poetry version patch

.PHONY: tag
tag:
	@poetry build
	@git push
	@git tag $(shell grep "^version \= " pyproject.toml | sed "s/version = //" | sed "s/\"//g") -a
	@git push origin --tags

.PHONY: pylint
pylint:
	@time poetry run pylint $(SOURCE_FOLDERS)
	# @poetry run mypy --version
	# @poetry run mypy .

isort:
	@poetry run isort --profile black --float-to-top --line-length 120 --py 311 $(SOURCE_FOLDERS)

black: clean
	@poetry run black --version
	@poetry run black  $(SOURCE_FOLDERS)

clean:
	@rm -rf .pytest_cache build dist .eggs *.egg-info
	@rm -rf .coverage coverage.xml htmlcov report.xml .tox
	@find . -type d -name '__pycache__' -exec rm -rf {} +
	@find . -type d -name '*pytest_cache*' -exec rm -rf {} +
	@find . -type d -name '.mypy_cache' -exec rm -rf {} +
	@rm -rf tests/output

clean_cache:
	@poetry cache clear pypi --all

requirements.txt: poetry.lock
	@poetry export --without-hashes -f requirements.txt --output requirements.txt

.PHONY: help check install version
.PHONY: lint flake8 pylint pylint_by_file yapf black isort tidy pylint_diff_only
.PHONY: test retest pytest
.PHONY: ready build tag bump.patch release fast-release
.PHONY: clean clean_cache update

venus:
	# @tar czvf ./tmp/VENUS.$(RUN_TIMESTAMP).tar.gz ./tests/test_data/VENUS
	@poetry run python -c 'from tests.pipeline.fixtures import create_test_data_bundles; create_test_data_bundles()'

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