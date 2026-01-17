.PHONY: install install-dev test test-cov lint format check clean build docs

PYTHONPATH := src

install:
	pip install -e .

install-dev:
	pip install -e ".[dev]"
	pre-commit install

test:
	PYTHONPATH=$(PYTHONPATH) pytest tests/ -v

test-cov:
	PYTHONPATH=$(PYTHONPATH) pytest tests/ --cov --cov-report=term-missing --cov-report=html

test-unit:
	PYTHONPATH=$(PYTHONPATH) pytest tests/unit -v -m "not spark"

test-spark:
	PYTHONPATH=$(PYTHONPATH) pytest tests/ -v -m spark

lint:
	ruff check src/ tests/
	mypy src/

format:
	ruff format src/ tests/
	isort src/ tests/
	black src/ tests/

check: lint test

clean:
	rm -rf build/ dist/ *.egg-info/ .pytest_cache/ .coverage htmlcov/ .mypy_cache/ .ruff_cache/

build:
	python -m build

docs:
	cd docs && make html

docs-clean:
	cd docs && make clean
