Contributing & Development
==========================

This guide covers setting up a development environment, running tests and
linters, and submitting pull requests.

Development Setup
-----------------

Clone the repository and install in editable mode with development
dependencies:

.. code-block:: bash

   git clone https://github.com/dwsmith1983/pyspark-pipeline-framework.git
   cd pyspark-pipeline-framework

   # Create and activate a virtual environment
   python -m venv .venv
   source .venv/bin/activate  # macOS/Linux
   # .venv\Scripts\activate   # Windows

   # Install with all dev dependencies
   pip install -e ".[dev]"

   # Install pre-commit hooks
   pre-commit install

Or use the Makefile shortcut that combines these steps:

.. code-block:: bash

   make setup

This runs ``pip install -e ".[dev]"`` and ``pre-commit install``.

Running Tests
-------------

The project uses ``pytest`` with class-based test organization. All test
commands use ``PYTHONPATH=src`` so imports resolve correctly.

.. code-block:: bash

   # Run all tests
   make test

   # Run tests with coverage report
   make test-cov

   # Run only unit tests (no Spark required)
   make test-unit

   # Run only Spark integration tests
   make test-spark

These targets correspond to:

.. code-block:: bash

   # Equivalent manual commands
   PYTHONPATH=src pytest tests/ -v
   PYTHONPATH=src pytest tests/ --cov --cov-report=term-missing --cov-report=html -v
   PYTHONPATH=src pytest tests/ -v -m "not spark"
   PYTHONPATH=src pytest tests/ -v -m spark

Pytest Markers
~~~~~~~~~~~~~~

Tests can be tagged with markers defined in ``pyproject.toml``:

.. list-table::
   :header-rows: 1
   :widths: 20 80

   * - Marker
     - Description
   * - ``@pytest.mark.slow``
     - Tests that take a long time to run
   * - ``@pytest.mark.spark``
     - Tests that require a running SparkSession
   * - ``@pytest.mark.integration``
     - Integration tests (external dependencies)

Coverage
~~~~~~~~

The project enforces a minimum of **80% code coverage**. Coverage is configured
in ``pyproject.toml``:

- ``source``: ``src/pyspark_pipeline_framework``
- ``branch``: ``true`` (branch coverage enabled)
- ``omit``: ``*/__init__.py``
- ``exclude_lines``: ``TYPE_CHECKING`` blocks, ``@abstractmethod``, ``pragma: no cover``

After running ``make test-cov``, view the HTML report:

.. code-block:: bash

   open htmlcov/index.html  # macOS
   # xdg-open htmlcov/index.html  # Linux


Type Checking
-------------

The project uses mypy in strict mode. Run type checking with:

.. code-block:: bash

   make lint

This runs both ``ruff check`` and ``mypy src/``:

.. code-block:: bash

   # Equivalent manual commands
   ruff check src/ tests/
   mypy src/

mypy is configured in ``pyproject.toml`` with:

- ``python_version = "3.10"``
- ``warn_return_any = true``
- ``warn_unused_configs = true``
- ``ignore_missing_imports = true`` (for optional dependencies like ``pyspark``)
- ``explicit_package_bases = true``

All PySpark imports must be guarded with ``TYPE_CHECKING`` in ``core/``
modules to maintain the zero-Spark-at-import-time guarantee:

.. code-block:: python

   from __future__ import annotations
   from typing import TYPE_CHECKING

   if TYPE_CHECKING:
       from pyspark.sql import DataFrame, SparkSession


Code Style
----------

The project uses three formatting tools, all configured in ``pyproject.toml``:

.. list-table::
   :header-rows: 1
   :widths: 20 80

   * - Tool
     - Purpose
   * - ``ruff``
     - Linting and fast formatting (line length 120, Python 3.10+)
   * - ``black``
     - Code formatting (line length 120)
   * - ``isort``
     - Import sorting (``black`` profile)

Format all files:

.. code-block:: bash

   make format

This runs:

.. code-block:: bash

   ruff format src/ tests/
   isort src/ tests/
   black src/ tests/

Key style conventions:

- **Line length**: 120 characters
- **Imports**: Absolute imports only (``from pyspark_pipeline_framework.core.component.base import PipelineComponent``)
- **Tests**: Class-based organization (``class TestClassName``)
- **Mocking**: Use ``MagicMock`` for ``SparkSession`` in unit tests
- **Type annotations**: All public APIs must be fully annotated


Project Structure
-----------------

.. code-block:: text

   src/pyspark_pipeline_framework/
   +-- core/
   |   +-- config/         # HOCON config models, loaders, presets
   |   +-- component/      # PipelineComponent ABC, protocols, exceptions
   |   +-- schema/         # DataType enum, SchemaField, SchemaDefinition
   |   +-- resilience/     # RetryExecutor, CircuitBreaker
   |   +-- quality/        # Data quality check types and implementations
   |   +-- audit/          # Audit events, sinks, config filtering
   |   +-- secrets/        # SecretsProvider ABC, Env/AWS/Vault, cache
   |   +-- metrics/        # MeterRegistry, metric types
   +-- runtime/
   |   +-- session/        # SparkSessionWrapper (lifecycle management)
   |   +-- dataflow/       # DataFlow ABC, SchemaAwareDataFlow
   |   +-- streaming/      # StreamingSource, StreamingSink, pipelines
   |   +-- loader.py       # Dynamic component loading (importlib)
   +-- runner/
   |   +-- hooks.py        # PipelineHooks protocol, NoOpHooks
   |   +-- hooks_builtin.py# LoggingHooks, MetricsHooks, CompositeHooks
   |   +-- simple_runner.py# SimplePipelineRunner
   |   +-- result.py       # PipelineResult, ComponentResult
   |   +-- checkpoint.py   # CheckpointState, LocalCheckpointStore
   |   +-- quality_hooks.py# DataQualityHooks
   |   +-- audit_hooks.py  # AuditHooks
   +-- examples/
       +-- batch.py        # ReadTable, SqlTransform, WriteTable, ReadCsv, WriteCsv
       +-- streaming.py    # FileToConsolePipeline, KafkaToDeltaPipeline

**Layer rules:**

- ``core/`` has **zero** PySpark dependency at import time. All Spark imports
  are guarded with ``TYPE_CHECKING``.
- ``runtime/`` depends on ``core/`` and PySpark.
- ``runner/`` depends on both ``core/`` and ``runtime/``.


Pre-commit Hooks
----------------

The project uses ``pre-commit`` to run checks before every commit:

.. code-block:: bash

   # Install hooks (done automatically by make setup)
   pre-commit install

   # Run all hooks against all files
   make pre-commit

   # Equivalent manual command
   pre-commit run --all-files

The ``make check`` target runs both pre-commit hooks and the full test suite:

.. code-block:: bash

   make check


Running Examples
----------------

The ``examples/`` package includes reference implementations for both batch and
streaming pipelines.

Batch Example
~~~~~~~~~~~~~

The batch examples (``ReadTable``, ``SqlTransform``, ``WriteTable``,
``ReadCsv``, ``WriteCsv``) demonstrate the source-transform-sink pattern:

.. code-block:: python

   from pyspark_pipeline_framework.examples.batch import (
       ReadCsv, ReadCsvConfig,
       SqlTransform, SqlTransformConfig,
       WriteCsv, WriteCsvConfig,
   )

   # Create components
   reader = ReadCsv(ReadCsvConfig(path="data/customers.csv", output_view="raw"))
   transform = SqlTransform(SqlTransformConfig(
       sql="SELECT id, UPPER(name) AS name FROM raw",
       output_view="cleaned",
   ))
   writer = WriteCsv(WriteCsvConfig(input_view="cleaned", path="/tmp/output"))

   # Set SparkSession and run
   for comp in [reader, transform, writer]:
       comp.set_spark_session(spark)
       comp.run()

Or run via HOCON configuration:

.. code-block:: bash

   ppf-run --config examples/batch_pipeline.conf

Streaming Example
~~~~~~~~~~~~~~~~~

The streaming examples (``FileToConsolePipeline``, ``KafkaToDeltaPipeline``)
demonstrate Structured Streaming:

.. code-block:: python

   from pyspark_pipeline_framework.examples.streaming import (
       FileToConsolePipeline,
   )
   from pyspark_pipeline_framework.runtime.streaming.sources import (
       FileStreamingSource,
   )
   from pyspark_pipeline_framework.runtime.streaming.sinks import (
       ConsoleStreamingSink,
   )

   pipeline = FileToConsolePipeline(
       source=FileStreamingSource(path="/data/input", file_format="json"),
       sink=ConsoleStreamingSink(checkpoint_location="/tmp/checkpoint"),
       filter_condition="value IS NOT NULL",
   )
   pipeline.set_spark_session(spark)
   pipeline.run()  # blocks until terminated


Pull Request Guidelines
-----------------------

1. **Branch from main.** Create a feature branch from ``main``:

   .. code-block:: bash

      git checkout -b feature/my-feature main

2. **All tests pass.** Run the full test suite before submitting:

   .. code-block:: bash

      make test

3. **mypy clean.** No type errors allowed:

   .. code-block:: bash

      make lint

4. **Code formatted.** Run the formatter before committing:

   .. code-block:: bash

      make format

5. **Coverage >= 80%.** New code must maintain the coverage threshold:

   .. code-block:: bash

      make test-cov

6. **Pre-commit hooks pass.** All hooks must pass:

   .. code-block:: bash

      make pre-commit

7. **Commit messages.** Use clear, descriptive commit messages:

   .. code-block:: text

      feat(core): add new secrets provider for GCP Secret Manager
      fix(runner): handle empty component list in SimplePipelineRunner
      docs: add streaming pipeline examples to user guide
      test: add coverage for circuit breaker half-open state

8. **Pull request description.** Describe what changed and why. Include:

   - A summary of the changes
   - Any new dependencies added
   - Test plan or manual verification steps

Quick Checklist
~~~~~~~~~~~~~~~

.. code-block:: bash

   # Before submitting a PR
   make format       # Format code
   make lint         # Type check + lint
   make test-cov     # Tests + coverage
   make pre-commit   # All pre-commit hooks


See Also
--------

- :doc:`/getting-started` -- Installation and quick start
- :doc:`/architecture` -- Project architecture overview
- :doc:`/deployment` -- Production deployment guide
