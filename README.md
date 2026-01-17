# PySpark Pipeline Framework

[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Development Status](https://img.shields.io/badge/status-alpha-orange.svg)]()

**Configuration-driven PySpark data pipeline framework with HOCON configuration**

Build maintainable, testable, and production-ready data pipelines by defining your ETL logic in declarative HOCON configuration files instead of hardcoded scripts.

---

## Table of Contents

- [Overview](#overview)
- [Why This Framework?](#why-this-framework)
- [Key Features](#key-features)
- [Architecture](#architecture)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Core Concepts](#core-concepts)
- [Development](#development)
- [License](#license)

---

## Overview

The PySpark Pipeline Framework provides a structured, configuration-driven approach to building Apache Spark data pipelines. Instead of writing monolithic PySpark scripts with hardcoded logic, you define reusable components in Python and orchestrate them through human-friendly HOCON configuration files.

**Core Philosophy:**
- **Separate concerns**: Configuration (what to run) from implementation (how to run)
- **Reusable components**: Write once, configure many times
- **Type safety**: Leverage Python typing for robust component contracts
- **Production-ready**: Built-in logging, metrics, fault tolerance, and secrets management

---

## Why This Framework?

### The Problem

Traditional PySpark pipelines often suffer from:
- **Hardcoded logic**: Business rules buried in code require deployments to change
- **Copy-paste sprawl**: Similar pipelines duplicated with minor variations
- **Testing challenges**: Difficult to test configuration vs. logic separately
- **Operational blindness**: Limited observability into pipeline execution
- **Fragile execution**: No standardized retry logic or error handling

### The Solution

This framework addresses these issues by:
- **Declarative pipelines**: Define data flows in HOCON config files
- **Component library**: Build reusable Extract, Transform, Load components
- **Dependency injection**: Runtime provides Spark session, configuration, and context
- **Lifecycle hooks**: Pluggable logging, metrics, and audit trail integration
- **Fault tolerance**: Built-in retry policies, circuit breakers, and error handling

---

## Key Features

### 🔧 Configuration-Driven Pipeline Orchestration
Define your entire ETL pipeline in HOCON (Human-Optimized Config Object Notation) files. Change pipeline behavior without code changes or redeployment.

### 📋 HOCON-Based Configuration
HOCON extends JSON with:
- Comments for documentation
- Include files for DRY configuration
- Variable substitution and environment overrides
- Human-friendly syntax (less noise than JSON/YAML)

### 🔒 Type-Safe Component Model
Leverage Python type hints for robust component interfaces. Catch configuration errors at startup, not during execution.

### 📊 Lifecycle Hooks
Plug in custom handlers for:
- Structured logging (via `structlog`)
- Metrics collection (Prometheus, OpenTelemetry)
- Audit trails and lineage tracking
- Custom monitoring and alerting

### 🛡️ Fault Tolerance
Production-ready error handling:
- Configurable retry policies with exponential backoff
- Circuit breakers to prevent cascade failures
- Dead letter queues for failed records
- Graceful degradation strategies

### 🔐 Secrets Management Integration
Secure credential handling via:
- HashiCorp Vault integration
- AWS Secrets Manager support
- Environment variable substitution
- Encrypted configuration values

### 🌊 Streaming and Batch Support
Unified API for both:
- Spark Structured Streaming pipelines
- Traditional batch processing
- Micro-batch processing patterns

---

## Architecture

The framework is organized into three core modules:

```
pyspark_pipeline_framework/
├── core/          # Component model and pipeline definitions
├── runner/        # CLI and execution orchestration
└── runtime/       # Execution engine and dependency injection
```

### Module Breakdown

#### `core/` - Component Model
- **Components**: Abstract base classes for Extract, Transform, Load steps
- **Pipeline**: DAG-based pipeline definition and validation
- **Hooks**: Lifecycle hook interfaces (before/after step, on error, etc.)
- **Config**: HOCON schema definitions and validation

#### `runner/` - CLI and Orchestration
- **CLI**: `ppf-run` command-line tool for executing pipelines
- **Config Loading**: Parse HOCON files and environment overrides
- **Validation**: Pre-flight checks before pipeline execution
- **Scheduling**: Integration points for Airflow/Dagster/etc.

#### `runtime/` - Execution Engine
- **Context**: Dependency injection container (Spark session, config, logger)
- **Executor**: Step execution with retry logic and error handling
- **Metrics**: Prometheus/OpenTelemetry metric collection
- **Secrets**: Vault/AWS Secrets Manager integration

### Data Flow

```
┌─────────────────┐
│  HOCON Config   │
│  pipeline.conf  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐      ┌──────────────┐
│  ppf-run CLI    │─────▶│   Runtime    │
│  (runner)       │      │   Context    │
└────────┬────────┘      └──────┬───────┘
         │                      │
         ▼                      ▼
┌─────────────────┐      ┌──────────────┐
│  Pipeline DAG   │◀─────│  Component   │
│  (core)         │      │  Registry    │
└────────┬────────┘      └──────────────┘
         │
         ▼
┌─────────────────┐
│  Step Executor  │──▶ Extract ──▶ Transform ──▶ Load
│  (runtime)      │
└─────────────────┘
         │
         ▼
    ┌────────┐  ┌─────────┐  ┌────────┐
    │ Logs   │  │ Metrics │  │ Alerts │
    └────────┘  └─────────┘  └────────┘
```

---

## Installation

### Basic Installation

```bash
pip install pyspark-pipeline-framework
```

### With Optional Dependencies

```bash
# Include PySpark (if not already installed)
pip install pyspark-pipeline-framework[spark]

# AWS integration
pip install pyspark-pipeline-framework[aws]

# Vault secrets management
pip install pyspark-pipeline-framework[vault]

# Metrics (Prometheus + OpenTelemetry)
pip install pyspark-pipeline-framework[metrics]

# Everything
pip install pyspark-pipeline-framework[all]
```

### Development Installation

```bash
git clone https://github.com/OWNER/pyspark-pipeline-framework.git
cd pyspark-pipeline-framework
make install-dev
```

---

## Quick Start

### 1. Define a Pipeline Configuration

Create `pipelines/user_etl.conf`:

```hocon
pipeline {
  name = "user-etl"
  description = "Extract users from S3, anonymize PII, load to Delta Lake"

  spark {
    app_name = "UserETL"
    master = "local[*]"  # Override via environment
  }

  steps = [
    {
      name = "extract-users"
      type = "pyspark_pipeline_framework.components.S3Source"
      config {
        path = "s3://raw-data/users/*.parquet"
        format = "parquet"
      }
    }
    {
      name = "anonymize-pii"
      type = "my_pipelines.transforms.UserAnonymizer"
      config {
        pii_fields = ["email", "phone", "ssn"]
        hash_algorithm = "sha256"
      }
    }
    {
      name = "load-warehouse"
      type = "pyspark_pipeline_framework.components.DeltaSink"
      config {
        path = "s3://warehouse/users"
        mode = "merge"
        merge_keys = ["user_id"]
      }
    }
  ]

  hooks {
    enable_metrics = true
    enable_audit_log = true
  }
}
```

### 2. Implement Custom Components

Create `my_pipelines/transforms.py`:

```python
from pyspark.sql import DataFrame
from pyspark_pipeline_framework.core import Transform, TransformConfig

class UserAnonymizerConfig(TransformConfig):
    pii_fields: list[str]
    hash_algorithm: str = "sha256"

class UserAnonymizer(Transform[UserAnonymizerConfig]):
    """Anonymize PII fields using configurable hashing."""

    def transform(self, df: DataFrame) -> DataFrame:
        from pyspark.sql.functions import sha2

        result = df
        for field in self.config.pii_fields:
            result = result.withColumn(
                field,
                sha2(result[field], 256) if self.config.hash_algorithm == "sha256" else hash(result[field])
            )

        self.logger.info(
            "anonymized_pii",
            field_count=len(self.config.pii_fields),
            algorithm=self.config.hash_algorithm
        )

        return result
```

### 3. Run the Pipeline

```bash
ppf-run --config pipelines/user_etl.conf
```

**With environment overrides:**

```bash
export SPARK_MASTER=spark://cluster:7077
ppf-run --config pipelines/user_etl.conf \
        --override spark.master=$SPARK_MASTER
```

---

## Configuration

### HOCON Features

HOCON (Human-Optimized Config Object Notation) extends JSON with powerful features:

#### Comments
```hocon
pipeline {
  # This is a comment
  name = "my-pipeline"  // Also a comment
}
```

#### Variable Substitution
```hocon
base_path = "s3://my-bucket"

pipeline {
  input = ${base_path}"/raw"
  output = ${base_path}"/processed"
}
```

#### Environment Variables
```hocon
pipeline {
  spark {
    master = ${?SPARK_MASTER}  # Use env var if set, else omit
  }
}
```

#### File Includes
```hocon
include "common.conf"  # Merge in shared configuration

pipeline {
  # Override or extend included config
  name = "specific-pipeline"
}
```

### Configuration Schema

See `docs/configuration.md` for the full schema reference.

---

## Core Concepts

### Components

All pipeline steps inherit from base component types:

- **Source**: Extract data from external systems
- **Transform**: Apply business logic transformations
- **Sink**: Load data to target systems
- **Validator**: Data quality checks and assertions

Each component receives:
- **Config**: Typed configuration object (validated at startup)
- **Context**: Runtime services (Spark session, logger, metrics)
- **DataFrame**: Input data (for Transform/Sink)

### Pipelines

Pipelines define a DAG of component steps:
- Steps execute in dependency order
- Each step's output becomes the next step's input
- Failed steps trigger retry logic or halt execution
- Hooks execute at step boundaries (before/after)

### Lifecycle Hooks

Customize pipeline behavior via hooks:

```python
from pyspark_pipeline_framework.core import Hook

class MetricsHook(Hook):
    def before_step(self, step_name: str, context: Context):
        context.metrics.start_timer(f"step.{step_name}")

    def after_step(self, step_name: str, context: Context, result: DataFrame):
        context.metrics.stop_timer(f"step.{step_name}")
        context.metrics.record_row_count(f"step.{step_name}", result.count())
```

### Fault Tolerance

Configure retry behavior per step:

```hocon
steps = [
  {
    name = "flaky-api-call"
    type = "my.APIExtract"
    retry {
      max_attempts = 3
      backoff = "exponential"
      initial_delay = "5s"
      max_delay = "60s"
    }
    circuit_breaker {
      failure_threshold = 5
      timeout = "30s"
    }
  }
]
```

---

## Development

### Setup

```bash
make install-dev
```

This installs the package in editable mode with all development dependencies and sets up pre-commit hooks.

### Running Tests

```bash
# All tests
make test

# Unit tests only (no Spark required)
make test-unit

# Spark integration tests
make test-spark

# With coverage report
make test-cov
```

### Code Quality

```bash
# Run linters (ruff, mypy)
make lint

# Format code (black, isort, ruff)
make format

# Both lint and test
make check
```

### Project Structure

```
pyspark_pipeline_framework/
├── src/
│   └── pyspark_pipeline_framework/
│       ├── core/           # Component abstractions
│       ├── runner/         # CLI and orchestration
│       └── runtime/        # Execution engine
├── tests/
│   ├── unit/              # Fast unit tests
│   └── integration/       # Spark integration tests
├── examples/              # Example pipelines
├── docs/                  # Sphinx documentation
├── pyproject.toml         # Project metadata and dependencies
└── Makefile               # Development commands
```

### Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Run tests and linters (`make check`)
5. Commit your changes (`git commit -m 'Add amazing feature'`)
6. Push to the branch (`git push origin feature/amazing-feature`)
7. Open a Pull Request

---

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

---

## Resources

- **Documentation**: https://pyspark-pipeline-framework.readthedocs.io
- **Issue Tracker**: https://github.com/OWNER/pyspark-pipeline-framework/issues
- **Source Code**: https://github.com/OWNER/pyspark-pipeline-framework

---

**Built with ❤️ for the data engineering community**
