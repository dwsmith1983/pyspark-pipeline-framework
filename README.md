# PySpark Pipeline Framework

A configuration-driven PySpark pipeline framework designed for enterprise-scale data engineering workflows with HOCON-based configuration, type safety, and operational resilience.

[![Python](https://img.shields.io/badge/python-3.9%2B-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

## Status

🚧 **Early Development** - This project is in active development. Core features are being implemented and the API is subject to change.

## Overview

PySpark Pipeline Framework provides a declarative approach to building, orchestrating, and operating PySpark data pipelines. Instead of writing imperative pipeline code, you define your data transformations and orchestration logic in HOCON configuration files, enabling:

- **Separation of Concerns**: Business logic in configuration, execution logic in the framework
- **Type Safety**: Compile-time validation of pipeline structure and data contracts
- **Operational Excellence**: Built-in observability, fault tolerance, and lifecycle management
- **Composability**: Reusable components and standardized patterns

## Architecture

The framework is organized into several key modules:

```
pyspark_pipeline_framework/
├── core/           # Core abstractions and type system
├── runner/         # Pipeline execution engine and CLI
└── runtime/        # Runtime components (secrets, metrics, lifecycle hooks)
```

### Design Principles

1. **Configuration-Driven**: Pipelines are defined in HOCON files, not code
2. **Type-Safe Components**: Strong typing with runtime validation via dataconf
3. **Fault Tolerant**: Built-in retry policies, circuit breakers, and graceful degradation
4. **Observable**: Structured logging (structlog), metrics, and audit trails
5. **Cloud Native**: First-class support for AWS, secrets management, and distributed execution

## Planned Features

### Core Pipeline Framework
- [ ] Pipeline configuration schema with HOCON support
- [ ] Type-safe component model with dataconf integration
- [ ] DAG-based pipeline orchestration
- [ ] Streaming and batch processing modes

### Operational Features
- [ ] Lifecycle hooks (pre/post execution, checkpoints)
- [ ] Structured logging with context propagation
- [ ] Metrics integration (Prometheus, OpenTelemetry)
- [ ] Distributed tracing support

### Fault Tolerance
- [ ] Retry policies with exponential backoff
- [ ] Circuit breaker pattern implementation
- [ ] Graceful degradation strategies
- [ ] Checkpoint and recovery mechanisms

### Integrations
- [ ] AWS integration (S3, Glue, Secrets Manager)
- [ ] HashiCorp Vault for secrets management
- [ ] Prometheus metrics export
- [ ] OpenTelemetry tracing

## Installation

### Basic Installation

```bash
pip install pyspark-pipeline-framework
```

### With Optional Dependencies

```bash
# Full installation with all features
pip install pyspark-pipeline-framework[all]

# Specific feature sets
pip install pyspark-pipeline-framework[spark]      # PySpark support
pip install pyspark-pipeline-framework[aws]        # AWS integrations
pip install pyspark-pipeline-framework[vault]      # Vault secrets
pip install pyspark-pipeline-framework[metrics]    # Metrics and telemetry
```

### Development Installation

```bash
# Clone the repository
git clone https://github.com/OWNER/pyspark-pipeline-framework.git
cd pyspark-pipeline-framework

# Install in development mode with dev dependencies
pip install -e ".[dev]"

# Set up pre-commit hooks
pre-commit install
```

## Quick Start

> **Note**: Full documentation will be available once core features are implemented.

### Basic Pipeline Configuration

```hocon
pipeline {
  name = "example-etl"
  mode = "batch"

  stages = [
    {
      name = "extract"
      type = "spark.read"
      format = "parquet"
      path = "s3://bucket/input/"
    },
    {
      name = "transform"
      type = "spark.sql"
      sql = "SELECT * FROM extract WHERE date >= '2024-01-01'"
    },
    {
      name = "load"
      type = "spark.write"
      format = "delta"
      path = "s3://bucket/output/"
      mode = "overwrite"
    }
  ]
}
```

### Running a Pipeline

```bash
# Using the CLI
ppf-run --config pipeline.conf

# With additional options
ppf-run --config pipeline.conf --env production --dry-run
```

### Python API

```python
from pyspark_pipeline_framework import Pipeline

# Load and execute pipeline
pipeline = Pipeline.from_config("pipeline.conf")
result = pipeline.run()
```

## Development

### Prerequisites

- Python 3.9 or later
- PySpark 3.4+ (optional, for Spark-based pipelines)
- pip or poetry for dependency management

### Setting Up Development Environment

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install development dependencies
pip install -e ".[dev]"

# Install pre-commit hooks
pre-commit install
```

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=pyspark_pipeline_framework --cov-report=html

# Run specific test categories
pytest -m "not slow"              # Skip slow tests
pytest -m "not spark"             # Skip Spark integration tests
pytest -m integration             # Run only integration tests
```

### Code Quality

The project uses several tools to maintain code quality:

```bash
# Format code
black src/ tests/
isort src/ tests/

# Lint code
ruff check src/ tests/

# Type checking
mypy src/

# Run all checks (automatically via pre-commit)
pre-commit run --all-files
```

### Project Structure

```
.
├── src/
│   └── pyspark_pipeline_framework/
│       ├── core/           # Core abstractions
│       ├── runner/         # CLI and execution engine
│       └── runtime/        # Runtime components
├── tests/                  # Test suite
│   ├── unit/              # Unit tests
│   ├── integration/       # Integration tests
│   └── fixtures/          # Test fixtures and data
├── examples/              # Example pipelines and configurations
├── docs/                  # Documentation
├── pyproject.toml        # Project metadata and dependencies
└── README.md             # This file
```

## Contributing

Contributions are welcome! This project is in early development, and we're actively building core features.

### How to Contribute

1. **Fork the repository** and create a feature branch
2. **Make your changes** with appropriate tests
3. **Ensure all tests pass** and code quality checks succeed
4. **Submit a pull request** with a clear description of changes

### Contribution Guidelines

- Follow the existing code style (enforced by black, isort, ruff)
- Add tests for new features
- Update documentation as needed
- Keep commits atomic and write clear commit messages
- Ensure CI checks pass before requesting review

### Development Workflow

```bash
# Create a feature branch
git checkout -b feature/your-feature-name

# Make changes and commit
git add .
git commit -m "Add: description of your changes"

# Push and create PR
git push origin feature/your-feature-name
```

## Roadmap

### Phase 1: Core Framework (Current)
- [x] Project setup and structure
- [x] Development tooling (linting, formatting, testing)
- [ ] Core configuration models
- [ ] Basic pipeline execution engine
- [ ] CLI implementation

### Phase 2: Pipeline Features
- [ ] DAG-based orchestration
- [ ] Streaming support
- [ ] Advanced transformations
- [ ] Data quality validation

### Phase 3: Operational Features
- [ ] Comprehensive logging and metrics
- [ ] Fault tolerance mechanisms
- [ ] Checkpoint and recovery
- [ ] Performance optimization

### Phase 4: Integrations
- [ ] AWS services integration
- [ ] Secrets management (Vault)
- [ ] Monitoring and alerting
- [ ] CI/CD templates

## Documentation

Documentation will be available at [https://pyspark-pipeline-framework.readthedocs.io](https://pyspark-pipeline-framework.readthedocs.io) once core features are implemented.

### Building Documentation Locally

```bash
# Install docs dependencies
pip install ".[docs]"

# Build documentation
cd docs
make html

# View documentation
open _build/html/index.html
```

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Support

- **Issues**: [GitHub Issues](https://github.com/OWNER/pyspark-pipeline-framework/issues)
- **Discussions**: [GitHub Discussions](https://github.com/OWNER/pyspark-pipeline-framework/discussions)

## Acknowledgments

- Inspired by modern data engineering frameworks and best practices
- Built on top of Apache Spark's powerful distributed computing engine
- Uses HOCON for flexible, hierarchical configuration management
