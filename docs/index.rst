pyspark-pipeline-framework
==========================

Configuration-driven PySpark pipeline framework with HOCON configuration,
resilience patterns, lifecycle hooks, and streaming support.

Build batch and streaming data pipelines using composable components, HOCON
configuration files, and a rich set of operational features including retry
policies, circuit breakers, data quality checks, audit trails, secrets
management, and checkpoint/resume.

**Supported Versions:**

- Python 3.10 - 3.13
- Apache Spark 3.4+ (optional runtime dependency)

Scope & Limitations
-------------------

pyspark-pipeline-framework is designed for **configuration-driven pipeline
orchestration** on PySpark.

**What it does well:**

- Define pipelines as HOCON configuration files with typed components
- Dynamically load and wire components at runtime
- Run batch pipelines with dependency ordering and resilience
- Run streaming pipelines with Kafka, Delta, and file sources/sinks
- Attach lifecycle hooks for logging, metrics, data quality, and audit
- Resume failed pipelines from the last successful checkpoint

**Known limitations:**

- PySpark is an optional dependency -- components that call ``self.spark`` require
  a Spark session at runtime
- Streaming pipelines use Spark Structured Streaming; no Kafka Streams or Flink support

.. toctree::
   :maxdepth: 2
   :caption: Getting Started

   quickstart

.. toctree::
   :maxdepth: 2
   :caption: Features

   features/batch-pipelines
   features/streaming-pipelines
   features/resilience
   features/hooks
   features/data-quality
   features/audit
   features/secrets
   features/checkpoint

.. toctree::
   :maxdepth: 2
   :caption: Reference

   architecture
   api
   glossary

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
