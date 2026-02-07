PySpark Pipeline Framework
==========================

Configuration-driven PySpark pipeline framework with HOCON configuration,
resilience patterns, lifecycle hooks, and streaming support.

Build batch and streaming data pipelines using composable components, HOCON
configuration files, and a rich set of operational features including retry
policies, circuit breakers, data quality checks, audit trails, secrets
management, and checkpoint/resume.

.. note::

   **Scala/JVM users** may also be interested in
   `spark-pipeline-framework <https://github.com/dwsmith1983/spark-pipeline-framework>`_,
   the Scala implementation using PureConfig and Typesafe Config.

**Supported Versions:**

- Python 3.10 -- 3.13
- Apache Spark 3.4+ (optional runtime dependency)

.. toctree::
   :maxdepth: 2
   :caption: Getting Started

   getting-started
   scope

.. toctree::
   :maxdepth: 2
   :caption: User Guide

   user-guide/components
   user-guide/configuration
   user-guide/config-validation
   user-guide/streaming
   user-guide/resilience
   user-guide/hooks
   user-guide/data-quality
   user-guide/audit
   user-guide/secrets
   user-guide/schema-contracts
   user-guide/checkpoint
   user-guide/migration

.. toctree::
   :maxdepth: 2
   :caption: Production

   deployment
   contributing

.. toctree::
   :maxdepth: 2
   :caption: Reference

   architecture
   api/index
   glossary

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
