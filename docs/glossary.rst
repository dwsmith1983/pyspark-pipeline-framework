Glossary
========

.. glossary::
   :sorted:

   Circuit Breaker
      A resilience pattern that tracks consecutive failures and temporarily
      rejects calls when a failure threshold is reached. Prevents cascading
      failures by giving a failing component time to recover.

   Checkpoint
      A snapshot of pipeline execution state recording which components have
      completed successfully. Used to resume failed pipelines from the last
      successful step.

   Component
      A unit of work in a pipeline. Components extend ``PipelineComponent``
      (or ``DataFlow``) and implement a ``run()`` method.

   CompositeHooks
      A hooks implementation that delegates to multiple child hooks, allowing
      logging, metrics, audit, and data quality hooks to coexist.

   ConfigurableInstance
      A runtime-checkable protocol for components that can be instantiated
      from a configuration dictionary via ``from_config()``.

   DataFlow
      An abstract base class for Spark-aware pipeline components. Extends
      ``PipelineComponent`` with SparkSession injection and a logger.

   Data Quality Check
      A validation rule that runs at a pipeline lifecycle point (before or
      after a component) to verify data integrity.

   HOCON
      Human-Optimized Config Object Notation. A superset of JSON used for
      pipeline configuration files. Parsed by the ``dataconf`` library.

   Hook
      A callback interface that receives lifecycle events during pipeline
      execution (start, end, retry, etc.).

   Pipeline Fingerprint
      A hash of the pipeline configuration used to detect configuration
      changes between runs. Stale checkpoints are invalidated when the
      fingerprint changes.

   PipelineComponent
      The abstract base class for all pipeline components. Defines the
      ``name`` property and ``run()`` method.

   RetryExecutor
      Executes a callable with exponential backoff and optional jitter.
      Configurable per-component via HOCON ``retry`` blocks.

   SchemaAwareDataFlow
      A ``DataFlow`` subclass that declares input and output schemas and
      validates them automatically before and after ``run()``.

   SchemaContract
      A protocol for components that declare input and output schemas.
      Not runtime-checkable; use ``hasattr`` checks.

   SecretsProvider
      An abstract base class for secret backends. Implementations include
      ``EnvSecretsProvider``, ``AwsSecretsProvider``, and
      ``VaultSecretsProvider``.

   SimplePipelineRunner
      The main pipeline orchestrator. Loads components from configuration,
      resolves dependencies, and executes them in order with resilience and
      hooks.

   SparkSessionWrapper
      A thread-safe singleton that manages the ``SparkSession`` lifecycle,
      including Spark Connect support.

   StreamingPipeline
      An abstract base class for Spark Structured Streaming pipelines that
      combines a ``StreamingSource``, optional transform, and
      ``StreamingSink``.
