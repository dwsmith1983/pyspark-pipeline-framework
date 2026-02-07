Lifecycle Hooks
===============

Hooks receive callbacks at pipeline and component lifecycle events. Use them
for logging, metrics collection, data quality checks, audit trails, and
checkpoint management.

Available Hooks
---------------

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Hook
     - Description
   * - ``LoggingHooks``
     - Logs lifecycle events via ``structlog``
   * - ``MetricsHooks``
     - Collects timing and retry counts
   * - ``DataQualityHooks``
     - Runs data quality checks at lifecycle points
   * - ``AuditHooks``
     - Emits audit events for compliance
   * - ``CheckpointHooks``
     - Saves checkpoint state for resume

Basic Usage
-----------

.. code-block:: python

   from pyspark_pipeline_framework.runner import (
       CompositeHooks, LoggingHooks, MetricsHooks,
       SimplePipelineRunner,
   )

   hooks = CompositeHooks(LoggingHooks(), MetricsHooks())
   runner = SimplePipelineRunner(config, hooks=hooks)
   result = runner.run()

Hook Lifecycle
--------------

Hooks are called at the following points during pipeline execution:

.. code-block:: text

   on_pipeline_start
   │
   ├── on_component_start("read_raw")
   │   └── on_component_end("read_raw", success)
   │
   ├── on_component_start("transform")
   │   ├── on_component_retry("transform", attempt=2)
   │   └── on_component_end("transform", success)
   │
   ├── on_component_start("write")
   │   └── on_component_end("write", success)
   │
   on_pipeline_end(success)

CompositeHooks
--------------

Combine multiple hooks into a single hooks instance. All hooks receive every
event in registration order:

.. code-block:: python

   from pyspark_pipeline_framework.runner import (
       CompositeHooks, LoggingHooks, MetricsHooks,
       DataQualityHooks, AuditHooks,
   )
   from pyspark_pipeline_framework.core.audit import LoggingAuditSink

   hooks = CompositeHooks(
       LoggingHooks(),
       MetricsHooks(),
       DataQualityHooks(spark_wrapper),
       AuditHooks(LoggingAuditSink()),
   )

Custom Hooks
------------

Implement the ``PipelineHooks`` protocol to create custom hooks:

.. code-block:: python

   from pyspark_pipeline_framework.runner.hooks import PipelineHooks


   class SlackNotificationHooks(PipelineHooks):
       def on_pipeline_start(self, pipeline_name: str) -> None:
           send_slack(f"Pipeline {pipeline_name} started")

       def on_pipeline_end(
           self, pipeline_name: str, success: bool, error: Exception | None = None,
       ) -> None:
           emoji = "white_check_mark" if success else "x"
           send_slack(f":{emoji}: Pipeline {pipeline_name} finished")

       def on_component_start(self, component_name: str) -> None:
           pass

       def on_component_end(
           self, component_name: str, success: bool, error: Exception | None = None,
       ) -> None:
           pass

       def on_component_retry(
           self, component_name: str, attempt: int, error: Exception | None = None,
       ) -> None:
           pass

NoOpHooks
---------

The default hooks implementation that does nothing. Used when no hooks are
provided:

.. code-block:: python

   from pyspark_pipeline_framework.runner.hooks import NoOpHooks

   runner = SimplePipelineRunner(config, hooks=NoOpHooks())

See Also
--------

- :doc:`/user-guide/data-quality` - Data quality checks
- :doc:`/user-guide/audit` - Audit trail
- :doc:`/user-guide/checkpoint` - Checkpoint and resume
