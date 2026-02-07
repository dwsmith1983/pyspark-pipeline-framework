Audit Trail
===========

Record an immutable audit trail of pipeline execution for compliance and
debugging. Audit events capture component starts, completions, failures,
and configuration details.

Basic Usage
-----------

.. code-block:: python

   from pyspark_pipeline_framework.core.audit import (
       LoggingAuditSink, FileAuditSink, CompositeAuditSink,
   )
   from pyspark_pipeline_framework.runner import (
       AuditHooks, CompositeHooks, LoggingHooks,
       SimplePipelineRunner,
   )

   audit_sink = CompositeAuditSink(
       LoggingAuditSink(),
       FileAuditSink("/var/log/pipeline-audit.jsonl"),
   )
   hooks = CompositeHooks(LoggingHooks(), AuditHooks(audit_sink))
   runner = SimplePipelineRunner(config, hooks=hooks)
   result = runner.run()

Audit Sinks
-----------

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Sink
     - Description
   * - ``LoggingAuditSink``
     - Writes audit events to the Python logger
   * - ``FileAuditSink``
     - Appends audit events as JSON lines to a file
   * - ``CompositeAuditSink``
     - Fans out events to multiple sinks

Audit Events
------------

Each event includes:

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Field
     - Description
   * - ``action``
     - ``PIPELINE_START``, ``COMPONENT_START``, ``COMPONENT_END``, etc.
   * - ``status``
     - ``SUCCESS``, ``FAILURE``, ``SKIPPED``
   * - ``timestamp``
     - ISO 8601 timestamp
   * - ``pipeline_name``
     - Name of the pipeline
   * - ``component_name``
     - Name of the component (if applicable)
   * - ``duration_ms``
     - Execution duration in milliseconds
   * - ``metadata``
     - Additional context (error messages, config, etc.)

Configuration Filtering
-----------------------

Use ``ConfigFilter`` to redact sensitive values before they appear in audit
events:

.. code-block:: python

   from pyspark_pipeline_framework.core.audit.filters import ConfigFilter

   # Redact keys containing "password", "secret", or "key"
   filter = ConfigFilter(redact_patterns=["password", "secret", "key"])
   sink = FileAuditSink("/var/log/audit.jsonl", config_filter=filter)

See Also
--------

- :doc:`/user-guide/hooks` - Lifecycle hooks
- :doc:`/user-guide/data-quality` - Data quality checks
- :doc:`/user-guide/secrets` - Secrets management
