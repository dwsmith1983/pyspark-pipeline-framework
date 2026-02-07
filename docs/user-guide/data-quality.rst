Data Quality Checks
===================

Run data quality checks at pipeline lifecycle points. Checks can run before
or after components, and can be configured to warn or fail the pipeline.

Basic Usage
-----------

.. code-block:: python

   from pyspark_pipeline_framework.core.quality import row_count_check, null_check
   from pyspark_pipeline_framework.runner import (
       DataQualityHooks, CompositeHooks, LoggingHooks,
       SimplePipelineRunner,
   )

   dq = DataQualityHooks(spark_wrapper)
   dq.register(row_count_check("curated.customers", min_rows=100))
   dq.register(null_check("curated.customers", "email", max_null_pct=5.0))

   hooks = CompositeHooks(LoggingHooks(), dq)
   runner = SimplePipelineRunner(config, hooks=hooks)
   result = runner.run()

Built-in Checks
---------------

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Check
     - Description
   * - ``row_count_check``
     - Verify a table has at least ``min_rows`` rows
   * - ``null_check``
     - Verify a column's null percentage is below ``max_null_pct``

Check Timing
------------

Control when checks run relative to component execution:

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Timing
     - Description
   * - ``PRE``
     - Run before the component
   * - ``POST``
     - Run after the component (default)

Failure Mode
------------

Control what happens when a check fails:

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Mode
     - Description
   * - ``WARN``
     - Log a warning but continue pipeline execution
   * - ``FAIL``
     - Stop the pipeline with an error (default)

Custom Checks
-------------

Create custom data quality checks by implementing the ``DataQualityCheck``
protocol:

.. code-block:: python

   from pyspark_pipeline_framework.core.quality.types import (
       CheckTiming, FailureMode, CheckResult,
   )
   from pyspark_pipeline_framework.core.quality.checks import DataQualityCheck


   class FreshnessCheck(DataQualityCheck):
       """Check that data was updated within the last N hours."""

       def __init__(
           self,
           table: str,
           timestamp_col: str,
           max_age_hours: int = 24,
       ) -> None:
           self._table = table
           self._timestamp_col = timestamp_col
           self._max_age_hours = max_age_hours

       @property
       def name(self) -> str:
           return f"freshness({self._table}.{self._timestamp_col})"

       @property
       def timing(self) -> CheckTiming:
           return CheckTiming.POST

       @property
       def failure_mode(self) -> FailureMode:
           return FailureMode.FAIL

       def execute(self, spark) -> CheckResult:
           # Your check logic here
           ...

See Also
--------

- :doc:`/user-guide/hooks` - Lifecycle hooks
- :doc:`/user-guide/audit` - Audit trail
