Configuration
=============

pyspark-pipeline-framework uses `HOCON <https://github.com/lightbend/config>`_
(Human-Optimized Config Object Notation) for declarative pipeline definitions.
Configuration is loaded through `dataconf <https://pypi.org/project/dataconf/>`_
into typed Python dataclasses, giving you both the flexibility of HOCON and
compile-time type safety.

Configuration Structure
-----------------------

A pipeline configuration file has three required top-level fields and several
optional ones:

.. code-block:: javascript

   {
     name: "customer-etl"
     version: "1.0.0"

     spark {
       app_name: "Customer ETL"
       master: "local[*]"
     }

     components: [
       {
         name: "read_raw"
         component_type: source
         class_path: "my.module.ReadRaw"
         config { table_name: "raw.customers" }
       }
     ]
   }

The configuration maps to three primary dataclasses:

- :class:`~pyspark_pipeline_framework.core.config.pipeline.PipelineConfig` --
  top-level pipeline definition
- :class:`~pyspark_pipeline_framework.core.config.spark.SparkConfig` --
  Spark runtime settings
- :class:`~pyspark_pipeline_framework.core.config.component.ComponentConfig` --
  individual component definitions

Spark Configuration
-------------------

The ``spark`` block controls the Spark runtime. All fields except ``app_name``
have sensible defaults:

.. list-table::
   :header-rows: 1
   :widths: 25 15 60

   * - Field
     - Default
     - Description
   * - ``app_name``
     - *(required)*
     - Spark application name
   * - ``master``
     - ``"local[*]"``
     - Spark master URL -- ``local[*]``, ``yarn``, ``k8s://...``
   * - ``deploy_mode``
     - ``client``
     - Deployment mode: ``client`` or ``cluster``
   * - ``driver_memory``
     - ``"2g"``
     - Memory allocated to the driver
   * - ``driver_cores``
     - ``1``
     - Number of cores for the driver
   * - ``executor_memory``
     - ``"4g"``
     - Memory allocated to each executor
   * - ``executor_cores``
     - ``2``
     - Number of cores per executor
   * - ``num_executors``
     - ``2``
     - Number of executors (ignored when ``dynamic_allocation`` is true)
   * - ``dynamic_allocation``
     - ``false``
     - Enable Spark dynamic executor allocation
   * - ``spark_conf``
     - ``{}``
     - Additional ``spark.*`` properties as key-value pairs
   * - ``connect_string``
     - ``null``
     - Spark Connect remote URL (see below)

.. code-block:: javascript

   spark {
     app_name: "Daily ETL"
     master: "yarn"
     deploy_mode: "cluster"
     driver_memory: "4g"
     executor_memory: "8g"
     executor_cores: 4
     num_executors: 10
     spark_conf {
       "spark.sql.shuffle.partitions": "200"
       "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
     }
   }

Spark Connect Support
---------------------

For Spark 3.4+ and Databricks Connect, set the ``connect_string`` field
instead of ``master``:

.. code-block:: javascript

   spark {
     app_name: "Remote Pipeline"
     connect_string: "sc://localhost:15002"
   }

When ``connect_string`` is set, the
:class:`~pyspark_pipeline_framework.runtime.session.SparkSessionWrapper`
creates the session via ``SparkSession.builder.remote(connect_string)``
rather than using the traditional master URL.

.. code-block:: text

   # Databricks Connect example
   spark {
     app_name: "Databricks Pipeline"
     connect_string: "sc://my-workspace.cloud.databricks.com:443/;token=dapi..."
   }

Pipeline Configuration
----------------------

:class:`~pyspark_pipeline_framework.core.config.pipeline.PipelineConfig`
is the top-level object. Required fields are ``name``, ``version``,
``spark``, and ``components``:

.. list-table::
   :header-rows: 1
   :widths: 25 15 60

   * - Field
     - Default
     - Description
   * - ``name``
     - *(required)*
     - Pipeline name (must be non-empty)
   * - ``version``
     - *(required)*
     - Pipeline version string
   * - ``spark``
     - *(required)*
     - Spark runtime configuration block
   * - ``components``
     - *(required)*
     - List of component definitions (at least one)
   * - ``environment``
     - ``dev``
     - Deployment environment: ``dev``, ``staging``, ``prod``, ``test``
   * - ``mode``
     - ``batch``
     - Execution mode: ``batch`` or ``streaming``
   * - ``hooks``
     - ``HooksConfig()``
     - Lifecycle hooks configuration (logging, metrics, audit)
   * - ``secrets``
     - ``null``
     - Secrets management configuration
   * - ``tags``
     - ``{}``
     - Arbitrary key-value metadata tags

.. code-block:: javascript

   {
     name: "customer-etl"
     version: "2.1.0"
     environment: "prod"
     mode: "batch"

     spark { app_name: "Customer ETL" }

     hooks {
       logging { level: "INFO", format: "json" }
       metrics { enabled: true, backend: "prometheus" }
       audit { enabled: true, audit_trail_path: "/var/log/audit" }
     }

     secrets {
       provider: "aws_secrets_manager"
       aws_region: "us-east-1"
       cache_ttl_seconds: 600
     }

     tags {
       team: "data-engineering"
       domain: "customer"
     }

     components: [ ... ]
   }

The pipeline validates on construction:

- ``name`` and ``version`` must be non-empty.
- ``components`` must contain at least one entry.
- Component names must be unique.
- Dependencies must reference existing components.
- Circular dependencies are detected and rejected.

Component Configuration
-----------------------

Each entry in the ``components`` list maps to a
:class:`~pyspark_pipeline_framework.core.config.component.ComponentConfig`:

.. list-table::
   :header-rows: 1
   :widths: 25 15 60

   * - Field
     - Default
     - Description
   * - ``name``
     - *(required)*
     - Unique component name within the pipeline
   * - ``component_type``
     - *(required)*
     - Type: ``source``, ``transformation``, or ``sink``
   * - ``class_path``
     - *(required)*
     - Fully qualified Python class path
   * - ``config``
     - ``{}``
     - Component-specific configuration dict
   * - ``depends_on``
     - ``[]``
     - Names of prerequisite components
   * - ``retry``
     - ``null``
     - Retry configuration (see :doc:`/user-guide/resilience`)
   * - ``circuit_breaker``
     - ``null``
     - Circuit breaker configuration (see :doc:`/user-guide/resilience`)
   * - ``enabled``
     - ``true``
     - Set to ``false`` to skip this component

.. code-block:: javascript

   components: [
     {
       name: "read_raw"
       component_type: source
       class_path: "my.pipelines.ReadRaw"
       config {
         table_name: "raw.customers"
         output_view: "raw_customers"
       }
     },
     {
       name: "transform"
       component_type: transformation
       class_path: "my.pipelines.CleanCustomers"
       depends_on: ["read_raw"]
       config { output_view: "cleaned" }
       retry {
         max_attempts: 3
         initial_delay_seconds: 2.0
       }
     },
     {
       name: "write_curated"
       component_type: sink
       class_path: "my.pipelines.WriteCurated"
       depends_on: ["transform"]
       config {
         input_view: "cleaned"
         output_table: "curated.customers"
       }
       enabled: true
     }
   ]

Configuration Loading
---------------------

Three loader functions are provided in
:mod:`pyspark_pipeline_framework.core.config.loader`:

**From a HOCON file:**

.. code-block:: python

   from pyspark_pipeline_framework.core.config import load_from_file, PipelineConfig

   config = load_from_file("pipeline.conf", PipelineConfig)

**From a HOCON string:**

.. code-block:: python

   from pyspark_pipeline_framework.core.config import load_from_string, PipelineConfig

   hocon = """
   {
     name: "inline-pipeline"
     version: "1.0.0"
     spark { app_name: "Inline" }
     components: [
       {
         name: "step1"
         component_type: source
         class_path: "my.module.Step1"
       }
     ]
   }
   """
   config = load_from_string(hocon, PipelineConfig)

**From environment variables:**

.. code-block:: python

   from pyspark_pipeline_framework.core.config import load_from_env, PipelineConfig

   # Reads PPF_NAME, PPF_VERSION, PPF_SPARK_APP_NAME, etc.
   config = load_from_env("PPF_", PipelineConfig)

Environment Variable Substitution
----------------------------------

HOCON supports environment variable substitution using the ``${?VAR}`` syntax.
The ``?`` makes the substitution optional -- if the variable is not set, the
field retains its default value:

.. code-block:: text

   {
     name: "customer-etl"
     version: "1.0.0"

     spark {
       app_name: ${?SPARK_APP_NAME}
       master: ${?SPARK_MASTER}
       executor_memory: ${?SPARK_EXECUTOR_MEMORY}
     }

     environment: ${?PIPELINE_ENV}

     components: [
       {
         name: "source"
         component_type: source
         class_path: "my.module.Source"
         config {
           table_name: ${TABLE_NAME}   # required -- no '?'
           output_view: "raw"
         }
       }
     ]
   }

Use ``${VAR}`` (without ``?``) when the variable is mandatory. The HOCON
parser will raise an error if the variable is not set.

Pre-built Policies
------------------

The :mod:`~pyspark_pipeline_framework.core.config.presets` module provides
ready-made retry and circuit breaker configurations:

**Retry policies:**

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Preset
     - Description
   * - ``RetryPolicies.NO_RETRY``
     - Single attempt, no retries (``max_attempts=1``)
   * - ``RetryPolicies.DEFAULT``
     - 3 attempts, 1 s initial delay, 2x backoff
   * - ``RetryPolicies.AGGRESSIVE``
     - 5 attempts, 0.5 s initial delay, 1.5x backoff
   * - ``RetryPolicies.CONSERVATIVE``
     - 2 attempts, 5 s initial delay, 30 s max delay

**Circuit breaker configs:**

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Preset
     - Description
   * - ``CircuitBreakerConfigs.DEFAULT``
     - 5 failures, 60 s timeout
   * - ``CircuitBreakerConfigs.SENSITIVE``
     - 3 failures, 120 s timeout
   * - ``CircuitBreakerConfigs.RESILIENT``
     - 10 failures, 30 s timeout

.. code-block:: python

   from pyspark_pipeline_framework.core.config import (
       RetryPolicies, CircuitBreakerConfigs,
   )

   # Use a pre-built retry policy
   retry = RetryPolicies.AGGRESSIVE
   print(retry.max_attempts)          # 5
   print(retry.initial_delay_seconds) # 0.5

   # Use a pre-built circuit breaker config
   cb = CircuitBreakerConfigs.SENSITIVE
   print(cb.failure_threshold)  # 3
   print(cb.timeout_seconds)    # 120.0

See Also
--------

- :doc:`/user-guide/config-validation` - Validate configuration before execution
- :doc:`/user-guide/components` - Building pipeline components
- :doc:`/user-guide/resilience` - Retry and circuit breaker patterns
- :doc:`/user-guide/secrets` - Secrets management configuration
