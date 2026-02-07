Components
==========

pyspark-pipeline-framework uses a component-based model for building
pipelines. Each pipeline is a sequence of components wired together through
HOCON configuration and executed by ``SimplePipelineRunner``.

Component Architecture
----------------------

All components descend from ``PipelineComponent``, which defines two
requirements: a ``name`` property and a ``run()`` method. For Spark-aware
components, extend ``DataFlow`` which adds SparkSession injection and a
logger:

.. code-block:: text

   PipelineComponent (ABC)       <-- core/, no Spark dependency
     └── DataFlow (ABC)          <-- runtime/, injects SparkSession
           └── SchemaAwareDataFlow  <-- adds input/output schema

Creating a Component
--------------------

Step 1: Extend ``DataFlow``
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from pyspark_pipeline_framework.runtime.dataflow.base import DataFlow


   class MyTransform(DataFlow):
       def __init__(self, output_view: str) -> None:
           super().__init__()
           self._output_view = output_view

       @property
       def name(self) -> str:
           return "MyTransform"

       @classmethod
       def from_config(cls, config: dict) -> "MyTransform":
           return cls(**config)

       def run(self) -> None:
           df = self.spark.sql("SELECT id, UPPER(name) AS name FROM raw")
           df.createOrReplaceTempView(self._output_view)

Step 2: Implement ``from_config``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``from_config`` classmethod enables the framework to instantiate your
component from HOCON configuration. Any class implementing
``ConfigurableInstance`` can be loaded dynamically:

.. code-block:: python

   @classmethod
   def from_config(cls, config: dict) -> "MyTransform":
       return cls(output_view=config["output_view"])

Step 3: Configure in HOCON
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: javascript

   {
     name: "transform"
     component_type: transformation
     class_path: "my.module.MyTransform"
     config {
       output_view: "cleaned"
     }
   }

SparkSession Access
-------------------

The framework injects a ``SparkSession`` via ``set_spark_session()`` before
calling ``run()``. Access it through ``self.spark``:

.. code-block:: python

   def run(self) -> None:
       df = self.spark.read.table("raw.events")
       df.createOrReplaceTempView("events")

Logging
-------

``DataFlow`` provides a logger via ``self.logger``:

.. code-block:: python

   def run(self) -> None:
       self.logger.info("Processing %d rows", self.spark.table("raw").count())

Built-in Example Components
----------------------------

Three reference components are included for common patterns:

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Component
     - Description
   * - ``ReadTable``
     - Reads a table and registers a temporary view
   * - ``SqlTransform``
     - Executes SQL and registers the result as a view
   * - ``WriteTable``
     - Writes a temporary view to an output table

.. code-block:: python

   from pyspark_pipeline_framework.examples.batch import (
       ReadTable, ReadTableConfig,
       SqlTransform, SqlTransformConfig,
       WriteTable, WriteTableConfig,
   )

HOCON Configuration
-------------------

Pipelines are defined declaratively in HOCON:

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
         class_path: "pyspark_pipeline_framework.examples.batch.ReadTable"
         config {
           table_name: "raw.customers"
           output_view: "raw_customers"
         }
       },
       {
         name: "transform"
         component_type: transformation
         class_path: "pyspark_pipeline_framework.examples.batch.SqlTransform"
         depends_on: ["read_raw"]
         config {
           sql: "SELECT id, UPPER(name) AS name FROM raw_customers"
           output_view: "cleaned"
         }
       },
       {
         name: "write"
         component_type: sink
         class_path: "pyspark_pipeline_framework.examples.batch.WriteTable"
         depends_on: ["transform"]
         config {
           input_view: "cleaned"
           output_table: "curated.customers"
         }
       }
     ]
   }

Running a Pipeline
------------------

.. code-block:: python

   from pyspark_pipeline_framework.runner import SimplePipelineRunner

   runner = SimplePipelineRunner.from_file("pipeline.conf")
   result = runner.run()

   print(result.status)            # PipelineResultStatus.SUCCESS
   print(result.total_duration_ms) # 1234

   # Inspect per-component results
   for comp_result in result.component_results:
       print(f"{comp_result.component_name}: {comp_result.status}")

Dynamic Component Loading
-------------------------

The framework dynamically loads components by ``class_path`` using
``importlib``. Any class on the Python path that implements
``PipelineComponent`` can be loaded:

.. code-block:: python

   from pyspark_pipeline_framework.runtime.loader import (
       load_component_class,
       instantiate_component,
   )

   cls = load_component_class("my.module.MyTransform")
   instance = instantiate_component(cls, config={"output_view": "result"})

Schema-Aware Components
-----------------------

Extend ``SchemaAwareDataFlow`` when you need input/output schema validation:

.. code-block:: python

   from pyspark_pipeline_framework.runtime.dataflow.schema import SchemaAwareDataFlow
   from pyspark_pipeline_framework.core.schema.definition import (
       SchemaDefinition,
       SchemaField,
       DataType,
   )


   class ValidatedTransform(SchemaAwareDataFlow):
       @property
       def name(self) -> str:
           return "ValidatedTransform"

       @property
       def input_schema(self) -> SchemaDefinition:
           return SchemaDefinition(fields=[
               SchemaField(name="id", data_type=DataType.LONG, nullable=False),
               SchemaField(name="name", data_type=DataType.STRING),
           ])

       @property
       def output_schema(self) -> SchemaDefinition:
           return SchemaDefinition(fields=[
               SchemaField(name="id", data_type=DataType.LONG, nullable=False),
               SchemaField(name="name", data_type=DataType.STRING),
           ])

       def run(self) -> None:
           df = self.spark.sql("SELECT id, UPPER(name) AS name FROM raw")
           df.createOrReplaceTempView("validated")

Error Handling
--------------

Components that fail raise ``ComponentExecutionError``. The runner catches
these and delegates to the configured resilience policy (retry, circuit
breaker) and hooks:

.. code-block:: python

   from pyspark_pipeline_framework.core.component.exceptions import (
       ComponentExecutionError,
       ComponentInstantiationError,
   )

Testing Components
------------------

Use ``MagicMock`` for the ``SparkSession``:

.. code-block:: python

   from unittest.mock import MagicMock


   def test_my_transform():
       spark = MagicMock()
       df = MagicMock()
       spark.sql.return_value = df

       comp = MyTransform(output_view="result")
       comp.set_spark_session(spark)
       comp.run()

       spark.sql.assert_called_once()
       df.createOrReplaceTempView.assert_called_once_with("result")

See Also
--------

- :doc:`/user-guide/configuration` -- HOCON configuration reference
- :doc:`/user-guide/streaming` -- Streaming pipeline guide
- :doc:`/user-guide/hooks` -- Lifecycle hooks for logging and metrics
- :doc:`/user-guide/resilience` -- Retry and circuit breaker configuration
- :doc:`/user-guide/schema-contracts` -- Schema validation
