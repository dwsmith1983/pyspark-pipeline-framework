Quick Start
===========

Requirements
------------

- Python 3.10 - 3.13
- Apache Spark 3.4+ (optional -- only needed at runtime)

Installation
------------

.. code-block:: bash

   pip install pyspark-pipeline-framework

**With PySpark included** (for users without a managed Spark environment):

.. code-block:: bash

   pip install pyspark-pipeline-framework[spark]

**With all optional providers** (AWS Secrets Manager, HashiCorp Vault, metrics):

.. code-block:: bash

   pip install pyspark-pipeline-framework[all]

You can combine extras: ``pip install pyspark-pipeline-framework[spark,aws,vault]``

For development:

.. code-block:: bash

   git clone https://github.com/dwsmith1983/pyspark-pipeline-framework.git
   cd pyspark-pipeline-framework
   pip install -e ".[dev]"

Basic Usage
-----------

Define a pipeline in HOCON and run it:

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

Run the pipeline:

.. code-block:: python

   from pyspark_pipeline_framework.runner import SimplePipelineRunner

   # Load pipeline from HOCON config and run
   runner = SimplePipelineRunner.from_file("pipeline.conf")
   result = runner.run()

   print(result.status)            # PipelineResultStatus.SUCCESS
   print(result.total_duration_ms) # 1234

Creating Components
-------------------

Extend ``DataFlow`` and implement ``name`` and ``run()``:

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

The framework injects a ``SparkSession`` via ``set_spark_session()`` before
calling ``run()``. Access it through ``self.spark``.

Testing Components
------------------

Use ``MagicMock`` for the ``SparkSession``:

.. code-block:: python

   from unittest.mock import MagicMock
   from my_project.components import MyTransform


   def test_my_transform():
       spark = MagicMock()
       df = MagicMock()
       spark.sql.return_value = df

       comp = MyTransform(output_view="result")
       comp.set_spark_session(spark)
       comp.run()

       spark.sql.assert_called_once()
       df.createOrReplaceTempView.assert_called_once_with("result")

Configuration Reference
-----------------------

.. list-table::
   :header-rows: 1
   :widths: 25 10 10 55

   * - Field
     - Type
     - Required
     - Description
   * - ``name``
     - string
     - yes
     - Pipeline name
   * - ``version``
     - string
     - yes
     - Pipeline version
   * - ``spark.app_name``
     - string
     - yes
     - Spark application name
   * - ``spark.master``
     - string
     - no
     - Spark master URL
   * - ``components[].name``
     - string
     - yes
     - Unique component name
   * - ``components[].component_type``
     - enum
     - yes
     - ``source``, ``transformation``, ``sink``
   * - ``components[].class_path``
     - string
     - yes
     - Python class to instantiate
   * - ``components[].config``
     - object
     - no
     - Component-specific configuration
   * - ``components[].depends_on``
     - list
     - no
     - Names of prerequisite components
   * - ``components[].enabled``
     - bool
     - no
     - Enable/disable (default: true)
   * - ``components[].retry``
     - object
     - no
     - Retry policy configuration
   * - ``components[].circuit_breaker``
     - object
     - no
     - Circuit breaker configuration

Next Steps
----------

- :doc:`/features/batch-pipelines` - Built-in batch components and patterns
- :doc:`/features/streaming-pipelines` - Kafka, Delta, and file streaming
- :doc:`/features/resilience` - Retry and circuit breaker configuration
- :doc:`/features/hooks` - Lifecycle hooks for logging, metrics, and more
- :doc:`/features/data-quality` - Data quality checks
- :doc:`/features/secrets` - Secrets management with pluggable providers
