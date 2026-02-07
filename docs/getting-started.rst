Getting Started
===============

Requirements
------------

- Python 3.10 -- 3.13
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

Modules
-------

The framework is organized into three layers:

.. code-block:: text

   ┌──────────────────────────────────────────┐
   │                  runner/                  │
   │  SimplePipelineRunner, Hooks, Checkpoint  │
   ├──────────────────────────────────────────┤
   │                 runtime/                  │
   │  DataFlow, Streaming, Loader, Session     │
   ├──────────────────────────────────────────┤
   │                  core/                    │
   │  Config, Component, Schema, Resilience,   │
   │  Quality, Audit, Secrets, Metrics         │
   └──────────────────────────────────────────┘
     core/ has ZERO Spark dependency at import time

- **core/** -- Configuration models, component abstractions, resilience patterns,
  data quality, audit, secrets, and metrics. No PySpark dependency at import time.
- **runtime/** -- Spark-aware components: ``DataFlow``, ``StreamingPipeline``,
  ``SparkSessionWrapper``, dynamic loader, and schema converter.
- **runner/** -- Pipeline orchestration: ``SimplePipelineRunner``, lifecycle hooks,
  checkpoint/resume, and result models.

Quick Example
-------------

**1. Create a pipeline component:**

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

**2. Create a pipeline configuration file** (``pipeline.conf``):

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

**3. Run the pipeline:**

.. code-block:: python

   from pyspark_pipeline_framework.runner import SimplePipelineRunner

   runner = SimplePipelineRunner.from_file("pipeline.conf")
   result = runner.run()

   print(result.status)            # PipelineResultStatus.SUCCESS
   print(result.total_duration_ms) # 1234

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

Next Steps
----------

- :doc:`/user-guide/components` -- Creating and configuring pipeline components
- :doc:`/user-guide/configuration` -- HOCON configuration reference
- :doc:`/user-guide/streaming` -- Kafka, Delta, and file streaming
- :doc:`/user-guide/resilience` -- Retry and circuit breaker configuration
- :doc:`/user-guide/hooks` -- Lifecycle hooks for logging, metrics, and more
