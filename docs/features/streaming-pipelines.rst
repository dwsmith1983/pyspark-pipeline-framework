Streaming Pipelines
===================

pyspark-pipeline-framework supports Spark Structured Streaming through
composable source, sink, and pipeline abstractions. Define a streaming pipeline
by combining a ``StreamingSource``, an optional transform, and a
``StreamingSink``.

Creating a Streaming Pipeline
-----------------------------

Extend ``StreamingPipeline`` and provide a source, sink, and optional
transform:

.. code-block:: python

   from pyspark_pipeline_framework.runtime.streaming.base import (
       StreamingPipeline, StreamingSource, StreamingSink,
       TriggerConfig, TriggerType,
   )
   from pyspark_pipeline_framework.runtime.streaming.sources import (
       KafkaStreamingSource,
   )
   from pyspark_pipeline_framework.runtime.streaming.sinks import (
       DeltaStreamingSink,
   )


   class EventIngestion(StreamingPipeline):
       def __init__(self) -> None:
           super().__init__()
           self._source = KafkaStreamingSource(
               bootstrap_servers="broker:9092", topics="events",
           )
           self._sink = DeltaStreamingSink(
               path="/data/delta/events",
               checkpoint_location="/checkpoints/events",
           )

       @property
       def name(self) -> str:
           return "EventIngestion"

       @property
       def source(self) -> StreamingSource:
           return self._source

       @property
       def sink(self) -> StreamingSink:
           return self._sink

       @property
       def trigger(self) -> TriggerConfig:
           return TriggerConfig(TriggerType.PROCESSING_TIME, "30 seconds")

       def transform(self, df):
           # Parse JSON value from Kafka
           return df.selectExpr("CAST(value AS STRING) AS raw_json")

Built-in Sources
----------------

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Source
     - Description
   * - ``KafkaStreamingSource``
     - Reads from Kafka topics
   * - ``FileStreamingSource``
     - Reads files from a directory (JSON, CSV, Parquet)
   * - ``DeltaStreamingSource``
     - Reads from a Delta Lake table
   * - ``IcebergStreamingSource``
     - Reads from an Iceberg table
   * - ``RateStreamingSource``
     - Generates synthetic data for testing

Built-in Sinks
--------------

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Sink
     - Description
   * - ``KafkaStreamingSink``
     - Writes to Kafka topics
   * - ``DeltaStreamingSink``
     - Writes to a Delta Lake table
   * - ``ConsoleStreamingSink``
     - Prints to console (debugging)
   * - ``IcebergStreamingSink``
     - Writes to an Iceberg table
   * - ``FileStreamingSink``
     - Writes to files (JSON, CSV, Parquet)

Example Pipelines
-----------------

Two reference pipelines are included:

.. code-block:: python

   from pyspark_pipeline_framework.examples.streaming import (
       FileToConsolePipeline,
       KafkaToDeltaPipeline,
   )

Running a Stream
----------------

.. code-block:: python

   pipeline.set_spark_session(spark)

   # Blocking -- runs until terminated
   pipeline.run()

   # Non-blocking -- returns StreamingQuery handle
   query = pipeline.start_stream()
   query.awaitTermination(timeout=60)

Trigger Configuration
---------------------

Control how frequently the stream processes micro-batches:

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Trigger Type
     - Description
   * - ``PROCESSING_TIME``
     - Fixed interval (e.g., ``"30 seconds"``)
   * - ``ONCE``
     - Process all available data, then stop
   * - ``AVAILABLE_NOW``
     - Process all available data in multiple batches, then stop
   * - ``CONTINUOUS``
     - Low-latency continuous processing (experimental)

.. code-block:: python

   from pyspark_pipeline_framework.runtime.streaming.base import (
       TriggerConfig, TriggerType,
   )

   # Process every 30 seconds
   trigger = TriggerConfig(TriggerType.PROCESSING_TIME, "30 seconds")

   # One-shot batch
   trigger = TriggerConfig(TriggerType.ONCE)

See Also
--------

- :doc:`/features/batch-pipelines` - Batch pipeline guide
- :doc:`/features/hooks` - Lifecycle hooks
