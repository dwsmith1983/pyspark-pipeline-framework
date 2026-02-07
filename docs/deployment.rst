Production Deployment
=====================

This guide covers packaging, deploying, and operating pyspark-pipeline-framework
in production environments including Databricks, Amazon EMR, and Kubernetes.

Packaging Your Application
--------------------------

pip install
~~~~~~~~~~~

Install the framework and your application code in a virtual environment:

.. code-block:: bash

   pip install pyspark-pipeline-framework

Include optional extras based on your environment:

.. code-block:: bash

   # With PySpark (standalone clusters)
   pip install pyspark-pipeline-framework[spark]

   # With AWS Secrets Manager
   pip install pyspark-pipeline-framework[aws]

   # With HashiCorp Vault
   pip install pyspark-pipeline-framework[vault]

   # With metrics (Prometheus, OpenTelemetry)
   pip install pyspark-pipeline-framework[metrics]

   # Everything
   pip install pyspark-pipeline-framework[all]

Building a Wheel
~~~~~~~~~~~~~~~~

Build a distributable wheel for deployment to clusters:

.. code-block:: bash

   # From your project root
   pip install build
   python -m build

   # Output: dist/pyspark_pipeline_framework-0.1.0-py3-none-any.whl

For projects that depend on the framework, include both wheels:

.. code-block:: bash

   # Build your project
   cd my-etl-project
   python -m build

   # Copy both wheels for deployment
   cp dist/my_etl_project-1.0.0-py3-none-any.whl deploy/
   cp /path/to/pyspark_pipeline_framework-0.1.0-py3-none-any.whl deploy/

Docker
~~~~~~

Package your pipeline as a Docker image for Kubernetes or containerized
deployments:

.. code-block:: dockerfile

   FROM python:3.12-slim

   # Install Java (required for PySpark)
   RUN apt-get update && \
       apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
       rm -rf /var/lib/apt/lists/*

   ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

   WORKDIR /app

   # Install dependencies
   COPY requirements.txt .
   RUN pip install --no-cache-dir -r requirements.txt

   # Copy application code and configs
   COPY src/ src/
   COPY conf/ conf/

   # Install application
   COPY pyproject.toml .
   RUN pip install --no-cache-dir .

   ENTRYPOINT ["python", "-m", "pyspark_pipeline_framework.runner.cli"]
   CMD ["--config", "conf/pipeline.conf"]

Build and run:

.. code-block:: bash

   docker build -t my-etl:latest .
   docker run --rm my-etl:latest --config conf/production.conf


Spark Connect Deployment
------------------------

Spark Connect (Spark 3.4+) allows your application to run as a thin client
that connects to a remote Spark server. This decouples driver code from the
Spark cluster.

Configure Spark Connect via HOCON:

.. code-block:: javascript

   {
     name: "customer-etl"
     version: "1.0.0"

     spark {
       app_name: "Customer ETL"
       connect_string: "sc://spark-server:15002"
     }

     components: [ ... ]
   }

Benefits of Spark Connect:

- **Smaller driver footprint** -- No local Spark JVM needed.
- **Language-agnostic server** -- Multiple applications share one Spark cluster.
- **Simpler dependency management** -- Only ``pyspark[connect]`` is needed on
  the client.

Install the connect-only PySpark package:

.. code-block:: bash

   pip install pyspark[connect]
   pip install pyspark-pipeline-framework

The ``SparkSessionWrapper`` detects the ``connect_string`` configuration and
creates a Spark Connect session automatically.


Databricks
----------

Wheel Upload
~~~~~~~~~~~~

Upload your wheel(s) to Databricks and install them on the cluster:

.. code-block:: bash

   # Upload wheel to DBFS
   databricks fs cp \
     dist/my_etl_project-1.0.0-py3-none-any.whl \
     dbfs:/FileStore/jars/my_etl_project-1.0.0-py3-none-any.whl

   databricks fs cp \
     dist/pyspark_pipeline_framework-0.1.0-py3-none-any.whl \
     dbfs:/FileStore/jars/pyspark_pipeline_framework-0.1.0-py3-none-any.whl

Add both wheels as cluster libraries in the Databricks workspace UI, or specify
them in a job definition.

Notebook Usage
~~~~~~~~~~~~~~

Run a pipeline from a Databricks notebook:

.. code-block:: python

   # %pip install /dbfs/FileStore/jars/pyspark_pipeline_framework-0.1.0-py3-none-any.whl

   from pyspark_pipeline_framework.runner import SimplePipelineRunner

   runner = SimplePipelineRunner.from_file("/dbfs/configs/pipeline.conf")
   result = runner.run()

   print(f"Status: {result.status}")
   print(f"Duration: {result.total_duration_ms}ms")

Databricks Connect
~~~~~~~~~~~~~~~~~~

Use Databricks Connect to develop and test locally against a remote Databricks
cluster:

.. code-block:: bash

   pip install databricks-connect
   pip install pyspark-pipeline-framework

Configure the connection via HOCON:

.. code-block:: javascript

   {
     spark {
       app_name: "Local Dev"
       connect_string: "sc://my-workspace.cloud.databricks.com:443"
       config {
         "spark.databricks.service.token": ${?DATABRICKS_TOKEN}
         "spark.databricks.service.clusterId": ${?DATABRICKS_CLUSTER_ID}
       }
     }
   }


Amazon EMR
----------

spark-submit
~~~~~~~~~~~~

Submit your pipeline to an EMR cluster using ``spark-submit``:

.. code-block:: bash

   spark-submit \
     --master yarn \
     --deploy-mode cluster \
     --py-files pyspark_pipeline_framework-0.1.0-py3-none-any.whl,my_etl-1.0.0-py3-none-any.whl \
     --files conf/production.conf \
     main.py --config production.conf

Where ``main.py`` is a thin entry point:

.. code-block:: python

   """Entry point for spark-submit."""
   import argparse

   from pyspark_pipeline_framework.runner import SimplePipelineRunner


   def main() -> None:
       parser = argparse.ArgumentParser()
       parser.add_argument("--config", required=True)
       args = parser.parse_args()

       runner = SimplePipelineRunner.from_file(args.config)
       result = runner.run()
       if not result.success:
           raise SystemExit(1)


   if __name__ == "__main__":
       main()

EMR Serverless
~~~~~~~~~~~~~~

For EMR Serverless, package your dependencies into a virtual environment
archive:

.. code-block:: bash

   # Create a venv archive for EMR Serverless
   python -m venv pyspark-env
   source pyspark-env/bin/activate
   pip install pyspark-pipeline-framework[aws]
   pip install my-etl-project

   # Package the venv
   pip install venv-pack
   venv-pack -o pyspark-env.tar.gz

   # Upload to S3
   aws s3 cp pyspark-env.tar.gz s3://my-bucket/emr/envs/

Submit the job:

.. code-block:: bash

   aws emr-serverless start-job-run \
     --application-id $APP_ID \
     --execution-role-arn $ROLE_ARN \
     --job-driver '{
       "sparkSubmit": {
         "entryPoint": "s3://my-bucket/emr/main.py",
         "entryPointArguments": ["--config", "s3://my-bucket/emr/conf/production.conf"],
         "sparkSubmitParameters": "--conf spark.archives=s3://my-bucket/emr/envs/pyspark-env.tar.gz#environment --conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python --conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python"
       }
     }'


Kubernetes
----------

Deploy pipelines to Kubernetes using the Spark Kubernetes scheduler:

.. code-block:: bash

   spark-submit \
     --master k8s://https://k8s-api-server:6443 \
     --deploy-mode cluster \
     --name customer-etl \
     --conf spark.kubernetes.container.image=my-etl:latest \
     --conf spark.kubernetes.namespace=spark-jobs \
     --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
     --conf spark.kubernetes.file.upload.path=s3a://my-bucket/spark-uploads \
     --py-files local:///app/dist/pyspark_pipeline_framework-0.1.0-py3-none-any.whl \
     local:///app/main.py --config /app/conf/production.conf

Use the Docker image from the `Docker`_ section above. Ensure the image
includes your application code, the framework wheel, and HOCON config files.

For Spark on Kubernetes Operator (``spark-operator``), define a
``SparkApplication`` custom resource:

.. code-block:: yaml

   apiVersion: sparkoperator.k8s.io/v1beta2
   kind: SparkApplication
   metadata:
     name: customer-etl
     namespace: spark-jobs
   spec:
     type: Python
     mode: cluster
     image: my-etl:latest
     mainApplicationFile: local:///app/main.py
     arguments:
       - "--config"
       - "/app/conf/production.conf"
     sparkVersion: "3.5.0"
     driver:
       cores: 1
       memory: "2g"
       serviceAccount: spark
     executor:
       cores: 2
       instances: 3
       memory: "4g"


Configuration Management
------------------------

Use HOCON's include and substitution features to manage environment-specific
configurations.

Base configuration (``conf/base.conf``):

.. code-block:: javascript

   {
     name: "customer-etl"
     version: "1.0.0"

     spark {
       app_name: "Customer ETL"
       config {
         "spark.sql.adaptive.enabled": true
         "spark.sql.shuffle.partitions": 200
       }
     }

     components: [
       {
         name: "read_raw"
         component_type: source
         class_path: "my_project.components.ReadCustomers"
         config {
           table_name: ${tables.raw_customers}
           output_view: "raw"
         }
       },
       {
         name: "transform"
         component_type: transformation
         class_path: "my_project.components.CleanCustomers"
         depends_on: ["read_raw"]
         config {
           output_view: "cleaned"
         }
       },
       {
         name: "write"
         component_type: sink
         class_path: "my_project.components.WriteCustomers"
         depends_on: ["transform"]
         config {
           input_view: "cleaned"
           output_table: ${tables.curated_customers}
         }
       }
     ]
   }

Development override (``conf/dev.conf``):

.. code-block:: javascript

   include "base.conf"

   spark {
     master: "local[*]"
     config {
       "spark.sql.shuffle.partitions": 4
     }
   }

   tables {
     raw_customers: "dev.raw_customers"
     curated_customers: "dev.curated_customers"
   }

Production override (``conf/production.conf``):

.. code-block:: javascript

   include "base.conf"

   spark {
     master: "yarn"
     config {
       "spark.sql.shuffle.partitions": 1000
       "spark.executor.memory": "8g"
       "spark.executor.cores": 4
     }
   }

   tables {
     raw_customers: "prod.raw_customers"
     curated_customers: "prod.curated_customers"
   }

Environment variable substitution is supported with ``${?VAR}`` (optional)
or ``${VAR}`` (required):

.. code-block:: javascript

   spark {
     master: ${?SPARK_MASTER}
     config {
       "spark.hadoop.fs.s3a.access.key": ${?AWS_ACCESS_KEY_ID}
       "spark.hadoop.fs.s3a.secret.key": ${?AWS_SECRET_ACCESS_KEY}
     }
   }


Secrets in Production
---------------------

Use ``SecretsResolver`` with pluggable providers to avoid embedding secrets in
configuration files.

.. code-block:: python

   from pyspark_pipeline_framework.core.secrets import (
       SecretsResolver, SecretsCache, SecretsReference,
       EnvSecretsProvider, AwsSecretsProvider,
   )

   # Register providers
   resolver = SecretsResolver()
   resolver.register(EnvSecretsProvider())
   resolver.register(AwsSecretsProvider(region_name="us-east-1"))

   # Wrap in a cache for performance (TTL 5 minutes)
   cache = SecretsCache(resolver, ttl_seconds=300)

   # Resolve a secret
   result = cache.resolve(
       SecretsReference(provider="aws", key="prod/db-password"),
   )

**Provider selection by environment:**

.. list-table::
   :header-rows: 1
   :widths: 25 25 50

   * - Environment
     - Provider
     - Notes
   * - Local development
     - ``EnvSecretsProvider``
     - Read from ``.env`` or shell exports
   * - AWS (EMR, EKS)
     - ``AwsSecretsProvider``
     - Reads from AWS Secrets Manager via IAM role
   * - HashiCorp Vault
     - ``VaultSecretsProvider``
     - Token or AppRole authentication

``SecretsCache`` is thread-safe and avoids repeated network calls within the
configured TTL window.


Monitoring
----------

MetricsHooks
~~~~~~~~~~~~

``MetricsHooks`` collects timing and retry metrics for each component. Pair it
with a ``MeterRegistry`` to export to Prometheus, Datadog, or CloudWatch:

.. code-block:: python

   from pyspark_pipeline_framework.runner import (
       CompositeHooks, LoggingHooks, MetricsHooks,
       SimplePipelineRunner,
   )

   hooks = CompositeHooks(
       LoggingHooks(),
       MetricsHooks(),
   )

   runner = SimplePipelineRunner(config, hooks=hooks)
   result = runner.run()

   # Access metrics from the result
   for comp_result in result.component_results:
       print(f"{comp_result.name}: {comp_result.duration_ms}ms")

AuditHooks
~~~~~~~~~~

``AuditHooks`` emits structured audit events for compliance and operational
visibility:

.. code-block:: python

   from pyspark_pipeline_framework.core.audit import (
       LoggingAuditSink, FileAuditSink, CompositeAuditSink, ConfigFilter,
   )
   from pyspark_pipeline_framework.runner import AuditHooks

   # Write audit events to both log and file
   sink = CompositeAuditSink(
       LoggingAuditSink(),
       FileAuditSink(path="/var/log/pipeline-audit"),
   )

   # Filter sensitive config keys from audit output
   config_filter = ConfigFilter(redact_keys={"password", "secret", "token"})

   hooks = AuditHooks(sink=sink, config_filter=config_filter)

LoggingHooks
~~~~~~~~~~~~

``LoggingHooks`` writes structured log entries at every lifecycle point via
``structlog``. These integrate with any log aggregation system (ELK,
Splunk, CloudWatch Logs, Datadog Logs):

.. code-block:: python

   from pyspark_pipeline_framework.runner import LoggingHooks, SimplePipelineRunner

   runner = SimplePipelineRunner(config, hooks=LoggingHooks())
   result = runner.run()


Health Checks
-------------

Use ``validate_pipeline()`` as a dry-run health check. It loads and validates
all component classes and configurations without executing any Spark operations:

.. code-block:: python

   from pyspark_pipeline_framework.runtime.loader import validate_pipeline

   errors = validate_pipeline("conf/production.conf")
   if errors:
       for error in errors:
           print(f"Validation error: {error}")
       raise SystemExit(1)
   print("Pipeline configuration is valid")

Integrate with container health checks or CI/CD pipelines:

.. code-block:: bash

   # In a Dockerfile HEALTHCHECK or CI step
   python -c "
   from pyspark_pipeline_framework.runtime.loader import validate_pipeline
   errors = validate_pipeline('conf/production.conf')
   raise SystemExit(1) if errors else print('OK')
   "

Use the CLI entry point for scripted health checks:

.. code-block:: bash

   # Returns exit code 0 on success, 1 on failure
   ppf-run --config conf/production.conf --validate-only


See Also
--------

- :doc:`/getting-started` -- Installation and quick example
- :doc:`/user-guide/secrets` -- Secrets management details
- :doc:`/user-guide/hooks` -- Lifecycle hooks reference
- :doc:`/contributing` -- Development setup and testing
