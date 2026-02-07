Secrets Management
==================

Resolve secrets at runtime from pluggable providers. Supports environment
variables, AWS Secrets Manager, and HashiCorp Vault out of the box.

Basic Usage
-----------

.. code-block:: python

   from pyspark_pipeline_framework.core.secrets import (
       EnvSecretsProvider, SecretsResolver, SecretsCache, SecretsReference,
   )

   resolver = SecretsResolver()
   resolver.register(EnvSecretsProvider())

   cache = SecretsCache(resolver, ttl_seconds=300)

   result = cache.resolve(SecretsReference(provider="env", key="DB_PASSWORD"))
   if result.value:
       print("Secret resolved successfully")

Providers
---------

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Provider
     - Description
   * - ``EnvSecretsProvider``
     - Reads secrets from environment variables
   * - ``AwsSecretsProvider``
     - Reads from AWS Secrets Manager (requires ``boto3``)
   * - ``VaultSecretsProvider``
     - Reads from HashiCorp Vault (requires ``hvac``)

**Installation for optional providers:**

.. code-block:: bash

   # AWS Secrets Manager
   pip install pyspark-pipeline-framework[aws]

   # HashiCorp Vault
   pip install pyspark-pipeline-framework[vault]

   # Both
   pip install pyspark-pipeline-framework[aws,vault]

SecretsResolver
---------------

The resolver dispatches secret references to the appropriate provider:

.. code-block:: python

   from pyspark_pipeline_framework.core.secrets import (
       SecretsResolver,
       EnvSecretsProvider,
       AwsSecretsProvider,
   )

   resolver = SecretsResolver()
   resolver.register(EnvSecretsProvider())
   resolver.register(AwsSecretsProvider(region_name="us-east-1"))

   # Routes to EnvSecretsProvider
   env_secret = resolver.resolve(
       SecretsReference(provider="env", key="API_KEY"),
   )

   # Routes to AwsSecretsProvider
   aws_secret = resolver.resolve(
       SecretsReference(provider="aws", key="prod/db-password"),
   )

SecretsCache
------------

Wrap the resolver in a thread-safe TTL cache to avoid repeated lookups:

.. code-block:: python

   from pyspark_pipeline_framework.core.secrets import SecretsCache

   cache = SecretsCache(resolver, ttl_seconds=300)

   # First call fetches from provider
   result1 = cache.resolve(SecretsReference(provider="env", key="DB_PASSWORD"))

   # Second call returns cached value (within TTL)
   result2 = cache.resolve(SecretsReference(provider="env", key="DB_PASSWORD"))

Custom Providers
----------------

Implement the ``SecretsProvider`` ABC to add new secret backends:

.. code-block:: python

   from pyspark_pipeline_framework.core.secrets.base import SecretsProvider


   class MyVaultProvider(SecretsProvider):
       @property
       def provider_name(self) -> str:
           return "my-vault"

       def get_secret(self, key: str) -> str | None:
           # Your implementation here
           ...

See Also
--------

- :doc:`/features/audit` - Audit trail with config filtering
- :doc:`/quickstart` - Installation options
