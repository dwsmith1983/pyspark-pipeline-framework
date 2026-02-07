Schema Contracts
================

Schema contracts define the data shape that components expect as input and
produce as output. The framework provides a platform-independent schema model,
a validation engine for checking compatibility between connected components,
and bidirectional converters to PySpark's native ``StructType``.

Overview
--------

The :class:`~pyspark_pipeline_framework.core.component.protocols.SchemaContract`
protocol declares two optional properties:

.. code-block:: python

   from pyspark_pipeline_framework.core.component.protocols import SchemaContract

   class SchemaContract(Protocol):
       @property
       def input_schema(self) -> Any | None: ...

       @property
       def output_schema(self) -> Any | None: ...

Components that implement this protocol enable compile-time validation of
data flow between pipeline stages. The framework checks that the output
schema of an upstream component is compatible with the input schema of
a downstream component.

.. note::

   ``SchemaContract`` is **not** ``@runtime_checkable``. Use ``hasattr``
   checks rather than ``isinstance`` when testing for schema support at
   runtime.

Defining Schemas
----------------

Schemas are built from three types in
:mod:`~pyspark_pipeline_framework.core.schema.definition`:

- :class:`~pyspark_pipeline_framework.core.schema.definition.DataType` --
  platform-independent type enum (``str`` mixin for natural comparison)
- :class:`~pyspark_pipeline_framework.core.schema.definition.SchemaField` --
  a single column definition (name, type, nullability, metadata)
- :class:`~pyspark_pipeline_framework.core.schema.definition.SchemaDefinition` --
  an ordered collection of fields with an optional description

.. code-block:: python

   from pyspark_pipeline_framework.core.schema.definition import (
       DataType, SchemaField, SchemaDefinition,
   )

   customer_schema = SchemaDefinition(
       fields=[
           SchemaField(name="id", data_type=DataType.LONG, nullable=False),
           SchemaField(name="name", data_type=DataType.STRING),
           SchemaField(name="email", data_type=DataType.STRING),
           SchemaField(name="created_at", data_type=DataType.TIMESTAMP),
           SchemaField(name="is_active", data_type=DataType.BOOLEAN),
       ],
       description="Cleaned customer records",
   )

   # Query the schema
   print(customer_schema.field_names())   # {"id", "name", "email", "created_at", "is_active"}
   print(customer_schema.get_field("id")) # SchemaField(name="id", ...)

``SchemaField`` auto-coerces string values to ``DataType`` when possible:

.. code-block:: python

   # Both produce the same result
   SchemaField(name="age", data_type=DataType.INTEGER)
   SchemaField(name="age", data_type="integer")

Supported Data Types
--------------------

The :class:`~pyspark_pipeline_framework.core.schema.definition.DataType`
enum provides platform-independent type identifiers:

.. list-table::
   :header-rows: 1
   :widths: 20 20 60

   * - DataType
     - Value
     - Description
   * - ``STRING``
     - ``"string"``
     - Variable-length text
   * - ``INTEGER``
     - ``"integer"``
     - 32-bit signed integer
   * - ``LONG``
     - ``"long"``
     - 64-bit signed integer
   * - ``FLOAT``
     - ``"float"``
     - 32-bit floating point
   * - ``DOUBLE``
     - ``"double"``
     - 64-bit floating point
   * - ``BOOLEAN``
     - ``"boolean"``
     - True/false
   * - ``TIMESTAMP``
     - ``"timestamp"``
     - Date and time with timezone
   * - ``DATE``
     - ``"date"``
     - Calendar date (no time component)
   * - ``BINARY``
     - ``"binary"``
     - Raw byte array
   * - ``ARRAY``
     - ``"array"``
     - Ordered collection (complex type)
   * - ``MAP``
     - ``"map"``
     - Key-value pairs (complex type)
   * - ``STRUCT``
     - ``"struct"``
     - Nested record (complex type)

.. note::

   Complex types (``ARRAY``, ``MAP``, ``STRUCT``) are supported as type
   identifiers but do not carry nested type information in ``SchemaField``.
   For complex schemas, use PySpark's native types directly via the
   converter functions.

Schema Validation
-----------------

:class:`~pyspark_pipeline_framework.core.schema.validator.SchemaValidator`
checks that an upstream component's output schema is compatible with a
downstream component's input schema:

.. code-block:: python

   from pyspark_pipeline_framework.core.schema.validator import SchemaValidator

   validator = SchemaValidator()

   output_schema = SchemaDefinition(fields=[
       SchemaField(name="id", data_type=DataType.LONG, nullable=False),
       SchemaField(name="name", data_type=DataType.STRING),
       SchemaField(name="extra_col", data_type=DataType.STRING),
   ])

   input_schema = SchemaDefinition(fields=[
       SchemaField(name="id", data_type=DataType.LONG, nullable=False),
       SchemaField(name="name", data_type=DataType.STRING),
   ])

   result = validator.validate(
       output_schema=output_schema,
       input_schema=input_schema,
       source_component="producer",
       target_component="consumer",
   )

   print(result.valid)     # True
   print(result.warnings)  # [ValidationIssue: extra_col not consumed]

**Validation rules:**

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Condition
     - Result
   * - Both schemas ``None`` (non-strict)
     - Valid -- nothing to check
   * - Both schemas ``None`` (strict mode)
     - ERROR -- schemas must be declared
   * - One schema ``None``
     - Valid -- partial schemas cannot be validated
   * - Required input field missing from output
     - ERROR
   * - Type mismatch between matching fields
     - ERROR
   * - Non-nullable input backed by nullable output
     - ERROR
   * - Extra output fields not in input
     - WARNING

**Strict mode** requires both components to declare schemas:

.. code-block:: python

   result = validator.validate(
       output_schema=None,
       input_schema=None,
       source_component="a",
       target_component="b",
       strict=True,
   )
   print(result.valid)  # False

The :class:`~pyspark_pipeline_framework.core.schema.validator.ValidationResult`
provides filtered access to issues:

.. code-block:: python

   result.valid      # True if no ERROR-level issues
   result.errors     # List of ERROR-level ValidationIssue objects
   result.warnings   # List of WARNING-level ValidationIssue objects
   result.issues     # All issues (errors + warnings)

Each :class:`~pyspark_pipeline_framework.core.schema.validator.ValidationIssue`
contains ``severity``, ``field_name``, ``message``, ``source_component``,
and ``target_component``.

Schema-Aware Components
-----------------------

Extend :class:`~pyspark_pipeline_framework.runtime.dataflow.schema.SchemaAwareDataFlow`
to create components that declare input and output schemas. This class
satisfies the ``SchemaContract`` protocol and integrates with the pipeline
validation system:

.. code-block:: python

   from pyspark_pipeline_framework.runtime.dataflow.schema import SchemaAwareDataFlow
   from pyspark_pipeline_framework.core.schema.definition import (
       SchemaDefinition, SchemaField, DataType,
   )


   class CleanCustomers(SchemaAwareDataFlow):
       @property
       def name(self) -> str:
           return "CleanCustomers"

       @property
       def input_schema(self) -> SchemaDefinition:
           return SchemaDefinition(fields=[
               SchemaField(name="id", data_type=DataType.LONG, nullable=False),
               SchemaField(name="name", data_type=DataType.STRING),
               SchemaField(name="email", data_type=DataType.STRING),
           ])

       @property
       def output_schema(self) -> SchemaDefinition:
           return SchemaDefinition(fields=[
               SchemaField(name="id", data_type=DataType.LONG, nullable=False),
               SchemaField(name="name", data_type=DataType.STRING),
               SchemaField(name="email", data_type=DataType.STRING),
               SchemaField(name="email_domain", data_type=DataType.STRING),
           ])

       @classmethod
       def from_config(cls, config: dict) -> "CleanCustomers":
           return cls()

       def run(self) -> None:
           df = self.spark.sql("""
               SELECT id, name, email,
                      split(email, '@')[1] AS email_domain
               FROM raw_customers
           """)
           df.createOrReplaceTempView("cleaned_customers")

Components that do not override ``input_schema`` or ``output_schema``
return ``None`` by default, which disables schema validation for that
boundary.

Spark Integration
-----------------

The :mod:`~pyspark_pipeline_framework.runtime.schema_converter` module
provides bidirectional conversion between the framework's
``SchemaDefinition`` and PySpark's ``StructType``:

**Convert SchemaDefinition to StructType:**

.. code-block:: python

   from pyspark_pipeline_framework.runtime.schema_converter import to_struct_type

   schema = SchemaDefinition(fields=[
       SchemaField(name="id", data_type=DataType.LONG, nullable=False),
       SchemaField(name="name", data_type=DataType.STRING),
       SchemaField(name="score", data_type=DataType.DOUBLE),
   ])

   struct_type = to_struct_type(schema)
   # StructType([
   #     StructField("id", LongType(), False),
   #     StructField("name", StringType(), True),
   #     StructField("score", DoubleType(), True),
   # ])

   # Use with Spark DataFrame creation
   df = spark.createDataFrame(data, schema=struct_type)

**Convert StructType to SchemaDefinition:**

.. code-block:: python

   from pyspark_pipeline_framework.runtime.schema_converter import from_struct_type

   # From an existing DataFrame's schema
   schema_def = from_struct_type(df.schema, description="Inferred from DataFrame")

   for field in schema_def.fields:
       print(f"{field.name}: {field.data_type} (nullable={field.nullable})")

The type mapping covers all primitive ``DataType`` values. Complex types
(``ARRAY``, ``MAP``, ``STRUCT``) are recognized during ``from_struct_type``
conversion but cannot be converted in the other direction because
``SchemaField`` does not carry nested type information. Use PySpark's
native types directly for complex schemas.

See Also
--------

- :doc:`/user-guide/components` - Building pipeline components
- :doc:`/user-guide/config-validation` - Validating pipeline configuration
- :doc:`/user-guide/data-quality` - Runtime data quality checks
