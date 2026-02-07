Configuration Validation
========================

Validate pipeline configuration before execution to catch errors early.
The :mod:`~pyspark_pipeline_framework.core.config.validator` module provides
lightweight validation (class path resolution, protocol checks) and full
dry-run validation (component instantiation without execution). Both modes
work without a running Spark cluster, making them ideal for CI/CD pipelines.

Quick Start
-----------

.. code-block:: python

   from pyspark_pipeline_framework.core.config import (
       load_from_file, PipelineConfig, validate_pipeline, dry_run,
   )

   config = load_from_file("pipeline.conf", PipelineConfig)

   # Lightweight validation -- checks class paths and protocols
   result = validate_pipeline(config)
   if not result.is_valid:
       for error in result.errors:
           print(f"[{error.phase}] {error.message}")
   else:
       print("Configuration is valid")

   # Full dry run -- instantiates components without executing
   dr = dry_run(config)
   if dr.is_valid:
       print(f"All {len(dr.instantiated)} components instantiated successfully")
   else:
       for error in dr.errors:
           print(f"Failed: {error.component_name} -- {error.message}")

Validation Phases
-----------------

:func:`~pyspark_pipeline_framework.core.config.validator.validate_pipeline`
checks configuration in four phases, each identified by a
:class:`~pyspark_pipeline_framework.core.config.validator.ValidationPhase`
enum value:

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Phase
     - What It Checks
   * - ``CONFIG_SYNTAX``
     - HOCON parsing succeeded (checked at load time by ``dataconf``)
   * - ``REQUIRED_FIELDS``
     - Pipeline ``name`` is non-empty; ``components`` list is not empty
   * - ``TYPE_RESOLUTION``
     - Each component ``class_path`` resolves to a valid ``PipelineComponent``
       subclass via ``importlib``
   * - ``COMPONENT_CONFIG``
     - Component class passes
       :func:`~pyspark_pipeline_framework.runtime.loader.validate_component_class`
       checks (e.g. ``ConfigurableInstance`` protocol compliance)

Phases run in order. If ``TYPE_RESOLUTION`` fails for a component, the
``COMPONENT_CONFIG`` phase is skipped for that component.

ValidationResult
----------------

:class:`~pyspark_pipeline_framework.core.config.validator.ValidationResult`
holds the outcome of validation:

.. code-block:: python

   from pyspark_pipeline_framework.core.config import validate_pipeline

   result = validate_pipeline(config)

   # True when there are no errors
   result.is_valid

   # List of ValidationError objects (fatal issues)
   result.errors

   # List of warning strings (non-fatal concerns)
   result.warnings

Each :class:`~pyspark_pipeline_framework.core.config.validator.ValidationError`
contains:

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Attribute
     - Description
   * - ``phase``
     - The :class:`~pyspark_pipeline_framework.core.config.validator.ValidationPhase`
       that produced this error
   * - ``message``
     - Human-readable error description
   * - ``component_name``
     - Name of the component involved (``None`` for pipeline-level errors)

Common Validation Errors
------------------------

**Missing pipeline fields:**

.. code-block:: text

   [required-fields] Pipeline name is empty
   [required-fields] Pipeline has no components

**Class not found:**

.. code-block:: text

   [type-resolution] Cannot load 'my.module.DoesNotExist': No module named 'my.module'

**Invalid ConfigurableInstance:**

.. code-block:: text

   [component-config] Validation failed for 'my.module.BadComponent': ...

**Dry-run instantiation failure:**

.. code-block:: text

   [component-config] Failed to instantiate 'my.module.BadComponent': TypeError(...)

Dry Run
-------

:func:`~pyspark_pipeline_framework.core.config.validator.dry_run` goes
further than ``validate_pipeline`` by actually calling ``from_config()``
(or the constructor) on each enabled component. This catches configuration
shape mismatches that static validation cannot detect:

.. code-block:: python

   from pyspark_pipeline_framework.core.config import dry_run

   result = dry_run(config)

   # Names of components that instantiated successfully
   print(result.instantiated)  # ["read_raw", "transform", "write"]

   # Components that failed instantiation
   for error in result.errors:
       print(f"{error.component_name}: {error.message}")

The :class:`~pyspark_pipeline_framework.core.config.validator.DryRunResult`
has the same ``is_valid`` property and ``errors`` list as ``ValidationResult``,
plus an ``instantiated`` list of component names that succeeded.

Disabled components (``enabled: false``) are skipped in both validation
and dry-run.

CI/CD Integration
-----------------

Run validation as a pre-deploy gate in your CI/CD pipeline. Since neither
``validate_pipeline`` nor ``dry_run`` requires a Spark cluster, they can
run in any Python environment:

.. code-block:: python

   #!/usr/bin/env python
   """Pre-deploy validation script for CI/CD."""

   import sys

   from pyspark_pipeline_framework.core.config import (
       load_from_file, PipelineConfig, validate_pipeline, dry_run,
   )


   def main() -> int:
       config = load_from_file("pipeline.conf", PipelineConfig)

       # Phase 1: lightweight validation
       result = validate_pipeline(config)
       if not result.is_valid:
           print("Validation FAILED:")
           for error in result.errors:
               print(f"  [{error.phase}] {error.message}")
           return 1

       if result.warnings:
           print("Warnings:")
           for warning in result.warnings:
               print(f"  {warning}")

       # Phase 2: dry run
       dr = dry_run(config)
       if not dr.is_valid:
           print("Dry run FAILED:")
           for error in dr.errors:
               print(f"  {error.component_name}: {error.message}")
           return 1

       print(f"OK -- {len(dr.instantiated)} components validated")
       return 0


   if __name__ == "__main__":
       sys.exit(main())

Example CI step (GitHub Actions):

.. code-block:: yaml

   - name: Validate pipeline config
     run: |
       pip install pyspark-pipeline-framework
       python scripts/validate_config.py

See Also
--------

- :doc:`/user-guide/configuration` - Configuration structure and loading
- :doc:`/user-guide/components` - Building pipeline components
- :doc:`/user-guide/schema-contracts` - Schema validation for data flow
