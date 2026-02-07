Checkpoint & Resume
===================

Resume failed pipelines from the last successful component. The checkpoint
system records which components completed successfully, computes a pipeline
fingerprint to detect configuration changes, and provides a hooks-based
integration with ``SimplePipelineRunner``.

Basic Usage
-----------

.. code-block:: python

   from pathlib import Path
   from pyspark_pipeline_framework.runner import (
       LocalCheckpointStore, CheckpointHooks, CompositeHooks, LoggingHooks,
       SimplePipelineRunner,
       compute_pipeline_fingerprint, load_checkpoint_for_resume,
   )

   store = LocalCheckpointStore(Path("/tmp/checkpoints"))
   fingerprint = compute_pipeline_fingerprint(config)
   checkpoint_hooks = CheckpointHooks(
       store, run_id="run-001", pipeline_fingerprint=fingerprint,
   )

   hooks = CompositeHooks(LoggingHooks(), checkpoint_hooks)
   runner = SimplePipelineRunner(config, hooks=hooks)

   # First run
   result = runner.run()

   # Resume after failure
   completed = load_checkpoint_for_resume(store, "run-001", config)
   result = runner.run(completed_components=completed)

How It Works
------------

1. ``CheckpointHooks`` records each successful component in a
   ``CheckpointState`` object via the ``LocalCheckpointStore``
2. ``compute_pipeline_fingerprint`` hashes the pipeline configuration so
   stale checkpoints (from a different config) are automatically invalidated
3. ``load_checkpoint_for_resume`` reads the checkpoint and returns the set
   of completed component names
4. ``SimplePipelineRunner.run(completed_components=...)`` skips those
   components on the next run

Checkpoint Store
----------------

``LocalCheckpointStore`` persists checkpoint state as JSON files on the local
filesystem:

.. code-block:: python

   from pathlib import Path
   from pyspark_pipeline_framework.runner.checkpoint import LocalCheckpointStore

   store = LocalCheckpointStore(Path("/tmp/checkpoints"))

   # Save checkpoint
   store.save(state)

   # Load checkpoint
   state = store.load(run_id="run-001")

Pipeline Fingerprint
--------------------

The fingerprint detects configuration changes between runs. If the
configuration has changed, the checkpoint is invalidated and the pipeline
starts from the beginning:

.. code-block:: python

   from pyspark_pipeline_framework.runner.checkpoint import (
       compute_pipeline_fingerprint,
   )

   fp1 = compute_pipeline_fingerprint(config_v1)
   fp2 = compute_pipeline_fingerprint(config_v2)

   if fp1 != fp2:
       print("Config changed -- starting fresh")

See Also
--------

- :doc:`/user-guide/hooks` - Lifecycle hooks
- :doc:`/user-guide/resilience` - Retry and circuit breaker
