"""End-to-end example: run the customer ETL pipeline.

Demonstrates loading a HOCON config, wiring up lifecycle hooks
(logging, data quality, audit, checkpoint), and executing the
pipeline with ``SimplePipelineRunner``.

Usage:
    python examples/run_customer_etl.py

Note:
    This example uses the built-in ``ReadTable``, ``SqlTransform``,
    and ``WriteTable`` components.  It requires a running Spark
    session with the ``raw.customers`` table available.  For a
    quick local test, see ``examples/run_local_demo.py``.
"""

from __future__ import annotations

from pathlib import Path

from pyspark_pipeline_framework.core.audit import LoggingAuditSink
from pyspark_pipeline_framework.core.config import PipelineConfig, load_from_file
from pyspark_pipeline_framework.runner import (
    AuditHooks,
    CheckpointHooks,
    CompositeHooks,
    LoggingHooks,
    MetricsHooks,
    SimplePipelineRunner,
    compute_pipeline_fingerprint,
)
from pyspark_pipeline_framework.runner.checkpoint import LocalCheckpointStore


def main() -> None:
    """Load config, attach hooks, and run the pipeline."""
    # 1. Load HOCON configuration
    config_path = str(Path(__file__).parent / "customer_etl.conf")
    config = load_from_file(config_path, PipelineConfig)
    print(f"Loaded pipeline: {config.name} v{config.version}")

    # 2. Set up checkpoint store for resume capability
    store = LocalCheckpointStore(Path("/tmp/ppf-checkpoints"))
    fingerprint = compute_pipeline_fingerprint(config)

    # 3. Compose lifecycle hooks
    hooks = CompositeHooks(
        LoggingHooks(),
        MetricsHooks(),
        AuditHooks(LoggingAuditSink()),
        CheckpointHooks(store, run_id="demo-001", pipeline_fingerprint=fingerprint),
    )

    # 4. Build and run the pipeline
    runner = SimplePipelineRunner(config, hooks=hooks)
    result = runner.run()

    # 5. Inspect results
    print(f"\nPipeline status: {result.status}")
    print(f"Total duration:  {result.total_duration_ms} ms")
    for comp in result.component_results:
        print(f"  {comp.component_name}: {comp.status} ({comp.duration_ms} ms)")


if __name__ == "__main__":
    main()
