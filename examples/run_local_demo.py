"""Local demo: run a pipeline entirely on local CSV files.

Demonstrates a complete pipeline run with sample data, lifecycle hooks,
and result inspection — no external services required.

Usage:
    python examples/run_local_demo.py
"""

from __future__ import annotations

import shutil
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

OUTPUT_DIR = Path("/tmp/ppf-local-demo/output")
CHECKPOINT_DIR = Path("/tmp/ppf-local-demo/checkpoints")


def main() -> None:
    """Load config, attach hooks, run the pipeline, and show output."""
    # Clean previous run
    shutil.rmtree("/tmp/ppf-local-demo", ignore_errors=True)

    # 1. Load HOCON configuration
    config_path = str(Path(__file__).parent / "local_demo.conf")
    config = load_from_file(config_path, PipelineConfig)
    print(f"Pipeline : {config.name} v{config.version}")
    print(f"Components: {', '.join(c.name for c in config.components)}")

    # 2. Set up checkpoint store for resume capability
    store = LocalCheckpointStore(CHECKPOINT_DIR)
    fingerprint = compute_pipeline_fingerprint(config)

    # 3. Compose lifecycle hooks
    hooks = CompositeHooks(
        LoggingHooks(),
        MetricsHooks(),
        AuditHooks(LoggingAuditSink()),
        CheckpointHooks(store, run_id="local-001", pipeline_fingerprint=fingerprint),
    )

    # 4. Build and run the pipeline
    runner = SimplePipelineRunner(config, hooks=hooks)
    result = runner.run()

    # 5. Print results
    print(f"\nPipeline status: {result.status.value}")
    print(f"Total duration:  {result.total_duration_ms} ms")
    for comp in result.component_results:
        status = "SUCCESS" if comp.success else "FAILED"
        print(f"  {comp.component_name}: {status} ({comp.duration_ms} ms)")

    # 6. Show the output data
    if result.status.value == "success":
        print("\n--- Output (cleaned_customers) ---")
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        output_df = spark.read.csv(str(OUTPUT_DIR), header=True)
        output_df.show(truncate=False)
        spark.stop()


if __name__ == "__main__":
    main()
