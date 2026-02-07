# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2026-02-07

### Added

- Core pipeline component abstractions (`PipelineComponent`, `DataFlow`, `SchemaAwareDataFlow`)
- Protocols: `ConfigurableInstance`, `SchemaContract`, `Resource`
- HOCON configuration via `dataconf` (`PipelineConfig`, `ComponentConfig`, `SparkConfig`)
- Configuration validation and dry-run (`validate_pipeline`, `dry_run`)
- Schema validation between components (`SchemaValidator`, `SchemaDefinition`, `DataType`)
- `SimplePipelineRunner` with topological execution ordering
- Retry with exponential backoff and jitter (`RetryExecutor`, `@with_retry`)
- Circuit breaker with thread-safe state machine (`CircuitBreaker`)
- `ResiliencePolicy` presets (DEFAULT, AGGRESSIVE, CONSERVATIVE, RETRY_ONLY, CIRCUIT_BREAKER_ONLY)
- Lifecycle hooks protocol (`PipelineHooks`) with built-in implementations:
  - `LoggingHooks`, `MetricsHooks`, `CompositeHooks`, `NoOpHooks`
  - `DataQualityHooks`, `AuditHooks`, `CheckpointHooks`
- Data quality checks: `row_count_check`, `null_check`, `uniqueness_check`, `value_range_check`, `pattern_check`, `custom_sql_check`
- Audit trail system (`AuditEvent`, `AuditSink`, `LoggingAuditSink`, `FileAuditSink`, `CompositeAuditSink`)
- Sensitive config redaction (`ConfigFilter.scrub`)
- Secrets management with pluggable providers (`EnvSecretsProvider`, `AwsSecretsProvider`, `VaultSecretsProvider`)
- Secret resolution in HOCON configs (`secret://PROVIDER/KEY` syntax)
- Thread-safe secrets cache with TTL (`SecretsCache`)
- Secrets audit logging (`SecretsAuditLogger`)
- Checkpoint and resume (`LocalCheckpointStore`, `CheckpointHooks`, pipeline fingerprinting)
- `SparkSessionWrapper` with thread-safe singleton and Spark Connect support
- Structured Streaming support:
  - Sources: Kafka, File, Delta, Iceberg, Rate, EventHubs, Kinesis
  - Sinks: Kafka, Delta, Console, Iceberg, File, CloudStorage, ForeachBatch
  - `StreamingPipeline` orchestration with `StreamingHooks`
- Dynamic component loading (`load_component_class`, `instantiate_component`)
- Schema converter (`to_struct_type`, `from_struct_type`)
- Metrics registry protocol with `InMemoryRegistry`
- Example batch and streaming components
- Sphinx documentation site (furo theme, autodoc, user guide, API reference)
- CLI entrypoint (`ppf-run` / `python -m pyspark_pipeline_framework`)
- GitHub Actions CI and release workflows
- ReadTheDocs integration

[1.0.0]: https://github.com/dwsmith1983/pyspark-pipeline-framework/releases/tag/v1.0.0
