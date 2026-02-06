"""Lifecycle hooks configuration models."""

from dataclasses import dataclass

from pyspark_pipeline_framework.core.config.base import LogFormat, LogLevel, MetricsBackend


@dataclass
class LoggingConfig:
    """Configuration for logging behavior."""

    level: LogLevel = LogLevel.INFO
    """Logging level (default: INFO)"""

    format: LogFormat = LogFormat.JSON
    """Log output format (default: JSON)"""

    output: str = "stdout"
    """Log output destination - stdout, stderr, or file path (default: stdout)"""

    structured: bool = True
    """Use structlog for structured logging (default: True)"""


@dataclass
class MetricsConfig:
    """Configuration for metrics collection and export."""

    enabled: bool = True
    """Enable metrics collection (default: True)"""

    backend: MetricsBackend = MetricsBackend.PROMETHEUS
    """Metrics backend to use (default: prometheus)"""

    push_gateway_url: str | None = None
    """URL for metrics push gateway (optional)"""

    export_interval_seconds: int = 60
    """Interval for exporting metrics in seconds (default: 60)"""

    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        if self.export_interval_seconds < 1:
            raise ValueError("export_interval_seconds must be at least 1")


@dataclass
class AuditConfig:
    """Configuration for audit trail logging."""

    enabled: bool = True
    """Enable audit trail (default: True)"""

    include_data_samples: bool = False
    """Include data samples in audit logs (default: False)"""

    audit_trail_path: str = "/var/log/pipeline/audit"
    """Path for audit trail logs (default: /var/log/pipeline/audit)"""

    retention_days: int = 90
    """Number of days to retain audit logs (default: 90)"""

    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        if self.retention_days < 1:
            raise ValueError("retention_days must be at least 1")


@dataclass
class HooksConfig:
    """Composite configuration for all lifecycle hooks."""

    logging: LoggingConfig = None  # type: ignore
    """Logging configuration"""

    metrics: MetricsConfig | None = None
    """Metrics configuration (optional)"""

    audit: AuditConfig | None = None
    """Audit configuration (optional)"""

    def __post_init__(self) -> None:
        """Initialize default logging if not provided."""
        if self.logging is None:
            self.logging = LoggingConfig()
