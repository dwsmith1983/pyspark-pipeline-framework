"""Base types and enums for configuration models."""

from enum import Enum


class Environment(str, Enum):
    """Deployment environment types."""

    DEV = "dev"
    STAGING = "staging"
    PROD = "prod"
    TEST = "test"


class PipelineMode(str, Enum):
    """Pipeline execution modes."""

    BATCH = "batch"
    STREAMING = "streaming"


class LogLevel(str, Enum):
    """Logging levels."""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class LogFormat(str, Enum):
    """Log output formats."""

    JSON = "json"
    TEXT = "text"


class ComponentType(str, Enum):
    """Component types in a pipeline."""

    SOURCE = "source"
    TRANSFORMATION = "transformation"
    SINK = "sink"


class SecretsProvider(str, Enum):
    """Secrets management providers."""

    VAULT = "vault"
    AWS_SECRETS_MANAGER = "aws_secrets_manager"
    ENV = "env"
    FILE = "file"


class MetricsBackend(str, Enum):
    """Metrics collection backends."""

    PROMETHEUS = "prometheus"
    OPENTELEMETRY = "opentelemetry"
    CUSTOM = "custom"


class SparkDeployMode(str, Enum):
    """Spark deployment modes."""

    CLIENT = "client"
    CLUSTER = "cluster"
