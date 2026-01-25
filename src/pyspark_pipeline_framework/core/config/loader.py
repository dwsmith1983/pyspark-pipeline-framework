"""HOCON configuration loader using dataconf.

This module provides functions for loading configuration from HOCON files,
strings, and environment variables using dataconf.
"""

from typing import TypeVar, cast

import dataconf

T = TypeVar("T")


def load_from_file(path: str, config_class: type[T]) -> T:
    """Load configuration from a HOCON file.

    Args:
        path: Path to the HOCON configuration file
        config_class: The configuration dataclass type to load into

    Returns:
        Instance of config_class populated with configuration from the file

    Example:
        >>> config = load_from_file("pipeline.conf", PipelineConfig)
    """
    return cast(T, dataconf.file(path, config_class))


def load_from_string(hocon_str: str, config_class: type[T]) -> T:
    """Load configuration from a HOCON string.

    Args:
        hocon_str: HOCON configuration as a string
        config_class: The configuration dataclass type to load into

    Returns:
        Instance of config_class populated with configuration from the string

    Example:
        >>> hocon = '''
        ... {
        ...   name: "my-pipeline"
        ...   version: "1.0.0"
        ... }
        ... '''
        >>> config = load_from_string(hocon, PipelineConfig)
    """
    return cast(T, dataconf.string(hocon_str, config_class))


def load_from_env(prefix: str, config_class: type[T]) -> T:
    """Load configuration from environment variables.

    Args:
        prefix: Prefix for environment variables (e.g., "PPF_")
        config_class: The configuration dataclass type to load into

    Returns:
        Instance of config_class populated with configuration from env vars

    Example:
        >>> # With PPF_NAME=my-pipeline PPF_VERSION=1.0.0
        >>> config = load_from_env("PPF_", PipelineConfig)

    Note:
        Environment variables should use the format: PREFIX_FIELD_NAME=value
        Nested fields use underscores: PREFIX_SPARK_APP_NAME=my-app
    """
    return cast(T, dataconf.env(prefix, config_class))
