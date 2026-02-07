"""Example batch pipeline components.

Reference implementations of :class:`~pyspark_pipeline_framework.runtime.dataflow.base.DataFlow`
for common ETL patterns: reading tables, executing SQL transforms, and
writing results.

Each component implements the
:class:`~pyspark_pipeline_framework.core.component.protocols.ConfigurableInstance`
protocol via a ``from_config()`` class method so it can be loaded
dynamically from a HOCON configuration file.

Example HOCON usage::

    components: [
      {
        name: "read_raw"
        component_type: source
        class_path: "pyspark_pipeline_framework.examples.batch.ReadTable"
        config {
          table_name: "raw.customers"
          output_view: "raw_customers"
        }
      }
    ]
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from pyspark_pipeline_framework.runtime.dataflow.base import DataFlow

if TYPE_CHECKING:
    pass


@dataclass
class ReadTableConfig:
    """Configuration for :class:`ReadTable`.

    Args:
        table_name: Fully qualified table name to read.
        output_view: Name for the temporary view to register.
        filter_condition: Optional SQL WHERE clause to apply.
    """

    table_name: str
    output_view: str
    filter_condition: str | None = None


class ReadTable(DataFlow):
    """Read a Spark table and register it as a temporary view.

    Reads the specified table, optionally applies a filter, and
    registers the result as a temporary view for downstream components.

    Args:
        config: Configuration specifying the table and view names.

    Example::

        component = ReadTable(ReadTableConfig(
            table_name="raw.customers",
            output_view="raw_customers",
            filter_condition="created_at >= '2024-01-01'",
        ))
    """

    def __init__(self, config: ReadTableConfig) -> None:
        super().__init__()
        self._config = config

    @property
    def name(self) -> str:
        """Return a descriptive component name."""
        return f"ReadTable({self._config.table_name})"

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> ReadTable:
        """Create a :class:`ReadTable` from a configuration dictionary.

        Args:
            config: Dictionary with keys matching :class:`ReadTableConfig`.

        Returns:
            Configured ``ReadTable`` instance.
        """
        return cls(ReadTableConfig(**config))

    def run(self) -> None:
        """Read the table, apply filter, and register as a temp view."""
        df = self.spark.table(self._config.table_name)
        if self._config.filter_condition:
            df = df.filter(self._config.filter_condition)
        df.createOrReplaceTempView(self._config.output_view)
        self.logger.info(
            "Registered view '%s' from table '%s'",
            self._config.output_view,
            self._config.table_name,
        )


@dataclass
class SqlTransformConfig:
    """Configuration for :class:`SqlTransform`.

    Args:
        sql: SQL query to execute.
        output_view: Name for the temporary view to register.
    """

    sql: str
    output_view: str


class SqlTransform(DataFlow):
    """Execute a SQL query and register the result as a temporary view.

    Args:
        config: Configuration specifying the SQL and output view name.

    Example::

        component = SqlTransform(SqlTransformConfig(
            sql="SELECT id, UPPER(name) AS name FROM raw_customers",
            output_view="cleaned_customers",
        ))
    """

    def __init__(self, config: SqlTransformConfig) -> None:
        super().__init__()
        self._config = config

    @property
    def name(self) -> str:
        """Return a descriptive component name."""
        return f"SqlTransform({self._config.output_view})"

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> SqlTransform:
        """Create a :class:`SqlTransform` from a configuration dictionary.

        Args:
            config: Dictionary with keys matching :class:`SqlTransformConfig`.

        Returns:
            Configured ``SqlTransform`` instance.
        """
        return cls(SqlTransformConfig(**config))

    def run(self) -> None:
        """Execute the SQL and register the result as a temp view."""
        df = self.spark.sql(self._config.sql)
        df.createOrReplaceTempView(self._config.output_view)
        self.logger.info(
            "Registered view '%s' from SQL",
            self._config.output_view,
        )


@dataclass
class WriteTableConfig:
    """Configuration for :class:`WriteTable`.

    Args:
        input_view: Name of the temporary view to read from.
        output_table: Fully qualified target table name.
        mode: Write mode (``"overwrite"``, ``"append"``, etc.).
        partition_by: Columns to partition the output by.
    """

    input_view: str
    output_table: str
    mode: str = "overwrite"
    partition_by: list[str] = field(default_factory=list)


class WriteTable(DataFlow):
    """Write a temporary view to a Spark table.

    Reads from a previously registered temp view and writes to the
    target table with optional partitioning.

    Args:
        config: Configuration specifying the source view and target table.

    Example::

        component = WriteTable(WriteTableConfig(
            input_view="cleaned_customers",
            output_table="curated.customers",
            mode="overwrite",
            partition_by=["region"],
        ))
    """

    def __init__(self, config: WriteTableConfig) -> None:
        super().__init__()
        self._config = config

    @property
    def name(self) -> str:
        """Return a descriptive component name."""
        return f"WriteTable({self._config.output_table})"

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> WriteTable:
        """Create a :class:`WriteTable` from a configuration dictionary.

        Args:
            config: Dictionary with keys matching :class:`WriteTableConfig`.

        Returns:
            Configured ``WriteTable`` instance.
        """
        return cls(WriteTableConfig(**config))

    def run(self) -> None:
        """Read the temp view and write to the target table."""
        df = self.spark.table(self._config.input_view)
        writer = df.write.mode(self._config.mode)
        if self._config.partition_by:
            writer = writer.partitionBy(*self._config.partition_by)
        writer.saveAsTable(self._config.output_table)
        self.logger.info(
            "Wrote view '%s' to table '%s'",
            self._config.input_view,
            self._config.output_table,
        )
