"""Core pipeline component abstraction."""

from abc import ABC, abstractmethod


class PipelineComponent(ABC):
    """Base class for all pipeline components.

    Every component must:
    1. Have a name (for logging/debugging)
    2. Implement run() to execute its logic

    This is a Spark-agnostic base class. Spark-aware components
    should extend DataFlow in the runtime module instead.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable component name for logging."""
        ...

    @abstractmethod
    def run(self) -> None:
        """Execute the component's main logic.

        Raises:
            Exception: Any exception will be caught by the runner
                and handled according to retry/circuit breaker policy.
        """
        ...
