"""Component-related exceptions."""


class ComponentError(Exception):
    """Base exception for component-related errors."""

    pass


class ComponentInstantiationError(ComponentError):
    """Failed to instantiate component from configuration."""

    def __init__(self, class_path: str, cause: Exception) -> None:
        self.class_path = class_path
        self.cause = cause
        super().__init__(f"Failed to instantiate '{class_path}': {cause}")
        self.__cause__ = cause


class ComponentExecutionError(ComponentError):
    """Component run() method raised an exception."""

    def __init__(self, component_name: str, cause: Exception) -> None:
        self.component_name = component_name
        self.cause = cause
        super().__init__(f"Component '{component_name}' failed: {cause}")
        self.__cause__ = cause
