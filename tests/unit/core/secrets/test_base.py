"""Tests for secrets base types."""

from __future__ import annotations

from pyspark_pipeline_framework.core.secrets.base import (
    SecretResolutionResult,
    SecretResolutionStatus,
    SecretsReference,
)


class TestSecretResolutionStatus:
    def test_values(self) -> None:
        assert SecretResolutionStatus.SUCCESS == "success"
        assert SecretResolutionStatus.NOT_FOUND == "not_found"
        assert SecretResolutionStatus.ERROR == "error"

    def test_is_str(self) -> None:
        assert isinstance(SecretResolutionStatus.SUCCESS, str)


class TestSecretsReference:
    def test_construction(self) -> None:
        ref = SecretsReference(provider="env", key="MY_SECRET")
        assert ref.provider == "env"
        assert ref.key == "MY_SECRET"


class TestSecretResolutionResult:
    def test_success(self) -> None:
        ref = SecretsReference(provider="env", key="KEY")
        result = SecretResolutionResult(
            reference=ref,
            status=SecretResolutionStatus.SUCCESS,
            value="secret-value",
        )
        assert result.value == "secret-value"
        assert result.error is None

    def test_not_found(self) -> None:
        ref = SecretsReference(provider="env", key="KEY")
        result = SecretResolutionResult(
            reference=ref,
            status=SecretResolutionStatus.NOT_FOUND,
            error="not set",
        )
        assert result.value is None
        assert result.error == "not set"

    def test_repr_masks_value(self) -> None:
        ref = SecretsReference(provider="env", key="KEY")
        result = SecretResolutionResult(
            reference=ref,
            status=SecretResolutionStatus.SUCCESS,
            value="super-secret",
        )
        r = repr(result)
        assert "super-secret" not in r
        assert "***" in r

    def test_repr_shows_none_when_no_value(self) -> None:
        ref = SecretsReference(provider="env", key="KEY")
        result = SecretResolutionResult(
            reference=ref,
            status=SecretResolutionStatus.NOT_FOUND,
        )
        r = repr(result)
        assert "None" in r
        assert "***" not in r
