"""Tests for ConfigFilter."""

from __future__ import annotations

from pyspark_pipeline_framework.core.audit.filters import ConfigFilter


class TestConfigFilter:
    def test_scrubs_password(self) -> None:
        data = {"db_password": "secret123", "host": "localhost"}
        result = ConfigFilter.scrub(data)
        assert result["db_password"] == "***REDACTED***"
        assert result["host"] == "localhost"

    def test_scrubs_multiple_patterns(self) -> None:
        data = {
            "api_token": "tok-123",
            "secret_key": "sk-456",
            "auth_header": "Bearer xyz",
            "credential_file": "/path",
            "name": "safe",
        }
        result = ConfigFilter.scrub(data)
        assert result["api_token"] == "***REDACTED***"
        assert result["secret_key"] == "***REDACTED***"
        assert result["auth_header"] == "***REDACTED***"
        assert result["credential_file"] == "***REDACTED***"
        assert result["name"] == "safe"

    def test_case_insensitive(self) -> None:
        data = {"DB_PASSWORD": "secret", "API_TOKEN": "tok"}
        result = ConfigFilter.scrub(data)
        assert result["DB_PASSWORD"] == "***REDACTED***"
        assert result["API_TOKEN"] == "***REDACTED***"

    def test_nested_dict(self) -> None:
        data = {
            "database": {
                "password": "secret",
                "host": "localhost",
            }
        }
        result = ConfigFilter.scrub(data)
        assert result["database"]["password"] == "***REDACTED***"
        assert result["database"]["host"] == "localhost"

    def test_list_with_dicts(self) -> None:
        data = {
            "connections": [
                {"password": "s1", "host": "h1"},
                {"password": "s2", "host": "h2"},
            ]
        }
        result = ConfigFilter.scrub(data)
        assert result["connections"][0]["password"] == "***REDACTED***"
        assert result["connections"][0]["host"] == "h1"
        assert result["connections"][1]["password"] == "***REDACTED***"

    def test_list_with_primitives(self) -> None:
        data = {"tags": ["a", "b", "c"]}
        result = ConfigFilter.scrub(data)
        assert result["tags"] == ["a", "b", "c"]

    def test_custom_replacement(self) -> None:
        data = {"password": "secret"}
        result = ConfigFilter.scrub(data, replacement="[HIDDEN]")
        assert result["password"] == "[HIDDEN]"

    def test_empty_dict(self) -> None:
        assert ConfigFilter.scrub({}) == {}

    def test_no_sensitive_keys(self) -> None:
        data = {"host": "localhost", "port": 5432}
        result = ConfigFilter.scrub(data)
        assert result == data
