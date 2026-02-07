"""Tests for the CLI entrypoint."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from pyspark_pipeline_framework.runner.cli import _build_parser, main
from pyspark_pipeline_framework.runner.result import PipelineResult, PipelineResultStatus


class TestBuildParser:
    def test_requires_config_argument(self) -> None:
        parser = _build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args([])

    def test_config_positional(self) -> None:
        args = _build_parser().parse_args(["pipeline.conf"])
        assert args.config == "pipeline.conf"

    def test_defaults(self) -> None:
        args = _build_parser().parse_args(["p.conf"])
        assert args.dry_run is False
        assert args.no_fail_fast is False
        assert args.skip_validation is False
        assert args.log_level == "INFO"

    def test_all_flags(self) -> None:
        args = _build_parser().parse_args([
            "p.conf",
            "--dry-run",
            "--no-fail-fast",
            "--skip-validation",
            "--log-level", "DEBUG",
        ])
        assert args.dry_run is True
        assert args.no_fail_fast is True
        assert args.skip_validation is True
        assert args.log_level == "DEBUG"


class TestMainSuccess:
    @patch("pyspark_pipeline_framework.runner.cli.SimplePipelineRunner")
    def test_success_returns_zero(self, mock_runner_cls: MagicMock) -> None:
        runner = MagicMock()
        runner.run.return_value = PipelineResult(
            status=PipelineResultStatus.SUCCESS,
            pipeline_name="test",
            total_duration_ms=100,
        )
        mock_runner_cls.from_file.return_value = runner

        code = main(["pipeline.conf"])
        assert code == 0
        mock_runner_cls.from_file.assert_called_once()

    @patch("pyspark_pipeline_framework.runner.cli.SimplePipelineRunner")
    def test_partial_success_returns_two(self, mock_runner_cls: MagicMock) -> None:
        runner = MagicMock()
        runner.run.return_value = PipelineResult(
            status=PipelineResultStatus.PARTIAL_SUCCESS,
            pipeline_name="test",
            total_duration_ms=100,
        )
        mock_runner_cls.from_file.return_value = runner

        code = main(["pipeline.conf"])
        assert code == 2

    @patch("pyspark_pipeline_framework.runner.cli.SimplePipelineRunner")
    def test_failure_returns_one(self, mock_runner_cls: MagicMock) -> None:
        runner = MagicMock()
        runner.run.return_value = PipelineResult(
            status=PipelineResultStatus.FAILURE,
            pipeline_name="test",
            total_duration_ms=100,
        )
        mock_runner_cls.from_file.return_value = runner

        code = main(["pipeline.conf"])
        assert code == 1


class TestMainDryRun:
    @patch("pyspark_pipeline_framework.runner.cli.SimplePipelineRunner")
    def test_dry_run_pass(self, mock_runner_cls: MagicMock) -> None:
        runner = MagicMock()
        runner.dry_run.return_value = []
        mock_runner_cls.from_file.return_value = runner

        code = main(["pipeline.conf", "--dry-run"])
        assert code == 0
        runner.dry_run.assert_called_once()
        runner.run.assert_not_called()

    @patch("pyspark_pipeline_framework.runner.cli.SimplePipelineRunner")
    def test_dry_run_fail(self, mock_runner_cls: MagicMock) -> None:
        runner = MagicMock()
        runner.dry_run.return_value = ["Cannot load 'bad.Module'"]
        mock_runner_cls.from_file.return_value = runner

        code = main(["pipeline.conf", "--dry-run"])
        assert code == 1


class TestMainLoadError:
    @patch("pyspark_pipeline_framework.runner.cli.SimplePipelineRunner")
    def test_load_error_returns_one(self, mock_runner_cls: MagicMock) -> None:
        mock_runner_cls.from_file.side_effect = FileNotFoundError("not found")
        code = main(["bad.conf"])
        assert code == 1


class TestMainFlags:
    @patch("pyspark_pipeline_framework.runner.cli.SimplePipelineRunner")
    def test_no_fail_fast_forwarded(self, mock_runner_cls: MagicMock) -> None:
        runner = MagicMock()
        runner.run.return_value = PipelineResult(
            status=PipelineResultStatus.SUCCESS,
            pipeline_name="test",
            total_duration_ms=100,
        )
        mock_runner_cls.from_file.return_value = runner

        main(["p.conf", "--no-fail-fast", "--skip-validation"])
        _, kwargs = mock_runner_cls.from_file.call_args
        assert kwargs["fail_fast"] is False
        assert kwargs["validate_before_run"] is False
