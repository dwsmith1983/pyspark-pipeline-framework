"""Command-line interface for running pipelines."""

from __future__ import annotations

import argparse
import logging
import sys

from pyspark_pipeline_framework.runner.hooks import CompositeHooks
from pyspark_pipeline_framework.runner.hooks_builtin import LoggingHooks
from pyspark_pipeline_framework.runner.result import PipelineResultStatus
from pyspark_pipeline_framework.runner.simple_runner import SimplePipelineRunner


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ppf-run",
        description="Run a PySpark pipeline from a HOCON configuration file.",
    )
    parser.add_argument(
        "config",
        help="Path to the HOCON pipeline configuration file.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Validate component classes without executing the pipeline.",
    )
    parser.add_argument(
        "--no-fail-fast",
        action="store_true",
        default=False,
        help="Continue running after a component failure instead of stopping.",
    )
    parser.add_argument(
        "--skip-validation",
        action="store_true",
        default=False,
        help="Skip pre-flight config validation.",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Set the logging level (default: INFO).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint for running pipelines.

    Args:
        argv: Command-line arguments. Defaults to ``sys.argv[1:]``.

    Returns:
        Exit code: 0 for success, 1 for failure, 2 for partial success.
    """
    parser = _build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    try:
        runner = SimplePipelineRunner.from_file(
            args.config,
            hooks=CompositeHooks(LoggingHooks()),
            fail_fast=not args.no_fail_fast,
            validate_before_run=not args.skip_validation,
        )
    except Exception as exc:
        logging.getLogger(__name__).error("Failed to load pipeline: %s", exc)
        return 1

    if args.dry_run:
        warnings = runner.dry_run()
        if warnings:
            for w in warnings:
                print(f"WARNING: {w}", file=sys.stderr)
            return 1
        print("Dry run passed: all component classes are valid.")
        return 0

    result = runner.run()

    if result.status is PipelineResultStatus.SUCCESS:
        return 0
    elif result.status is PipelineResultStatus.PARTIAL_SUCCESS:
        return 2
    else:
        return 1


if __name__ == "__main__":
    sys.exit(main())
