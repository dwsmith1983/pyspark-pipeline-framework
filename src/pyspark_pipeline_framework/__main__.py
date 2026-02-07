"""Allow ``python -m pyspark_pipeline_framework`` to run a pipeline."""

import sys

from pyspark_pipeline_framework.runner.cli import main

sys.exit(main())
