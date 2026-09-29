import os
import sys


def configure_pyspark_python():
    """Point PySpark's driver AND worker subprocesses at the interpreter
    that's actually running this process (the venv's python), instead of
    letting Spark fall back to whatever "python" resolves to on PATH."""
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)