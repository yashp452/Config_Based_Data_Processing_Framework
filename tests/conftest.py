import os

import pytest
from pyspark.sql import SparkSession

# When PySpark is invoked locally it may pick up shell vars like
# PYSPARK_DRIVER_PYTHON=jupyter, which tries to launch a Jupyter server as
# the driver and hangs.  Always run tests with a plain Python driver.
os.environ.pop("PYSPARK_DRIVER_PYTHON", None)
os.environ.pop("PYSPARK_DRIVER_PYTHON_OPTS", None)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("TestPipeline")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
