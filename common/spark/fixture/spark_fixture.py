import logging
import sys

import pytest
from pyspark.sql import SparkSession

from common.logging.structlogger import new_logger

DEFAULT_TIMEZONE = "Asia/Bangkok"


@pytest.fixture(scope="session")
def spark():
    import os
    os.environ["TZ"] = DEFAULT_TIMEZONE

    spark = (
        SparkSession.builder
            .master("local[4]")
            .appName("PySpark:pytest")
            .config("spark.driver.memory", "4g")
            .config("spark.default.parallelism", 1)
            .config("spark.sql.shuffle.partitions", 1)
            .config("spark.rdd.compress", False)
            .config("spark.shuffle.compress", False)
            .config("spark.sql.crossJoin.enabled", True)
            .config("spark.sql.codegen.wholeStage", False)
            .config("spark.dynamicAllocation.enabled", False)
            .config("spark.sql.session.timeZone", DEFAULT_TIMEZONE)
            .config("spark.sql.execution.arrow.pyspark.enabled", True)
            .config("spark.sql.execution.arrow.pyspark.fallback.enabled", True)
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def logger():
    logger_name = "pytest_logger"
    logging.basicConfig(format="%(message)s", level=logging.INFO, stream=sys.stdout)
    _logger = logging.getLogger(logger_name)
    return new_logger(logger=_logger)


@pytest.fixture
def spark_context(spark):
    return spark.sparkContext


@pytest.fixture(autouse=True)
def no_spark_stop(monkeypatch):
    """ Disable stopping the shared spark session during tests """

    def noop(*args, **kwargs):
        print("Disabled spark.stop for testing")

    monkeypatch.setattr("pyspark.sql.SparkSession.stop", noop)


@pytest.fixture(autouse=True)
def auto_clear_cache(spark):
    yield
    spark.catalog.clearCache()
