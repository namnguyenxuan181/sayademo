import logging

from pyspark.sql import DataFrame, SparkSession

# noinspection PyUnresolvedReferences
from common.spark.extension import row_ext

__author__ = 'ThucNC'
_logger = logging.getLogger(__name__)


def test_to_dict(spark: SparkSession):
    source_df: DataFrame = spark.createDataFrame(
        [
            (1, "nguyen"),
            (2, "thuc")
        ],
        ("id", "name")
    )

    rows = source_df.collect()
    assert rows[0].to_dict() == {"id": 1, "name": "nguyen"}
    assert rows[0].to_dict() == {"name": "nguyen", "id": 1}
    assert rows[1].to_dict() == {"name": "thuc", "id": 2}
    assert rows[1].to_dict() == {"id": 2, "name": "thuc"}


def test_to_tuple(spark: SparkSession):
    source_df: DataFrame = spark.createDataFrame(
        [
            (1, "nguyen"),
            (2, "thuc")
        ],
        ("id", "name")
    )

    rows = source_df.collect()
    assert rows[0].to_tuple() == (1, "nguyen")
    assert not rows[0].to_tuple() == ("nguyen", 1)
    assert rows[1].to_tuple() == (2, "thuc")
