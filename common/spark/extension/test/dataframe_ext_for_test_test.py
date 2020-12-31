from pyspark.sql import SparkSession, DataFrame, types as T

__author__ = 'ThucNC'


def test_expect_data(spark: SparkSession):
    data = [
        (1, "nguyen"),
        (2, "thuc")
    ]
    source_df: DataFrame = spark.createDataFrame(
        data,
        ("id", "name")
    )

    correct_data1 = [
        (1, "nguyen"),
        (2, "thuc")
    ]

    correct_data2 = [
        (2, "thuc"),
        (1, "nguyen")
    ]

    incorrect_data1 = [
        (2, "thuc"),
        (3, "chi"),
        (1, "nguyen")
    ]

    incorrect_data2 = [
        ("thuc", 2),
        ("nguyen", 1)
    ]

    incorrect_data3 = [
        (2, "thuc1"),
        (1, "nguyen")
    ]

    incorrect_data4 = []

    assert source_df.expect_data(correct_data1)
    assert source_df.expect_data(correct_data2)
    assert not source_df.expect_data(correct_data1[1:])
    assert not source_df.expect_data(correct_data2[1:])
    assert not source_df.expect_data(incorrect_data1)
    assert not source_df.expect_data(incorrect_data2)
    assert not source_df.expect_data(incorrect_data3)
    assert not source_df.expect_data(incorrect_data4)
    assert source_df.empty_clone().expect_df(source_df.empty_clone())


def test_expect_df(spark: SparkSession):
    source_df: DataFrame = spark.createDataFrame(
        [
            (1, "nguyen"),
            (2, "thuc")
        ],
        ("id", "name")
    )

    another_df1: DataFrame = spark.createDataFrame(
        [
            (2, "thuc"),
            (1, "nguyen"),
        ],
        ("id", "name")
    )

    another_df2: DataFrame = spark.createDataFrame(
        [
            ("thuc", 2),
            ("nguyen", 1),
        ],
        ("name", "id")
    )

    different_df1: DataFrame = spark.createDataFrame(
        [
            ("thuc", 2),
            ("nguyen", 1),
        ],
        ("name", "user_id")
    )

    different_df2: DataFrame = spark.createDataFrame(
        [
            ("thuc1", 2),
            ("nguyen", 1),
        ],
        ("name", "id")
    )

    different_df3: DataFrame = spark.createDataFrame(
        [
            ("nguyen", 1),
        ],
        ("name", "id")
    )

    different_df4: DataFrame = spark.createDataFrame(
        [
            (1, "nguyen"),
            (2, "thuc")
        ],
        T.StructType([
            T.StructField("id", T.StringType(), True),
            T.StructField("name", T.StringType(), True),
        ])
    )

    assert source_df.expect_df(source_df)
    assert source_df.expect_df(another_df1)
    assert source_df.expect_df(another_df2)
    assert another_df1.expect_df(source_df)
    assert another_df1.expect_df(another_df2)
    assert not different_df1.expect_df(source_df)
    assert not different_df2.expect_df(source_df)
    assert not different_df3.expect_df(source_df)
    assert not different_df4.expect_df(source_df)
    assert not different_df4.expect_df(another_df1)
    assert not another_df1.expect_df(different_df3)
