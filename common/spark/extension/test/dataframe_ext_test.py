import copy
import logging
import random

import pandas as pd
import pytest
from pyspark.sql import SparkSession, DataFrame, functions as F, types as T

__author__ = 'ThucNC'
_logger = logging.getLogger(__name__)


def test_get_sample(spark):
    source_df = spark.range(1000)
    assert 400 < source_df.get_sample(0.511111).count() < 650, "General case"
    assert source_df.get_sample(0).count() == 0, "Empty sample size"
    assert source_df.get_sample(10000).count() == 1000, "Sample size > number of rows"
    assert source_df.get_sample(1000).count() == 1000, "Sample size = number of rows"
    assert source_df.get_sample(100.12).count() == 100, "Sample size is float, and > 1"
    assert source_df.get_sample(1).count() == 1000, "Sample size = int(1), return full df"
    assert source_df.get_sample(1.0).count() == 1000, "Sample size = float(1), return full df"
    with pytest.raises(ValueError) as exc_info:
        source_df.get_sample(-1)
    assert str(exc_info.value) == "Invalid sample size -1", "Invalid sample size"

    testcases = [123, 1234, 12345, 123456]
    for t in testcases:
        source_df = spark.range(t)
        for _ in range(10):
            n = random.randint(int(t / 100), t)
            n = max(n, 2)
            sampled_df = source_df.get_sample(n=n)
            assert sampled_df.count() == n, f"Random df with {t} rows and sample size = {n}"

    data = [
        [1, 1.2, '01'],
        [2, 2.4, '02'],
        [3, 3.6, '03'],
    ]
    columns = ['name1', 'name2', 'name3']

    input_df = spark.createDataFrame(data, columns)
    emplty_df = input_df.empty_clone()
    assert emplty_df.get_sample(0).count() == 0, "Empty input df and sample size = 0"
    assert emplty_df.get_sample(10).count() == 0, "Empty input df and sample size = 10"

    with_null_df = input_df.withColumn('a', F.lit(None))
    with_null_sampled_df = with_null_df.get_sample(2)
    assert with_null_sampled_df.count() == 2, "Input df with null column"


def test_select_cols(spark):
    data = [
        [1, '1', '01'],
        [2, '2', '02'],
        [3, '3', '03'],
    ]
    columns = ['name1', 'name2', 'name3']

    input_df = spark.createDataFrame(data, columns)

    input_df \
        .select_cols(['name1', 'name3']) \
        .expect_schema(['name1', 'name3'])

    input_df \
        .select_cols({'name1': "cus_name1", 'name3': "cus_name3"}) \
        .expect_schema(['cus_name1', 'cus_name3'])


def test_prefix_cols(spark):
    data = [
        [1, '1', '01'],
        [2, '2', '02'],
        [3, '3', '03'],
    ]
    columns = ['name1', 'name2', 'name3']
    expected_columns_1 = ['cus_name1', 'cus_name2', 'name3']

    input_df = spark.createDataFrame(data, columns)
    expected_df = spark.createDataFrame(data, expected_columns_1)

    actual_df1 = input_df.prefix_cols('cus_', exclude_cols=['name3'])
    actual_df2 = input_df.prefix_cols('cus_', cols=['name1', 'name2'])
    actual_df3 = input_df.prefix_cols(prefix='cus_')
    actual_df4 = input_df.prefix_cols(prefix='cus_', suffix="_a30", cols=['name1', 'name2'], exclude_cols=['name1'])

    actual_df1.expect_df(expected_df)
    actual_df2.expect_df(expected_df)
    actual_df3.expect_schema(['cus_name1', 'cus_name2', 'cus_name3'])
    actual_df4.expect_schema(['name1', 'cus_name2_a30', 'name3'])


def test_suffix_cols(spark):
    data = [
        [1, '1', '01'],
        [2, '2', '02'],
        [3, '3', '03'],
    ]
    columns = ['name1', 'name2', 'name3']
    expected_columns_1 = ['name1_a7', 'name2_a7', 'name3']

    input_df = spark.createDataFrame(data, columns)
    expected_df = spark.createDataFrame(data, expected_columns_1)

    actual_df1 = input_df.suffix_cols('_a7', exclude_cols=['name3'])
    actual_df2 = input_df.suffix_cols('_a7', cols=['name1', 'name2'])
    actual_df3 = input_df.suffix_cols(suffix="_a7")
    actual_df4 = input_df.suffix_cols(suffix="_a7", prefix="cus_", cols=['name1', 'name2'], exclude_cols=['name1'])

    actual_df1.expect_df(expected_df)
    actual_df2.expect_df(expected_df)
    actual_df3.expect_schema(['name1_a7', 'name2_a7', 'name3_a7'])
    actual_df4.expect_schema(['name1', 'cus_name2_a7', 'name3'])


def test_pd_head(spark):
    data = [
        (1, "nguyen"),
        (2, "thuc")
    ]
    source_df: DataFrame = spark.createDataFrame(
        data,
        ("id", "name")
    )

    pd_df = source_df.pd_head(1)
    assert isinstance(pd_df, pd.DataFrame)
    assert pd_df.shape == (1, 2)
    assert pd_df.iloc[0].tolist() == [1, 'nguyen']


def test_rename_cols(spark: SparkSession):
    test_cases = [
        {
            "name": "Happy case",
            "input": [(1, 2, 3)],
            "columns": ["x", "y", "z"],
            "mapping": {"x": "a", "y": "b"},
            "expected_columns": ["a", "b", "z"],
        },
    ]

    for test in test_cases:
        source_df = spark.createDataFrame(test['input'], test["columns"])
        expected_df = spark.createDataFrame(test['input'], test["expected_columns"])
        actual_df = source_df.rename_cols(test["mapping"])
        assert actual_df.expect_df(expected_df), test["name"]


def test_apply_cols(spark: SparkSession):
    test_cases = [
        {
            "name": "Transform should work well with spark's functions",
            "input": [("A", "B")],
            "transformation": F.lower,
            "expected": [("a", "b")],
        },
        {
            "name": "Transform should work well with lambda",
            "input": [(1, 2)],
            "transformation": lambda c: c + 1,
            "expected": [(2, 3)],
        },
    ]

    for test in test_cases:
        source_df = spark.createDataFrame(test['input'], ['a', 'b'])
        actual_df = source_df.apply_cols(source_df.columns, test['transformation'])
        actual_df2 = source_df.apply_cols(None, test['transformation'])
        assert actual_df.expect_data(test['expected'])
        assert actual_df2.expect_df(source_df)


def test_trim(spark: SparkSession):
    test_cases = [
        {
            "name": "Trim should work both sides",
            "input": [("  trim_left", "trim_right   ", "  trim_both  ")],
            "expected": [("trim_left", "trim_right", "trim_both")],
        },
    ]

    for test in test_cases:
        source_df = spark.createDataFrame(test['input'], ['a', 'b', 'c'])
        actual_df = source_df.trim_cols(source_df.columns)
        assert actual_df.expect_data(test['expected']), test['name']


def test_to_upper(spark: SparkSession):
    test_cases = [
        {
            "name": "to_upper should work well with both lower & upper cases",
            "input": [("a", "A")],
            "expected": [("A", "A")],
        },
    ]

    for test in test_cases:
        source_df = spark.createDataFrame(test['input'], ['a', 'b'])
        actual_df = source_df.upper_cols(source_df.columns)
        assert actual_df.expect_data(test['expected']), test['name']


def test_to_lower(spark: SparkSession):
    test_cases = [
        {
            "name": "to_lower should work well with both lower & upper cases",
            "input": [("a", "A")],
            "expected": [("a", "a")],
        },
    ]

    for test in test_cases:
        source_df = spark.createDataFrame(test['input'], ['a', 'b'])
        actual_df = source_df.lower_cols(source_df.columns)
        assert actual_df.expect_data(test['expected']), test['name']


def test_cast(spark: SparkSession):
    test_cases = [
        {
            "name": "Cast should work well with data_type is a string",
            "input_schema": T.StructType([
                T.StructField('a', T.FloatType(), True),
            ]),
            "data_type": 'integer',
            "expected_schema": T.StructType([
                T.StructField('a', T.IntegerType(), True),
            ]),
        },
        {
            "name": "Cast should work well with data_type is a pyspark's DataType object",
            "input_schema": T.StructType([
                T.StructField('a', T.FloatType(), True),
            ]),
            "data_type": T.IntegerType(),
            "expected_schema": T.StructType([
                T.StructField('a', T.IntegerType(), True),
            ]),
        },
    ]

    for test in test_cases:
        source_df = spark.createDataFrame([], test['input_schema'])
        expected_df = spark.createDataFrame([], test['expected_schema'])
        actual_df = source_df.cast_cols(source_df.columns, test['data_type'])
        assert expected_df.schema == actual_df.schema


def test_to_list_of_dict(spark: SparkSession):
    source_df: DataFrame = spark.createDataFrame(
        [
            (1, "nguyen"),
            (2, "thuc")
        ],
        ("id", "name")
    )

    expected_data = [
        {"id": 1, "name": "nguyen"},
        {"id": 2, "name": "thuc"}
    ]

    assert source_df.to_list_dict() == expected_data


def test_to_list_of_tuple(spark: SparkSession):
    data = [
        (1, "nguyen"),
        (2, "thuc")
    ]
    source_df: DataFrame = spark.createDataFrame(
        data,
        ("id", "name")
    )

    expected_data = [copy.deepcopy(t) for t in data]

    assert source_df.to_list_tuple() == expected_data


def test_expect_schema(spark: SparkSession):
    source_df: DataFrame = spark.createDataFrame(
        [
            (1, "nguyen"),
            (2, "thuc")
        ],
        ("id", "name")
    )
    another_data = [
        ("nguyen", 1)
    ]

    another_df: DataFrame = spark.createDataFrame(
        another_data,
        ("name", "id")
    )

    different_df: DataFrame = spark.createDataFrame(
        another_data,
        ("name", "user_id")
    )

    assert source_df.expect_schema(["id", "name"])
    assert source_df.expect_schema(["name", "id"])
    assert not source_df.expect_schema(["name1", "id"])
    assert not source_df.expect_schema(["name"])
    assert not source_df.expect_schema(["name", "id", "score"])
    assert source_df.expect_schema(source_df)
    assert source_df.expect_schema(another_df)
    assert another_df.expect_schema(source_df)
    assert not different_df.expect_schema(source_df)
    assert not different_df.expect_schema(another_df)
    assert not source_df.expect_schema(different_df)


def test_empty_clone(spark: SparkSession):
    source_df: DataFrame = spark.createDataFrame(
        [
            (1, "nguyen"),
            (2, "thuc")
        ],
        ("id", "name")
    )

    empty_df = source_df.empty_clone()

    assert empty_df.count() == 0
    assert empty_df.expect_schema(source_df)


def test_add_rows(spark: SparkSession):
    source_df: DataFrame = spark.createDataFrame(
        [
            (1, "nguyen"),
            (2, "thuc")
        ],
        ("id", "name")
    )

    new_df1 = source_df.empty_clone().add_rows([
        (3, "chi"),
        (4, "teko")
    ])

    new_df2 = source_df.add_row((3, "chi"))

    assert isinstance(new_df1, DataFrame)
    assert isinstance(new_df2, DataFrame)
    assert new_df1.expect_schema(source_df)
    assert new_df2.expect_schema(source_df)

    assert new_df1.expect_data([
        (3, "chi"),
        (4, "teko")
    ])
    assert new_df2.expect_data([
        (1, "nguyen"),
        (2, "thuc"),
        (3, "chi")
    ])


def test_missing_columns(spark):
    source_df: DataFrame = spark.createDataFrame(
        [(1, "thuc", 1.2)],
        ("id", "name", "score")
    )

    target_df1: DataFrame = spark.createDataFrame(
        [(1, "thuc", 1.2, 35)],
        ("id", "name", "score", "trans")
    )

    target_df2: DataFrame = spark.createDataFrame(
        [(1, "thuc")],
        ("id", "name")
    )

    assert source_df.missing_columns(["id", "name", "trans"]) == ["trans"]
    assert source_df.missing_columns(["id", "name"]) == []
    assert source_df.missing_columns(["id", "name", "score"]) == []
    assert source_df.missing_columns(target_df1) == ["trans"]
    assert source_df.missing_columns(target_df2) == []


def test_common_columns(spark):
    source_df: DataFrame = spark.createDataFrame(
        [(1, "thuc", 1.2)],
        ("id", "name", "score")
    )

    target_df1: DataFrame = spark.createDataFrame(
        [(1.2, "thuc", 35)],
        ("score", "name", "trans")
    )

    assert source_df.common_columns([]) == []
    assert source_df.common_columns(["name", "score", "trans"]) == ["name", "score"]
    assert source_df.common_columns(["score", "name", "trans"]) == ["score", "name"]
    assert source_df.common_columns(["score", "name", "trans"], exclude_cols=["score"]) == ["name"]
    assert source_df.common_columns(["score", "name", "trans"], exclude_cols=["name"]) == ["score"]
    assert source_df.common_columns(target_df1) == ["score", "name"]
    assert source_df.common_columns(target_df1, exclude_cols=["score"]) == ["name"]


class SampleTransformation:
    @staticmethod
    def activate(df: DataFrame):
        return df.withColumn("active", F.lit(1))

    @staticmethod
    def with_label(label: str):
        def inner(df):
            return df.withColumn("label", F.lit(label))

        return inner

    @staticmethod
    def with_label2(df: DataFrame, label: str):
        return df.withColumn("label2", F.lit(label))


def test_chaining_transformation(spark: SparkSession):
    data = [
        (1, "nguyen"),
        (2, "chi"),
        (3, "thuc")
    ]

    source_df: DataFrame = spark.createDataFrame(
        data,
        ("id", "name")
    )

    actual_df = source_df \
        .transform(SampleTransformation.activate) \
        .transform(SampleTransformation.with_label("A")) \
        .transform(SampleTransformation.with_label2, "B")

    expected_data = [
        (1, "nguyen", 1, "A", "B"),
        (2, "chi", 1, "A", "B"),
        (3, "thuc", 1, "A", "B")
    ]

    expected_df = spark.createDataFrame(
        expected_data,
        ("id", "name", "active", "label")
    )

    assert actual_df.expect_data(expected_data)


#     assert actual_df.expect_df(expected_df)


def test_squash(spark: SparkSession):
    name = "Only one version of each record can be kept"
    source_df = spark.createDataFrame([
        (1, 1, 1),
        (1, 2, 2,),
    ], ['ID', 'VALUE', 'date'])
    expected_df = spark.createDataFrame([(1, 2)], ['ID', 'VALUE'])
    actual_df = source_df.squash(primary_key='ID', partition_col='date')
    assert actual_df.expect_df(expected_df), name

    name = "Not squash if partition_col is not existed"
    source_df = spark.createDataFrame([
        (1, 1, 1),
        (1, 2, 2,),
    ], ['ID', 'VALUE', 'date'])
    expected_df = source_df
    actual_df = source_df.squash(primary_key='ID', partition_col='other_partition_col')
    assert actual_df.expect_df(expected_df), name

    name = "Work well if primary key is a list"
    source_df = spark.createDataFrame([
        (1, 0, 1, 1),
        (1, 0, 2, 2,),
    ], ['ID', 'KEY', 'VALUE', 'date'])
    expected_df = spark.createDataFrame([(1, 0, 2)], ['ID', 'KEY', 'VALUE'])
    actual_df = source_df.squash(primary_key=['ID', 'KEY'], partition_col='date')
    assert actual_df.expect_df(expected_df), name

    name = "Keep partition value"
    source_df = spark.createDataFrame([
        (1, 0, 1, 1),
        (1, 0, 2, 2,),
    ], ['ID', 'KEY', 'VALUE', 'date'])
    expected_df = spark.createDataFrame([(1, 0, 2, 2)], ['ID', 'KEY', 'VALUE', 'date'])
    actual_df = source_df.squash(primary_key=['ID', 'KEY'], partition_col='date', keep_partition_col=True)
    assert actual_df.expect_df(expected_df), name

    name = "Sort ascending partition col"
    source_df = spark.createDataFrame([
        (1, 0, 1, 1),
        (1, 0, 2, 2,),
    ], ['ID', 'KEY', 'VALUE', 'date'])
    expected_df = spark.createDataFrame([(1, 0, 1, 1)], ['ID', 'KEY', 'VALUE', 'date'])
    actual_df = source_df.squash(primary_key=['ID', 'KEY'], partition_col='date', keep_partition_col=True,
                                 descending_order=False)
    assert actual_df.expect_df(expected_df), name
