from datetime import date, datetime

import pytest
from pyspark.sql.types import IntegerType, TimestampType, DateType, StructType, StructField, StringType

from common.spark.utils.transformation import format_date_id


def test_convert_to_int_date(spark):
    date_col = 'date_column'
    date_int_col = 'date'

    test_cases = [
        {
            'name': 'Happy path',
            'input_data': [(date(2020, 1, 1),)],
            'input_schema': StructType(
                [
                    StructField(date_col, DateType()),
                ]
            ),
            'expected_data': [(date(2020, 1, 1), 20200101)],
            'expected_schema': StructType(
                [
                    StructField(date_col, DateType()),
                    StructField(date_int_col, IntegerType()),
                ]
            )
        },
        {
            'name': 'Timestamp column will correctly handle',
            'input_data': [(datetime(2020, 1, 1),)],
            'input_schema': StructType(
                [
                    StructField(date_col, TimestampType()),
                ]
            ),
            'expected_data': [(datetime(2020, 1, 1), 20200101)],
            'expected_schema': StructType(
                [
                    StructField(date_col, TimestampType()),
                    StructField(date_int_col, IntegerType()),
                ]
            )
        },
        {
            'name': 'Wrong input data type will raise error',
            'input_data': [(20200101,)],
            'input_schema': StructType(
                [
                    StructField(date_col, IntegerType()),
                ]
            ),
            'expected_error_msg': 'Expected column type is TimestampType or DateType, but got IntegerType'
        },
        {
            'name': 'Format string works correctly',
            'input_data': [(date(2020, 1, 1),)],
            'input_schema': StructType(
                [
                    StructField(date_col, DateType()),
                ]
            ),
            'date_id_format': 'yyyy-MM-dd',
            'expected_data': [(date(2020, 1, 1), '2020-01-01')],
            'expected_schema': StructType(
                [
                    StructField(date_col, DateType()),
                    StructField(date_int_col, StringType()),
                ]
            )
        },
    ]

    for test in test_cases:
        input_df = spark.createDataFrame(test['input_data'], test['input_schema'])
        if 'expected_data' in test:
            expected_df = spark.createDataFrame(test['expected_data'], test['expected_schema'])
            if 'date_id_format' in test:
                actual_df = format_date_id(input_df, date_column=date_col, alias=date_int_col,
                                           date_id_format=test['date_id_format'], dtype=StringType())
            else:
                actual_df = format_date_id(input_df, date_column=date_col, alias=date_int_col)
            actual_df.assert_equals(expected_df, test['name'])
        else:
            with pytest.raises(TypeError) as exc_info:
                format_date_id(input_df, date_column=date_col, alias=date_int_col)
            assert exc_info.value.args[0] == test['expected_error_msg'], test['name']
