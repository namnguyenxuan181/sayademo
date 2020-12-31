import json
from typing import List, Tuple, Union

from pyspark.sql import DataFrame

# noinspection PyUnresolvedReferences
from common.spark.extension import dataframe_ext
# noinspection PyUnresolvedReferences
from .row_ext import *

__author__ = 'ThucNC'


class DataFrameExtForTest:
    def expect_data(self, data: List[Tuple]) -> bool:
        """
        Check current dataframe for having given data

        Args:
            self: current dataframe
            data: list of tuples, each tuple present a sing row

        Returns:

        """
        return sorted(self.to_list_tuple(), key=str) == sorted(data)

    def expect_df(self: DataFrame, another_df: DataFrame) -> bool:
        """
        Check two dataframe for equality: same data and schema
        Args:
            self: current dataframe
            another_df:

        Returns:

        """
        if not self.expect_schema(another_df):
            return False
        # use key=lambda x: json.dumps(x, sort_keys=True) instead of str for nested Row
        return \
            sorted(self.to_list_dict(sort_dict_keys=True),
                   key=lambda x: json.dumps(x, sort_keys=True, default=str)) \
            == sorted(another_df.to_list_dict(sort_dict_keys=True),
                      key=lambda x: json.dumps(x, sort_keys=True, default=str))

    def assert_schema_equals(self: DataFrame, another_df: DataFrame, error_message: Union[str] = None):
        """
        Check two of dataframe for equality.
        Args:
            self: current dataframe
            another_df:
            error_message:

        Raises:
            AssertError: 'Expected 2 schema equal.'
        """
        error_message = error_message or 'Expected 2 schema equal.'

        assert sorted(self.dtypes) == sorted(another_df.dtypes), error_message

    def assert_equals(self: DataFrame, another_df: DataFrame, error_message: Union[str] = None):
        """
        Check two dataframe for equality: same data and schema.
        Args:
            self: current dataframe
            another_df:
            error_message: error message.

        Raises:
            AssertError: 'Expected 2 data frame equal.'

        """
        self.assert_schema_equals(another_df)
        error_message = error_message or 'Expected 2 data frame equal.'
        # use key=lambda x: json.dumps(x, sort_keys=True) instead of str for nested Row
        assert sorted(self.to_list_dict(sort_dict_keys=True),
                      key=lambda x: json.dumps(x, sort_keys=True, default=str)) \
               == sorted(another_df.to_list_dict(sort_dict_keys=True),
                         key=lambda x: json.dumps(x, sort_keys=True, default=str)), \
            error_message


# Extend Spark DataFrame with all public methods from DataFrameExt
for func_name in dir(DataFrameExtForTest):
    func = getattr(DataFrameExtForTest, func_name)
    if callable(func) and not func_name.startswith("__"):
        setattr(DataFrame, func_name, func)
