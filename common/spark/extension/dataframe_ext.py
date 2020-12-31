from typing import Callable, List, Tuple, Dict, Union, Optional, Iterable

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DataType

# noinspection PyUnresolvedReferences
from .row_ext import *

__author__ = ['ThucNC', 'NhatNV', 'TramNN']


class DataFrameExt:
    @staticmethod
    def get_sample(self: DataFrame, n=1000, seed=None):
        """
        Return a sampled subset of current dataframe
        :param self:
        :param n:
            if 0 < float(n) <=1 --> fraction of sample
            if n > 1 --> number of sample
        :param seed: seed for sampling (default - random seed)
        :return:
        """
        if 0 <= n <= 1:
            return self.sample(False, fraction=float(n), seed=seed)

        n = int(n)

        if n < 0:
            raise ValueError(f"Invalid sample size {n}")

        rdd = self.rdd.takeSample(False, n, seed=seed)

        if not rdd:
            return self.empty_clone()

        return self.sql_ctx.createDataFrame(rdd, schema=self.schema)

    @staticmethod
    def squash(
        self: DataFrame,
        primary_key: Union[str, List[str]] = 'ID',
        partition_col: str = 'date',
        keep_partition_col: bool = False,
        descending_order: bool = True
    ):
        """
        Squash data (if needed) by a window function when we load multiple snapshots of source data.
        For each records (determinate by primary key), keep the newest version of it (determinate by partition).

        Args:
            self: current data-frame
            primary_key: primary-key column
            partition_col: partition column (each partition is a snapshot of data)
            keep_partition_col: True if drop `partition_col` after squash else False
            descending_order: sort partition_col descending if True, otherwise sort partition_col ascending

        Returns: squashed data-frame

        Example:
            Given a DataFrame `df` of two partitions that contain two versions of a same record.
            Columns: `ID`: primary key (a.k.a natural key)
                     `VALUE`: an attribute of record that can change
                     `date`: partition column, the date that data was captured
            +-----+-------+----------+
            | ID  | VALUE | date     |
            +-----+-------+----------+
            | 1   | x     | 20200101 |
            +-----+-------+----------+
            | 1   | y     | 20200102 |  <- The value is changed from 'x' to 'y' - Called version 2nd of Rercord(ID=1)
            +-----+-------+----------+

            To get newest version of Record(ID=1) (and also every record that existed):
                squashed_data = df.squash(primary_key='ID', partition_col='date')
            In shorthand (since params have default values):
                squashed_data = df.squash()

            And the result will be:
            +-----+-------+
            | ID  | VALUE |
            +-----+-------+
            | 1   | y     |
            +-----+-------+
            Note that partition column (in this case is `date` column) will be removed.

        """
        primary_keys = [primary_key] if isinstance(primary_key, str) else primary_key
        order_by_col = F.desc(partition_col) if descending_order else partition_col
        if partition_col in self.columns:
            window = Window().partitionBy(*primary_keys).orderBy(order_by_col)
            squash_df = (
                self
                    .withColumn('_rn', F.row_number().over(window))
                    .filter(F.col('_rn') == 1)
                    .drop('_rn')
            )
            return squash_df if keep_partition_col else squash_df.drop(partition_col)
        return self

    @staticmethod
    def select_cols(self: DataFrame, cols: Union[Iterable[str], Dict[str, str]]):
        """
        Select and rename columns in dataframe at the same time
        :param self:
        :param cols:
            - if cols is a list of column names --> normal select
            - if cols is a mapping of current names and new names --> select with rename
        :return:
        """
        if isinstance(cols, dict):
            return self.select(*[
                F.col(column).alias(alias)
                for column, alias in cols.items()
            ])
        else:
            return self.select(*cols)

    @staticmethod
    def prefix_cols(
        self: DataFrame,
        prefix: str = '',
        suffix: str = '',
        cols: Optional[Iterable[str]] = None,
        exclude_cols: Optional[Iterable[str]] = None):
        """
        Add prefix and/or suffix to multiple of columns
        :param self:
        :param prefix: prefix to add
        :param suffix: suffix to add
        :param cols: list of col names to add prefix, if None, add to all columns
        :param exclude_cols: list of col names to exclude
        :return:
        """
        if not cols:
            cols = self.columns

        cols = self.common_columns(cols, exclude_cols)
        prefix = prefix or ''
        suffix = suffix or ''
        mapping = {col: prefix + col + suffix for col in cols}
        return self.rename_cols(mapping)

    @staticmethod
    def suffix_cols(
        self: DataFrame,
        suffix: str = '',
        prefix: str = '',
        cols: Optional[Iterable[str]] = None,
        exclude_cols: Optional[Iterable[str]] = None):
        return self.prefix_cols(prefix=prefix, suffix=suffix, cols=cols, exclude_cols=exclude_cols)

    def apply_cols(self: DataFrame, cols: List[str], fn: Callable) -> DataFrame:
        """
        Apply transformation to multiple columns at once
        Args:
            self: current dataframe
            cols: list of column names to be transformed
            fn: a function that does actual transformation

        Returns:

        """
        df = self
        if not cols:
            return df
        for column in cols:
            df = df.withColumn(column, fn(F.col(column)))
        return df

    def transform(self: DataFrame, fn: Callable, *args, **kwargs) -> DataFrame:
        """
        Support dataframe chaining transformation
        Args:
            self: current dataframe
            fn: a function that does actual transformation

        Returns:
            transformed dataframe
        """
        return fn(self, *args, **kwargs)

    def expect_schema(self: DataFrame, schema: Union[DataFrame, List[str]]) -> bool:
        """
        Check schema of current dataframe, return True if it has same schema as `schema`.

        `schema` can be another Dataframe, schema of which will be compared, or a list of column names.

        The order of columns in `schema` is not important.

        Args:
            self: current dataframe
            schema: Dataframe or list of column names

        Returns:

        """
        if isinstance(schema, DataFrame):
            schema: DataFrame
            return sorted(self.dtypes) == sorted(schema.dtypes)
        elif isinstance(schema, list):
            return sorted(self.columns) == sorted(schema)
        else:
            raise TypeError(f"Expected `DataFrame` or `list` type for `schema`, but {type(schema)} found.")

    def to_list_dict(self: DataFrame, sort_dict_keys=False) -> List[Dict]:
        """
        Collect current dataframe and convert it to Python list of row dicts
        Args:
            self: current dataframe
            sort_dict_keys: sort keys of row dicts, this works for Python 3.7+ only

        Returns:

        """
        return [row.to_dict(sort_keys=sort_dict_keys) for row in self.collect()]

    def to_list_tuple(self) -> List[Tuple]:
        """
        Collect current dataframe and convert it to Python list of tuple
        Args:
            self: current dataframe

        Returns:

        """
        return [row.to_tuple() for row in self.collect()]

    def empty_clone(self: DataFrame):
        """
        Create an empty dataframe with the same schema as current one
        Args:
            self:

        Returns:

        """
        return self.sql_ctx.createDataFrame([], self.schema)

    def add_rows(self: DataFrame, row_data: List[Tuple]) -> DataFrame:
        """
        Add rows (tuples of data) to current dataframe and return new dataframe
        Args:
            self:
            row_data: a list of tuples, represents row data

        Returns:

        """
        additional_df = self.sql_ctx.createDataFrame(row_data, self.schema)
        return self.unionByName(additional_df)

    def add_row(self: DataFrame, row_data: List[Tuple]) -> DataFrame:
        """
        Add a row (a tuple of data) to current dataframe and return new dataframe
        Args:
            self:
            row_data: a tuple, represents row data

        Returns:

        """
        return self.add_rows([row_data])

    def missing_columns(self: DataFrame, another_df: Union[DataFrame, List[str]]):
        """
        Return list of columns that current dataframe misses from list of given column names `columns`
        Args:
            self:
            another_df: second DataFrame or list of column names

        Returns:

        """
        if isinstance(another_df, DataFrame):
            another_df: DataFrame
            cols = another_df.columns
        elif isinstance(another_df, list):
            cols = another_df
        else:
            raise TypeError(f"Expected `DataFrame` or `list` type for `another_df`, but {type(another_df)} found.")

        return list(set(cols) - set(self.columns))

    def common_columns(self: DataFrame, another_df: Union[DataFrame, List[str]] = None, exclude_cols=None):
        """
        Common columns of two dataframes, preserve the column order in the second one
        Args:
            self: current DataFrame
            another_df: second DataFrame or list of column names
            exclude_cols: column names to exclude

        Returns:

        """
        if not another_df:
            cols = []
        elif isinstance(another_df, DataFrame):
            another_df: DataFrame
            cols = another_df.columns
        elif isinstance(another_df, list):
            cols = another_df
        else:
            raise TypeError(f"Expected `DataFrame` or `list` type for `another_df`, but {type(another_df)} found.")

        exclude_cols = exclude_cols or []
        columns = (set(self.columns) & set(cols)) - set(exclude_cols)

        return [c for c in cols if c in columns]

    def trim_cols(self: DataFrame, cols: List[str]):
        """
            Quickly trim a bunch of columns useful in pre-processing
            :return: trimmed DataFrame
            """
        return self.apply_cols(cols, F.trim)

    def upper_cols(self: DataFrame, cols: List[str]):
        """
            Quickly upper-case a bunch of columns useful in pre-processing
            :return: upper-cased DataFrame
            """
        return self.apply_cols(cols, F.upper)

    def lower_cols(self: DataFrame, cols: List[str]):
        """
            Quickly low-case a bunch of columns useful in pre-processing
            :return: low-cased DataFrame
            """
        return self.apply_cols(cols, F.lower)

    def cast_cols(self: DataFrame, cols: List[str], data_type: Union[str, DataType]) -> DataFrame:
        """
        Quickly convert a bunch of columns to a data type.
        Useful when we need to convert Float-ID from the source to Integer-ID/Long-ID
        :return: converted DataFrame
        """
        return self.apply_cols(cols, lambda column: column.cast(data_type))

    def rename_cols(self: DataFrame, name_mapping: dict) -> DataFrame:
        """
        Quickly rename list of columns specified in name_mapping
        Args:
            name_mapping: {"old_name1": "new_name1", "old_name2": "new_name2"}

        Returns:
            converted DataFrame
        """
        df = self
        if not name_mapping:
            return df
        for old_name, new_name in name_mapping.items():
            df = df.withColumnRenamed(old_name, new_name)
        return df

    def pd_head(self: DataFrame, n: int = 10):
        """
        Take top `n` row and convert to pandas dataframe
        Args:
            n:

        Returns:

        """
        return self.limit(n).toPandas()


# Extend Spark DataFrame with all public methods from DataFrameExt
for func_name in dir(DataFrameExt):
    func = getattr(DataFrameExt, func_name)
    if callable(func) and not func_name.startswith("__"):
        setattr(DataFrame, func_name, func)
