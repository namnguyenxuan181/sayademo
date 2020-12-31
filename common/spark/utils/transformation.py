from pyspark.sql import DataFrame
from pyspark.sql.functions import date_format
from pyspark.sql.types import IntegerType, TimestampType, DateType, DataType

DATE_ID_FORMAT = 'yyyyMMdd'


def format_date_id(df: DataFrame, date_column: str, alias: str = 'date', date_id_format: str = DATE_ID_FORMAT,
                   dtype: DataType = IntegerType()) -> DataFrame:
    """Converting date/timestamp column to format integer/string
    Example:
        >>> df.show()
            +-----------+
            |date_column|
            +-----------+
            | 2020-01-01|
            +-----------+

        >>> format_date_id(df, date_column='date_column', alias='date_int')
            +-----------+--------+
            |date_column|date_int|
            +-----------+--------+
            | 2020-01-01|20200101|
            +-----------+--------+

    """
    data_type = df.select(date_column).schema[0].dataType
    if isinstance(data_type, TimestampType) or isinstance(data_type, DateType):
        return df.withColumn(alias, date_format(date_column, date_id_format).cast(dtype))
    raise TypeError(f'Expected column type is TimestampType or DateType, but got {data_type}')
