from enum import Enum

from pyspark.sql import Column, DataFrame, functions as F
from pyspark.sql.types import Row, LongType, StructType, StructField
from pyspark.sql.window import Window, WindowSpec


class Hint(Enum):
    SINGLE_PARTITION = 1
    DISTRIBUTED = 2


def generate_sequential_ids(
    df: DataFrame, offset=0, id_col="id", sortable_col=None, hint=Hint.SINGLE_PARTITION
) -> DataFrame:
    """Generate monotonically increasing and unique, and consecutive id

    Reason on why most solutions are not scalable and needed this func
    https://towardsdatascience.com/adding-sequential-ids-to-a-spark-dataframe-fa0df5566ff6

    Args:
        df: input data frame that doesn't have sequential id
        offset: define when the id number should start with
        id_col: name of the sequential id column, default to "id"
        sortable_col: only use for SINGLE_PARTITION hint, where data will be sorted based on it
            in a single partition
        hint: SINGLE_PARTITION: suitable for dataset fit in a single mem partition
              DISTRIBUTED: using zipWithIndex and recreate the dataframe

    Returns:
        dataframe with sequential id as first column, and then columns from original dataframe
    """
    if hint == Hint.SINGLE_PARTITION:
        return generate_sequential_ids_single_partition(df, offset, id_col, sortable_col)
    elif hint == Hint.DISTRIBUTED:
        return generate_sequential_ids_distributed(df, offset, id_col)
    else:
        return generate_sequential_ids_distributed(df, offset, id_col)


def generate_sequential_ids_single_partition(
    df: DataFrame, offset=0, id_col="id", sortable_col=None
) -> DataFrame:
    unique_id_col = sortable_col if sortable_col else "monotonically_increasing_id"
    # Add monotonically increasing 64-bit integers if no sortable column available
    unique_id_df = (
        df
        if sortable_col
        else df.withColumn(unique_id_col, F.monotonically_increasing_id())
    )

    window_spec: WindowSpec = Window.orderBy(F.col(unique_id_col))
    # Generate sequential ids
    final_id_col: Column = F.lit(offset) + F.row_number().over(window_spec)
    final_df = unique_id_df.withColumn(id_col, final_id_col)
    # Don't drop sortable col if it is parts of the original df column
    # only drop when using helper column monotonically increasing id
    final_df = final_df if sortable_col else final_df.drop(unique_id_col)
    return final_df.select(F.col(id_col).cast(LongType()), *df.columns)


def generate_sequential_ids_distributed(df: DataFrame, offset=0, id_col="id") -> DataFrame:
    spark = df.sql_ctx.sparkSession

    assert id_col not in df.columns, f"DataFrame've already contained `{id_col}` column"

    indexed_rdd = df.rdd.zipWithIndex()
    indexed_df = spark.createDataFrame(indexed_rdd, schema=StructType([
        StructField('data_dict', df.schema),
        StructField('index', LongType())
    ]))

    final_df = indexed_df.withColumn(id_col, F.col('index') + 1 + offset)

    return final_df.select(
        id_col,
        *[
            F.col(f'data_dict.{col}').alias(col)
            for col in df.columns
        ],
    )
