from pyspark.sql import DataFrame


def save_into_table(df: DataFrame, name: str, format: str = None, mode: str = None, **options):
    """
    Save df into given table.

    Args:
        df: The DataFrame you wanna save
        name: Table name that df save into
        format: Data file format, e.g: parquet, orc,...
        mode: Save mode: append, overwrite, error, ignore
        **options: Extra options

    Returns:
        None
    """
    spark = df.sql_ctx.sparkSession

    table = spark.table(name)
    assert sorted(df.columns) == sorted(table.columns)
    df = df.select(table.columns)

    writer = df.write.mode(mode).options(**options)
    if format is not None:
        writer.format(format)

    writer.insertInto(name)
