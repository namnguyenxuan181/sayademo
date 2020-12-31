# Spark Extensions

This package contains some extensions to existing Spark DataFrame, Row and Column for better development
productivity.

## How to use
Just import needed extensions to use all the magics:

- For **production**

```
# noinspection PyUnresolvedReferences
from common.spark_extensions import dataframe_ext
```

- For **dev/test**

The only differences between `_for_test` extension and prod extension: `df.expect_df` and `df.expect_data` are available
in dev/test version only.
```
# noinspection PyUnresolvedReferences
from common.spark_extensions import dataframe_ext_for_test
```

### Quick recipes
Refer to `test/dataframe_ext_test.py` for more samples of usage.

- Chaining transformation
```
class SampleTransformation:
    @staticmethod
    def activate(df: DataFrame):
        return df.withColumn("active", F.lit(1))

    @staticmethod
    def with_label(label: str):
        def inner(df):
            return df.withColumn("label", F.lit(label))

        return inner

actual_df = source_df \
        .transform(SampleTransformation.activate) \
        .transform(SampleTransformation.with_label("A"))
```

- Check a dataframe for expected schema:
```
df.expect_schema(["col1", "col2"])
```
or:
```
df1.expect_schema(df2)
```

- Check two dataframes for equality (both schema and data), available in `_for_test` version only:
```
actual_df.expect_df(expected_df)
```

or

```
actual_df.expect_data([
        (1, "nguyen"),
        (2, "thuc")
    ])
```

- Rename columns of a dataframe:
```
df.rename_cols({
    "old_col1": "new_col1",
    "old_col2": "new_col2",
    "old_col3": "new_col3",
})
```

- Refer to `test/dataframe_ext_test.py` for samples of usage.

### DataFrame extensions

### Row extensions

### Column extensions

### SparkSession extensions