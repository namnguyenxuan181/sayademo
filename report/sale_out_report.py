from datetime import date, timedelta
import pandas as pd
from pyspark.sql import functions as F, types as T, DataFrame, SparkSession
import common.spark.extension.dataframe_ext
from common.utils import RunDate

BOOKING = 'booking'
REFER = 'refer'
RECEPTIONIST = 'receptionist'


class SaleOutReport:
    ORDER_SCHEMA = T.StructType([
        T.StructField('tran_at', T.TimestampType()),
        T.StructField('order_code', T.StringType()),
        T.StructField('amount', T.LongType()),
    ])
    ORDER_DETAIL_SCHEMA = T.StructType([
        T.StructField('order_code', T.StringType()),
        T.StructField('product_name', T.StringType()),
        T.StructField('quantity', T.DoubleType()),
    ])

    OUTPUT_SCHEMA = T.StructType([
        T.StructField('employee', T.StringType()),
        T.StructField('revenue', T.LongType()),
        T.StructField('revenue_wtd', T.LongType()),
        T.StructField('revenue_mtd', T.LongType()),
    ])

    @staticmethod
    def get_booking(df: DataFrame) -> DataFrame:
        return (
            df
            .filter(
                F.col('product_name').contains(BOOKING) |
                F.col('product_name').contains(REFER)
            ).squash(
                primary_key='order_code', partition_col='quantity'
            )
            .select('order_code', F.col('product_name').alias('employee'))
        )

    @staticmethod
    def populate_order(order_detail: DataFrame, order: DataFrame) -> DataFrame:
        return order.select('order_code', 'amount', 'tran_at').join(order_detail, on='order_code', how='left')

    @staticmethod
    def aggregate(df: DataFrame, report_date: date) -> DataFrame:
        condition_1d = F.to_date('tran_at') == report_date
        condition_wtd = F.to_date('tran_at').between(report_date - timedelta(days=report_date.weekday()), report_date + timedelta(days=1))
        condition_mtd = F.to_date('tran_at').between(report_date.replace(day=1), report_date+timedelta(days=1))
        return df.groupBy('employee').agg(
            F.sum(F.when(condition_1d, F.col('amount'))).alias('revenue'),
            F.sum(F.when(condition_wtd, F.col('amount'))).alias('revenue_wtd'),
            F.sum(F.when(condition_mtd, F.col('amount'))).alias('revenue_mtd'),
        ).withColumn('report_date', F.lit(report_date))

    @staticmethod
    def run(order: DataFrame, order_detail: DataFrame, report_date: date) -> DataFrame:
        return(
            order_detail.transform(SaleOutReport.get_booking)
            .transform(SaleOutReport.populate_order, order)
            .transform(SaleOutReport.aggregate, report_date)
        )


if __name__ == "__main__":

    run_date = RunDate(date.today())
    spark = SparkSession.builder.getOrCreate()

    sale_order = spark.createDataFrame(
        pd.read_csv(f'data/etl/fact/sale_order/{run_date}.csv')[['tran_at', 'order_code', 'amount']]
    )
    sale_order_detail = spark.createDataFrame(
        pd.read_csv(f'data/etl/fact/sale_order_detail/{run_date}.csv')[['order_code', 'product_name', 'quantity']]
    )

    sale_out_report = SaleOutReport.run(sale_order, sale_order_detail, run_date.to_date())
    sale_out_report.toPandas().to_csv(f'data/report/sale_out_report/{run_date}.csv', index=False)

