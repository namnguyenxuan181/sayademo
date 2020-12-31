from datetime import date
import time
import pandas as pd

from pyspark.sql import SparkSession

from common.utils import RunDate
from etl.fact_sale_order import FactSaleOrder
from etl.fact_sale_order_detail import FactSaleOrderDetail

from ingestion.sale_order import download_sale_order
from ingestion.sale_order_detail import download_sale_order_detail
from report.sale_out_report import SaleOutReport
from common.sale_out_email import send_sale_out_email
REPORT_TIME_RANGE = 60 * 1


def main(spark: SparkSession):
    run_date = RunDate(date.today())
    while True:
        download_sale_order()
        FactSaleOrder(run_date).clean_and_save_data()
        download_sale_order_detail()
        FactSaleOrderDetail(run_date).clean_and_save_data()
        sale_order = spark.createDataFrame(
            pd.read_csv(f'data/etl/fact/sale_order/{run_date}.csv')[['tran_at', 'order_code', 'amount']]
        )
        sale_order_detail = spark.createDataFrame(
            pd.read_csv(f'data/etl/fact/sale_order_detail/{run_date}.csv')[['order_code', 'product_name', 'quantity']]
        )

        sale_out_report = SaleOutReport.run(
            sale_order, sale_order_detail, run_date.to_date()
        ).fillna('khách lẻ', subset=['employee']).fillna(0, subset=['revenue', 'revenue_wtd', 'revenue_mtd']).toPandas()

        send_sale_out_email(sale_out_report)
        sale_out_report.to_csv(f'data/report/sale_out_report/{run_date}.csv', index=False)
        time.sleep(REPORT_TIME_RANGE)


if __name__ == "__main__":
    _spark = SparkSession.builder.getOrCreate()
    main(_spark)

