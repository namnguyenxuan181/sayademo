from datetime import date
import time
from pyspark.sql import SparkSession

from common.utils import RunDate
from etl.fact_sale_order import FactSaleOrder
from etl.dispose import FactDispose

from ingestion.dispose import download_dispose
from ingestion.sale_order import download_sale_order
from report.fraud_detection import detect_fraud

REPORT_TIME_RANGE = 60 * 1


def main(spark: SparkSession):
    run_date = RunDate(date.today())
    while True:
        download_sale_order()
        FactSaleOrder(run_date).clean_and_save_data()
        download_dispose()
        FactDispose(run_date).clean_and_save_data()
        detect_fraud(spark)
        time.sleep(REPORT_TIME_RANGE)


if __name__ == "__main__":
    _spark = SparkSession.builder.getOrCreate()
    main(_spark)

