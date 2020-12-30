from datetime import date

from common.utils import RunDate
from common.constants import Report, TimeRange
from ingestion.base_ingest import SaleOrderCrawler


def download_sale_order():
    run_date = RunDate(date.today())
    save_path = f'data/raw/sale_order/{run_date}.xlsx'
    SaleOrderCrawler(
        report_name=Report.ORDER_REPORT,
        time_range=TimeRange.TIME_RANGE_7DAYS,
        save_path=save_path,
    ).run()


if __name__ == '__main__':
    download_sale_order()
