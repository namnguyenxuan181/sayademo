from datetime import datetime, date

import pandas as pd
import xlrd

from common.utils import RunDate
from pyspark.sql import types as T


class FactDispose:
    def __init__(self, run_date: RunDate):
        self.run_date = run_date

    RENAME_COLS = {
        'Người tạo': 'employee',
        'Tên Phòng / Bàn': 'table',
        'Ngày tạo': 'tran_at',
        'Lý do': 'reason',
        'Tên hàng hóa': 'product',
        'Số lượng': 'quantity',
        'Giá bán': 'prices',
        'Thao tác sau tạm tính?': 'new_amount',
    }
    SELECT_COLS = ['tran_at', 'employee', 'product', 'table', 'quantity', 'prices', 'reason']
    OUTPUT_SCHEMA = T.StructType([
        T.StructField('tran_at', T.TimestampType()),
        T.StructField('employee', T.StringType()),
        T.StructField('product', T.StringType()),
        T.StructField('table', T.StringType()),
        T.StructField('quantity', T.DoubleType()),
        T.StructField('prices', T.LongType()),
        T.StructField('reason', T.StringType()),
    ])

    @staticmethod
    def convert_time(row):
        return datetime(*xlrd.xldate_as_tuple(row, 0))

    def clean(self):

        raw_data = pd.read_excel(f'data/raw/dispose/{self.run_date}.xlsx').rename(columns=self.RENAME_COLS)

        raw_data['tran_at'] = raw_data['tran_at'].apply(self.convert_time)

        cleaned_dispose = raw_data[self.SELECT_COLS]

        return cleaned_dispose

    def clean_and_save_data(self):
        cleaned_data = self.clean()

        save_path = f'data/etl/fact/dispose/{self.run_date}.csv'
        cleaned_data.to_csv(save_path, index=False)


if __name__ == '__main__':
    run_date = RunDate(date.today())

    FactDispose(run_date).clean_and_save_data()
