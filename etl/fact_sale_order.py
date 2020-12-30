from datetime import date

import pandas as pd

from common.utils import RunDate


class FactSaleOrder:
    def __init__(self, run_date: RunDate):
        self.run_date = run_date

    INPUT_COLS = ['_0', 'order_code', '_1', 'tran_at', 'employee', 'table', 'customer_count', '_2', '_3', 'discount', 'amount', '_4', 'debit']
    SELECT_COLS = ['tran_at', 'order_code', 'employee', 'table', 'customer_count', 'discount', 'amount']

    @staticmethod
    def remove_invalid_data(row: str):
        row = str(row)
        if len(row) > 16:
            return row
        return None

    def clean(self):

        raw_data = pd.read_excel(f'data/raw/sale_order/{self.run_date}.xlsx', names=self.INPUT_COLS)
        raw_data['tran_at'] = raw_data['tran_at'].apply(self.remove_invalid_data)
        raw_data = raw_data[(raw_data.tran_at > '0') & (raw_data.employee > '0')].astype({'employee': 'str'})

        cleaned_sale_order = raw_data[self.SELECT_COLS]

        return cleaned_sale_order

    def clean_and_save_data(self):
        cleaned_data = self.clean()
        save_path = f'data/etl/fact/sale_order/{self.run_date}.csv'
        cleaned_data.to_csv(save_path, index=False)


if __name__ == '__main__':
    run_date = RunDate(date.today())
    print(f'run fact sale_order for {run_date}')
    FactSaleOrder(run_date).clean_and_save_data()
