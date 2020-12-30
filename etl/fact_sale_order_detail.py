from datetime import date

import pandas as pd

from common.utils import RunDate


class FactSaleOrder:
    def __init__(self, run_date: RunDate):
        self.run_date = run_date

    INPUT_COLS = ['product_id', '_0', 'product_name', '_1', '_2', '_3', '_4', 'order_code', '_5',
                  'total_amount', 'tran_at', 'customer_id', 'customer_name', '_7', 'quantity',
                  'unit_prices', '_8', 'retail_prices', 'discount_amount', '_9', '_10', 'amount']
    SELECT_COLS = ['tran_at', 'order_code', 'product_id', 'product_name', 'customer_id', 'customer_name', 'quantity', 'unit_prices', 'discount_amount', 'amount']

    @staticmethod
    def remove_invalid_data(row: str):
        row = str(row)
        if len(row) > 16:
            return row
        return None

    def clean(self):

        raw_data = pd.read_excel(f'data/raw/sale_order_detail/{self.run_date}.xlsx', names=self.INPUT_COLS)

        raw_data['tran_at'] = raw_data['tran_at'].apply(self.remove_invalid_data)
        raw_data = raw_data[(raw_data.tran_at > '0') & (raw_data.product_name > '0')]

        cleaned_sale_order = raw_data[self.SELECT_COLS]

        return cleaned_sale_order

    def clean_and_save_data(self):
        cleaned_data = self.clean()
        save_path = f'data/etl/fact/sale_order_detail/{self.run_date}.csv'
        cleaned_data.to_csv(save_path, index=False)


if __name__ == '__main__':
    run_date = RunDate(date.today())
    print(f'run fact sale_order_detail for {run_date}')
    FactSaleOrder(run_date).clean_and_save_data()
