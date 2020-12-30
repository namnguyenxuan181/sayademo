from datetime import date

import pandas as pd
from pyspark.sql import SparkSession

from common.email import send_email
from common.utils import RunDate


def detect_order_fraud(df: pd.DataFrame) -> pd.DataFrame:
    cashier_list = ['cashier', 'Admin']
    select_cols = ['order_code', 'tran_at', 'employee', 'discount', 'amount']
    return df[~df['employee'].isin(cashier_list)][select_cols]


def detect_dispose_fraud(df: pd.DataFrame) -> pd.DataFrame:
    def detect(employee: str, product: str):
        cashier = 'cashier'
        admin = 'admin'
        if (employee != product) and (cashier not in employee) and (admin not in employee.lower()):
            return True
        return False
    select_cols = ['tran_at', 'employee', 'product', 'reason']
    df['is_fraud'] = df.apply(lambda x: detect(x['employee'], x['product']), axis=1)

    return df[df.is_fraud == True][select_cols]


def detect_fraud(spark: SparkSession):
    run_date = RunDate(date.today())
    print('detection fraud')
    sale_order = pd.read_csv(f'data/etl/fact/sale_order/{run_date}.csv')

    dispose = pd.read_csv(f'data/etl/fact/dispose/{run_date}.csv')
    try:
        existing_fraud_dispose = spark.createDataFrame(pd.read_csv(f'data/report/fraud_dispose/{run_date}.csv'))
        existing_fraud_order = spark.createDataFrame(pd.read_csv(f'data/report/fraud_order/{run_date}.csv'))
    except:
        existing_fraud_dispose = None
        existing_fraud_order = None

    fraud_dispose = spark.createDataFrame(detect_dispose_fraud(dispose))

    fraud_sale_order = spark.createDataFrame(detect_order_fraud(sale_order))

    new_fraud_dispose = fraud_dispose.subtract(existing_fraud_dispose).toPandas() if existing_fraud_dispose else fraud_dispose.toPandas()

    new_fraud_sale_order = fraud_sale_order.subtract(existing_fraud_order).toPandas() if existing_fraud_order else fraud_sale_order.toPandas()

    print(new_fraud_dispose)
    print(new_fraud_sale_order)
    if len(new_fraud_dispose) > 0 or len(new_fraud_sale_order) > 0:
        send_email(
            new_fraud_dispose,
            new_fraud_sale_order
        )
        fraud_dispose.toPandas().to_csv(f'data/report/fraud_dispose/{run_date}.csv', index=False)
        fraud_sale_order.toPandas().to_csv(f'data/report/fraud_order/{run_date}.csv', index=False)
    else:
        print("don't have any fraud data")


if __name__ == '__main__':
    _spark = SparkSession.builder.getOrCreate()
    detect_fraud(_spark)