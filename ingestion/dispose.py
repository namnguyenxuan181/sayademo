import logging

import requests
from datetime import date
from common.constants import ShopInfo
from common.config import shop_cookie
from common.utils import RunDate

logger = logging.getLogger()


def download_dispose():
    run_date = RunDate(date.today())
    customer = requests.get(
        f'https://{ShopInfo.NAME}.pos365.vn/Export/RoomHistory?time=7days',
        headers={
            'origin': f'https://{ShopInfo.NAME}.pos365.vn',
            'referer': f'https://{ShopInfo.NAME}.pos365.vn/',
            'Content-Type': 'application/json; charset=UTF-8',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        },
        cookies=shop_cookie
    )
    logger.info(f'download raw dispose in {run_date} for 7 days')
    with open(f"data/raw/dispose/{run_date}.xlsx", 'wb') as f:
        f.write(customer.content)


if __name__ == '__main__':
    download_dispose()

