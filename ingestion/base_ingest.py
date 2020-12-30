import logging
import requests
import time
from common.constants import ShopInfo

logger = logging.getLogger()


class SaleOrderCrawler:
    def __init__(self, report_name: str, time_range: str, save_path: str):
        self.base_url = 'https://reporting.pos365.vn/api/reports'
        self.report_name = report_name
        self.time_range = time_range
        self.save_path = save_path
        self.headers = {
            'origin': f'https://{ShopInfo.NAME}.pos365.vn',
            'referer': f'https://{ShopInfo.NAME}.pos365.vn/',
            'accept': 'application/json, text/javascript, */*; q=0.01',
            'Content-Type': 'application/json; charset=UTF-8',
        }

    def get_client(self) -> str:
        return requests.post(
            'https://reporting.pos365.vn/api/reports/clients',
            headers=self.headers
        ).json()['clientId']

    def get_instances(self, client: str) -> str:
        data = {
            "report": self.report_name,
            'parameterValues': {
                'filter': "{\"TimeRange\":\"%s\",\"Report\":\"%s\",\"SoldBy\":0,\"RID\":\"%s\",\"BID\":\"%s\",\"CurrentUserId\":%s}" \
                          % (self.time_range, self.report_name, ShopInfo.RID, ShopInfo.BID, ShopInfo.CurrentUserId)
            }
        }
        print(data)

        return requests.post(
            f'{self.base_url}/clients/{client}/instances',
            headers=self.headers,
            json=data
        ).json()['instanceId']

    def set_param(self, client: str):

        data = {
            "report": self.report_name,
            'parameterValues': {
                'filter': "{\"TimeRange\":\"%s\",\"Report\":\"%s\",\"SoldBy\":0,\"RID\":\"%s\",\"BID\":\"%s\",\"CurrentUserId\":%s}" \
                          % (
                              self.time_range, self.report_name, ShopInfo.RID, ShopInfo.BID, ShopInfo.CurrentUserId)
            }
        }
        param = requests.post(
            f'{self.base_url}/clients/{client}/parameters',
            headers=self.headers,
            json=data,

        )
        print('param', param.json())

    def get_base_document(self, client: str, instance: str) -> str:
        data = {
            'format': 'HTML5Interactive',
            'deviceInfo': {
                'enableSearch': True,
                'ContentOnly': True,
                'UseSVG': True,
                'BasePath': self.base_url,
            },
            'useCache': False
        }

        return requests.post(
            f'{self.base_url}/clients/{client}/instances/{instance}/documents',
            headers=self.headers,
            json=data,
        ).json()['documentId']

    def confirm_document_ready(self, client: str, instance: str, document: str):
        while (
            requests.get(
                f'{self.base_url}/clients/{client}/instances/{instance}/documents/{document}/info'
            ).json()['documentReady'] is not True
        ):
            print(requests.get(
                f'{self.base_url}/clients/{client}/instances/{instance}/documents/{document}/info').json()['documentReady'])
            time.sleep(1)

    def get_document(self,  client: str, instance: str, base_document: str) -> str:
        data = {
            "format": "XLS",
            "deviceInfo": {
                "enableSearch": True,
                "BasePath": self.base_url
            },
            "useCache": True,
            "baseDocumentID": base_document,
        }

        doc_id = requests.post(
            f'{self.base_url}/clients/{client}/instances/{instance}/documents',
            headers=self.headers,
            json=data
        ).json()['documentId']

        return doc_id

    def download_data(self, client: str, instance: str, document: str):
        report = requests.get(
            f'{self.base_url}/clients/{client}/instances/{instance}/documents/{document}?response-content-disposition=attachment',
            headers={
                'origin': f'https://{ShopInfo.NAME}.pos365.vn',
                'referer': f'https://{ShopInfo.NAME}.pos365.vn/',
                'Content-Type': 'application/json; charset=UTF-8',
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            },
        )
        print(self.save_path)
        with open(self.save_path, 'wb') as f:
            f.write(report.content)

    def run(self):
        logger.info(self.headers)
        client = self.get_client()
        logger.info(f'client: {client}')
        self.set_param(client)
        instance = self.get_instances(client=client)
        logger.info(f'instance: {instance}')
        base_document = self.get_base_document(client=client, instance=instance)
        logger.info(f'base_document: {base_document}')
        self.confirm_document_ready(client, instance, base_document)
        document = self.get_document(client, instance, base_document)
        logger.info(f'document: {document}')
        self.confirm_document_ready(client, instance, document)
        self.download_data(client=client, instance=instance, document=document)

