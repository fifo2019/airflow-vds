from airflow.sensors.base import BaseSensorOperator
import requests


class ParquetFileSensor(BaseSensorOperator):
    template_fields = ('url',)

    def __init__(self, url, **kwargs):
        super().__init__(**kwargs)
        self.url = url

    def poke(self, context):
        try:
            r = requests.head(self.url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
            self.log.info(f"URL: {self.url} | Status: {r.status_code}")
            return r.status_code == 200
        except Exception as e:
            self.log.warning(f"Error: {e}")
            return False