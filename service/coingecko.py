"""Класс биржи, необходимый для получения стоимости ETH"""

from prometheus_client import Gauge
from requests import Session
from misc import COINGECKO_APIKEY


class Coingecko:

    url = 'https://api.coingecko.com/api/v3/simple/price?ids=ethereum'
    prom_ethusd_gauge = Gauge("eth_usd", "ETH/USD обменный курс")
    prom_ethrub_gauge = Gauge("eth_rub", "ETH/RUB обменный курс")
    prometheus_metrics = {'usd' : prom_ethusd_gauge, 'rub': prom_ethrub_gauge}

    def __init__(self) -> None:
        self.session = Session()

    def get_eth_exchange_rate(self, currency: str) -> None:
        """Передаём стоимость ETH в Prometheus

        """
        try:
            response = self.session.get(f'{Coingecko.url}&vs_currencies={currency}', headers={'x-cg-api-key': COINGECKO_APIKEY})
            rate = response.json().get('ethereum').get(currency)
            Coingecko.prometheus_metrics.get(currency).set(rate)
            print(f'Стоимость ETH в {currency} = {rate}')
        except:
            print(f'Невозможно определить стоимость ETH ({currency})')

