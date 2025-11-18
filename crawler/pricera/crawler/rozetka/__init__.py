from dataclasses import dataclass
from pricera.common.collectors import BaseCollector


@dataclass
class RozetkaProductCrawler(BaseCollector):
    urls: list[str]
    payload_key: str = "rozetka_product"

    def crawl(self):
        from pricera.crawler.rozetka.spiders.product_spider import RozetkaProductSpider

        return self.process_scrapy_spider(spider_cls=RozetkaProductSpider, urls=self.urls, proxy_config=None)

    @classmethod
    def get_crawler(cls, message: dict) -> "RozetkaProductCrawler":
        payload = message["payload"]
        return RozetkaProductCrawler(urls=payload[cls.payload_key])
