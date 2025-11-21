from dataclasses import dataclass
from pricera.common.collectors import BaseCollector


@dataclass
class RozetkaProductCrawler(BaseCollector):
    urls: list[str]
    payload_key: str = "rozetka_product"
    bucket: str = "pricera-crawled-data"
    path: str = "rozetka_product/"

    def crawl(self):
        from pricera.crawler.rozetka.spiders.product_spider import RozetkaProductSpider

        return self.process_scrapy_spider(
            RozetkaProductSpider, s3_bucket=self.bucket, s3_prefix=self.path, start_urls=self.urls, proxy_config=None
        )

    @classmethod
    def get_crawler(cls, message: dict) -> "RozetkaProductCrawler":
        payload = message["payload"]
        return cls(urls=payload[cls.payload_key])
