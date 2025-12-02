from dataclasses import dataclass
from pricera.common.collectors import BaseCollector
from pricera.models import URLWithHash
from rozetka_mixins import RozetkaProductMixin
import logging

logger = logging.getLogger("rozetka_product_crawler")


@dataclass
class RozetkaProductCrawler(BaseCollector, RozetkaProductMixin):
    urls: list[str]

    def __post_init__(self):
        super().__init__()
        self.urls_with_hash: list[URLWithHash] = self.prepare_urls(self.urls)

    def crawl(self):
        from pricera.rozetka.spiders.rozetka_product_spider import RozetkaProductSpider

        return self.process_scrapy_spider(
            spider_cls=RozetkaProductSpider,
            storage_bucket=self.storage_bucket,
            storage_prefix=self.storage_prefix,
            start_urls=self.urls_with_hash,
            proxy_config=None,
        )

    @classmethod
    def get_crawler(cls, message: dict) -> "RozetkaProductCrawler":
        payload = message["payload"]
        return cls(urls=payload[cls.payload_key])
