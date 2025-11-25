from dataclasses import dataclass
from pricera.common.collectors import BaseCollector
from pricera.models import URLWithHash


@dataclass
class RozetkaProductCollector(BaseCollector):
    urls: list[str]
    payload_key: str = "rozetka_product"
    bucket: str = "pricera-crawled-data"
    path: str = "rozetka_product/"

    def __post_init__(self):
        super().__init__()
        self.urls_with_hash: list[URLWithHash] = self.prepare_urls(self.urls)

    def crawl(self):
        from pricera.rozetka.spiders.rozetka_product_spider import RozetkaProductSpider

        return self.process_scrapy_spider(
            spider_cls=RozetkaProductSpider,
            s3_bucket=self.bucket,
            s3_prefix=self.path,
            start_urls=self.urls_with_hash,
            proxy_config=None,
        )

    @classmethod
    def get_crawler(cls, message: dict) -> "RozetkaProductCollector":
        payload = message["payload"]
        return cls(urls=payload[cls.payload_key])

    def parse(self):
        from pricera.rozetka.parsers.rozetka_product_parser import RozetkaProductParser

        file = self.load_file_from_s3(
            bucket=self.bucket,
            prefix=self.path,
            filename=self.storage_key,
        )

        try:
            RozetkaProductParser.parse(file)
        except Exception as e:
            # todo: replace with proper logging
            print(e)

    @classmethod
    def get_parser(cls, message=dict) -> "RozetkaProductCollector":
        payload = message["payload"]
        return cls(urls=payload[cls.payload_key])
