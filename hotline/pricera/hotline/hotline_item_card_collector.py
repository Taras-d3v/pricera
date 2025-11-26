from dataclasses import dataclass
from pricera.common.collectors import BaseCollector
from pricera.models import URLWithHash


@dataclass
class HotlineItemCardCollector(BaseCollector):
    urls: list[str]
    payload_key: str = "hotline_item_card"
    bucket: str = "pricera-crawled-data"
    path: str = "hotline_item_card/"

    def __post_init__(self):
        super().__init__()
        self.urls_with_hash: list[URLWithHash] = self.prepare_urls(self.urls)

    def crawl(self):
        from pricera.hotline.spiders.hotline_item_card_spider import HotlineItemCardSpider

        return self.process_scrapy_spider(
            spider_cls=HotlineItemCardSpider,
            s3_bucket=self.bucket,
            s3_prefix=self.path,
            start_urls=self.urls_with_hash,
            proxy_config=None,
        )

    @classmethod
    def get_crawler(cls, message: dict) -> "HotlineItemCardCollector":
        payload = message["payload"]
        return cls(urls=payload[cls.payload_key])

    def parse(self):
        from pricera.hotline.parsers.hotline_item_card_parser import HotlineItemCardParser

        file = self.load_file_from_s3(
            bucket=self.bucket,
            prefix=self.path,
            filename=self.storage_key,
        )

        try:
            HotlineItemCardParser.parse(file)
        except Exception as e:
            # todo: replace with proper logging
            print(e)

    @classmethod
    def get_parser(cls, message=dict) -> "HotlineItemCardCollector":
        payload = message["payload"]
        return cls(urls=payload[cls.payload_key])
