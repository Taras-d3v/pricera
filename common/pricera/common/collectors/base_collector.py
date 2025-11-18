from dataclasses import dataclass

from scrapy import signals
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings


class ScrapyConfigurationMixin:
    def __init__(self):
        self.spider_instance = None

    def process_scrapy_spider(
        self, spider_cls, urls: list[str], s3_bucket: str, s3_prefix: str, proxy_config=None, **kwargs
    ):
        process = CrawlerProcess(get_project_settings())
        crawler = process.create_crawler(spider_cls)

        def handle_spider_opened(spider):
            self.spider_instance = spider

        crawler.signals.connect(handle_spider_opened, signal=signals.spider_opened)

        process.crawl(
            crawler, start_urls=urls, proxy_config=proxy_config, s3_bucket=s3_bucket, s3_prefix=s3_prefix, **kwargs
        )

        process.start()  # блокирует выполнение до конца работы паука

        return self.spider_instance


@dataclass
class BaseCollector(ScrapyConfigurationMixin):
    def crawl(self):
        raise NotImplemented

    def parse(self, *args, **kwargs) -> dict:
        raise NotImplemented

    @classmethod
    def get_collector(cls, *args, **kwargs) -> "BaseCollector":
        raise NotImplemented
