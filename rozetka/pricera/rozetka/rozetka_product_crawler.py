from dataclasses import dataclass
from pricera.common.collectors import BaseCollector
from pricera.models import URLWithHash
from pricera.rozetka.rozetka_mixins import RozetkaProductMixin
import logging
from pymongo import MongoClient, UpdateOne

logger = logging.getLogger("rozetka_product_crawler")


@dataclass
class RozetkaProductCrawler(BaseCollector, RozetkaProductMixin):
    urls: list[str]
    mongo_client: MongoClient
    is_synchronous: bool = False

    def __post_init__(self):
        super().__init__()
        self.urls_with_hash: list[URLWithHash] = self.prepare_urls(self.urls)
        self.db_collection = self.mongo_client[self.db_name][self.collection_name]

    def crawl(self):
        from pricera.rozetka.spiders.rozetka_product_spider import RozetkaProductSpider

        return self.process_scrapy_spider(
            spider_cls=RozetkaProductSpider,
            storage_bucket=self.storage_bucket,
            storage_prefix=self.storage_prefix,
            start_urls=self.urls_with_hash,
            proxy_config=None,
        )

    def update_crawl_status(self, spider):
        # Expecting spider to accumulate crawl statuses in stats under key 'custom_status'
        # statuses structure: {url_hash: {"status": str, "reason": str|None, ...}}
        statuses = spider.crawler.stats.get_value("custom_status")
        if not statuses:
            return

        hash_to_url = {url_with_hash.hash: url_with_hash.url for url_with_hash in self.urls_with_hash}

        bulk_requests: list[UpdateOne] = []
        for url_hash, status in statuses.items():
            url = hash_to_url[url_hash]
            # Minimal payload normalization
            crawl_status = {f"pricera.{self.collector_name}.crawl_status": status}

            # Compose the update operation. We assume there's a collection named according to storage_prefix
            # or a default collection accessible via mixin/collector. If storage_prefix is not a collection name,
            # replace with the appropriate collection.
            bulk_requests.append(
                UpdateOne(
                    filter={"product_url": url},
                    update={"$set": crawl_status},
                    upsert=True,
                )
            )

        if not bulk_requests:
            return

        try:
            self.db_collection.bulk_write(bulk_requests, ordered=False)
            logger.info("Finished updating crawl statuses")
        except Exception as e:
            logger.error("Failed during the bulk updating crawl statuses", exc_info=e)

    @classmethod
    def get_crawler(cls, message: dict, mongo_client: MongoClient, **kwargs) -> "RozetkaProductCrawler":
        payload = message["payload"]
        return cls(urls=payload[cls.payload_key], mongo_client=mongo_client)
