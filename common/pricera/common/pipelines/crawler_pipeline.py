from pricera.common.pipelines.collector_mapping import PAYLOAD_KEY_TO_CRAWLER
from logging import getLogger
from pricera.common.pipelines.utilities import prepare_message
from pymongo import MongoClient

logger = getLogger("crawler_pipeline")


def crawler_pipeline(mongo_client: MongoClient, message: dict, trigger_to_cls_mapping=PAYLOAD_KEY_TO_CRAWLER) -> None:
    for item in prepare_message(message=message, collector_mapping=trigger_to_cls_mapping):
        if not item:
            continue
        crawler_cls, prepared_message = item
        crawler_cls_obj = crawler_cls.get_crawler(message=prepared_message, mongo_client=mongo_client)
        crawler_cls_obj.crawl()
