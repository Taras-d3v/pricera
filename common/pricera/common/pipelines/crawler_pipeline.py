from pricera.common.pipelines.collector_mapping import PAYLOAD_KEY_TO_CRAWLER
from pymongo import MongoClient


class CrawlerFactory:
    @staticmethod
    def get_message_key_from_message(message: dict) -> str:
        message_payload = message["payload"]
        # todo: handle multiple keys in payload
        message_key = list(message_payload.keys())[0]
        return message_key

    @classmethod
    def get_crawler(cls, message: dict, **kwargs):
        message_key = cls.get_message_key_from_message(message)
        crawler_cls = PAYLOAD_KEY_TO_CRAWLER[message_key]
        return crawler_cls.get_crawler(message=message, **kwargs)


def crawler_pipeline(mongo_client: MongoClient, message: dict, factory=CrawlerFactory) -> None:
    collector = factory.get_crawler(message=message, mongo_client=mongo_client)
    collector.crawl()
