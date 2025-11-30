from pricera.common.pipelines.collector_mapping import PAYLOAD_KEY_TO_CRAWLER


class CrawlerFactory:
    @staticmethod
    def get_message_key_from_message(message: dict) -> str:
        message_payload = message["payload"]
        # todo: handle multiple keys in payload
        message_key = list(message_payload.keys())[0]
        return message_key

    @classmethod
    def get_crawler(cls, message: dict):
        message_key = cls.get_message_key_from_message(message)
        crawler_cls = PAYLOAD_KEY_TO_CRAWLER[message_key]
        return crawler_cls.get_crawler(message=message)


def crawler_pipeline(message: dict, factory=CrawlerFactory) -> None:
    collector = factory.get_crawler(message=message)
    collector.crawl()
