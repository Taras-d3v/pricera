from pricera.crawler.rozetka import RozetkaProductCrawler

# explicitly defined payload key to crawler mapping:
PAYLOAD_KEY_TO_CRAWLER = {
    RozetkaProductCrawler.payload_key: RozetkaProductCrawler,
}


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


if __name__ == "__main__":
    from pricera.crawler.crawler_pipeline import launch_crawler

    launch_crawler(factory=CrawlerFactory)
