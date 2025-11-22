from pricera.common.collectors import BaseCollector


def etl_pipeline(collector_cls: BaseCollector, message: dict, queue: str) -> None:
    payload = message["payload"][queue]
    collector = collector_cls.get_collector(payload=payload)

    crawler = collector.crawl()
    responses: list = list(crawler.responses.values())

    list(map(collector.parse, responses))


def parse_pipeline(parser_cls, message: dict, queue: str) -> None:
    payload = message["payload"][queue]
    parser = parser_cls.get_parser(payload=payload)

    parser.parse()
