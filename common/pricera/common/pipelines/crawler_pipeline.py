from pricera.common.pipelines.collector_mapping import PAYLOAD_KEY_TO_CRAWLER
from logging import getLogger
from pricera.common.pipelines.utilities import prepare_message

logger = getLogger("crawler_pipeline")


def crawler_pipeline(original_message: dict, trigger_to_cls_mapping=PAYLOAD_KEY_TO_CRAWLER) -> None:
    for item in prepare_message(original_message=original_message, collector_mapping=trigger_to_cls_mapping):
        if not item:
            continue
        crawler_cls, message = item
        crawler_cls_obj = crawler_cls.get_crawler(message=message)
        crawler_cls_obj.crawl()
