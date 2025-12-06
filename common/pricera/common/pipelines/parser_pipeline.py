from pricera.common.pipelines.collector_mapping import PAYLOAD_KEY_TO_PARSER
from logging import getLogger
from pricera.common.pipelines.utilities import prepare_message
from pymongo import MongoClient

logger = getLogger("parser_pipeline")


def parser_pipeline(mongo_client: MongoClient, message: dict, trigger_to_cls_mapping=PAYLOAD_KEY_TO_PARSER) -> None:
    for item in prepare_message(message=message, collector_mapping=trigger_to_cls_mapping):
        if not item:
            continue
        parser_cls, prepared_message = item
        parser_cls_obj = parser_cls.get_parser(message=prepared_message, mongo_client=mongo_client)
        parser_cls_obj.parse()
