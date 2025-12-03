from pricera.common.pipelines.collector_mapping import PAYLOAD_KEY_TO_PARSER
from logging import getLogger
from pricera.common.pipelines.utilities import prepare_message

logger = getLogger("parser_pipeline")


def parser_pipeline(message: dict, trigger_to_cls_mapping=PAYLOAD_KEY_TO_PARSER) -> None:
    for item in prepare_message(original_message=message, collector_mapping=trigger_to_cls_mapping):
        if not item:
            continue
        parser_cls, message = item
        parser_cls_obj = parser_cls.get_parser(message=message)
        parser_cls_obj.parse()
