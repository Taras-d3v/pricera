from pricera.common.pipelines.collector_mapping import PAYLOAD_KEY_TO_PARSER
from logging import getLogger
from pricera.common import ensure_list
from copy import deepcopy

logger = getLogger("parser_pipeline")

class ParserFactory:
    @staticmethod
    def get_message_key_from_message(message: dict) -> str:
        message_payload = message["payload"]
        # todo: handle multiple keys in payload
        message_key = list(message_payload.keys())[0]
        return message_key

    @staticmethod
    def get_parser_cls_from_payload_key(payload_key: str):
        parser_cls = PAYLOAD_KEY_TO_PARSER.get(payload_key)
        if not parser_cls:
            logger.error(f"Unable to map parser cls by payload key", extra={"payload_key": payload_key})
            return None
        logger.info("Successfully mapped parser cls by payload key")
        return parser_cls

    @classmethod
    def get_parser(cls, message: dict):
        message_key = cls.get_message_key_from_message(message)
        parser_cls = PAYLOAD_KEY_TO_PARSER[message_key]
        return parser_cls.get_parser(message=message)


def parser_pipeline(message: dict, factory=ParserFactory) -> None:
    payload = message.get("payload")
    if not payload:
        logger.warning("Message payload is empty. Skipping parsing.")
        return

    # todo: handle multiple keys in payload
    payload_key, payload_value = list(payload.items())[0]
    parser_cls = factory.get_parser_cls_from_payload_key(payload_key)
    payload_value = ensure_list(payload_value)
    for item in payload_value:
        single_message = deepcopy(message)
        single_message["payload"] = {payload_key: item}
        parser_cls_obj = parser_cls.get_parser(message=single_message)
        parser_cls_obj.parse()
