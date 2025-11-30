from pricera.common.pipelines.collector_mapping import PAYLOAD_KEY_TO_PARSER


class ParserFactory:
    @staticmethod
    def get_message_key_from_message(message: dict) -> str:
        message_payload = message["payload"]
        # todo: handle multiple keys in payload
        message_key = list(message_payload.keys())[0]
        return message_key

    @classmethod
    def get_parser(cls, message: dict):
        message_key = cls.get_message_key_from_message(message)
        parser_cls = PAYLOAD_KEY_TO_PARSER[message_key]
        return parser_cls.get_parser(message=message)


def parser_pipeline(message: dict, factory=ParserFactory) -> None:
    parser = factory.get_parser(message=message)
    parser.parse()
