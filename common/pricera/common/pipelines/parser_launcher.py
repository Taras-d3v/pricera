from pricera.rozetka.rozetka_product_collector import RozetkaProductCollector

# explicitly defined payload key to crawler mapping:
PAYLOAD_KEY_TO_PARSER = {
    RozetkaProductCollector.payload_key: RozetkaProductCollector,
}


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


if __name__ == "__main__":
    from pricera.common.pipelines.parser_pipeline import launch_parser

    launch_parser(factory=ParserFactory)
