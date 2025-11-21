import hashlib
from pricera.common.parser.pricera_base_parser import BaseParser
from dataclasses import dataclass


@dataclass
class RozetkaProductParser(BaseParser):
    url: str
    payload_key: str = "rozetka_product"
    bucket: str = "pricera-crawled-data"
    path: str = "rozetka_product/"

    def __post_init__(self):
        self.storage_key = f"{hashlib.md5(self.url.encode()).hexdigest()}.jsonl.gz"

    def parse(self):
        from pricera.parser.rozetka.product_parser import RozetkaProductParser

        file = self.load_file_from_s3(
            bucket=self.bucket,
            prefix=self.path,
            filename=self.storage_key,
        )

        try:
            RozetkaProductParser.parse(file)
        except Exception as e:
            # todo: replace with proper logging
            print(e)

    @classmethod
    def get_parser(cls, message=dict) -> "RozetkaProductParser":
        payload = message["payload"]
        return cls(url=payload[cls.payload_key])
