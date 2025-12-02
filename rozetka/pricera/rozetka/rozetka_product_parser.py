from dataclasses import dataclass
from pricera.common.collectors import BaseCollector
import logging
from pricera.rozetka.rozetka_mixins import RozetkaProductMixin

logger = logging.getLogger("rozetka_product_parser")


@dataclass
class RozetkaProductParser(BaseCollector, RozetkaProductMixin):
    url: str

    def __post_init__(self):
        super().__init__()
        self.storage_file_name = self.get_storage_file_name_from_url(self.url)

    def parse(self):
        from pricera.rozetka.parsers.rozetka_product_parser import RozetkaProductParser

        file = self.load_file_from_s3(
            bucket=self.storage_bucket,
            prefix=self.storage_prefix,
            filename=self.storage_file_name,
        )

        try:
            res = RozetkaProductParser.parse(file)
            print(res)
        except Exception as e:
            logger.error("Error during rozetka product parsing", exc_info=e)

    @classmethod
    def get_parser(cls, message=dict) -> "RozetkaProductParser":
        payload = message["payload"]
        return cls(url=payload[cls.payload_key])
