from dataclasses import dataclass
from pricera.common.collectors import BaseCollector
import logging
from pricera.rozetka.rozetka_mixins import RozetkaProductMixin
from pymongo import MongoClient
from typing import ClassVar

logger = logging.getLogger("rozetka_product_parser")


@dataclass
class RozetkaProductParser(BaseCollector, RozetkaProductMixin):
    url: str
    mongo_client: MongoClient
    collection_name: ClassVar[str] = "rozetka_product"

    def __post_init__(self):
        super().__init__()
        self.storage_file_name = self.get_storage_file_name_from_url(self.url)
        self.db_collection = self.mongo_client[self.db_name][self.collection_name]
        self.db_filter = {"product_url": self.url}

    def parse(self):
        from pricera.rozetka.parsers.rozetka_product_parser import RozetkaProductParser

        file = self.load_file_from_s3(
            bucket=self.storage_bucket,
            prefix=self.storage_prefix,
            filename=self.storage_file_name,
        )

        try:
            res = RozetkaProductParser.parse(file)
            self.db_collection.update_one(filter=self.db_filter, update={"$set": res}, upsert=True)
            logger.info("Successfully parsed rozetka product")
        except Exception as e:
            self.db_collection.update_one(
                filter=self.db_filter,
                update={"$set": {"parse_status": "error"}},
                upsert=True,
            )
            logger.error("Error during rozetka product parsing", exc_info=e)

    @classmethod
    def get_parser(cls, message: dict, mongo_client: MongoClient) -> "RozetkaProductParser":
        payload = message["payload"]
        return cls(url=payload[cls.payload_key], mongo_client=mongo_client)
