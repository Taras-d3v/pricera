from dataclasses import dataclass
from pricera.common.collectors import BaseCollector
import logging
from pricera.rozetka.rozetka_mixins import RozetkaProductMixin
from pymongo import MongoClient

logger = logging.getLogger("rozetka_product_parser")


@dataclass
class RozetkaProductParser(BaseCollector, RozetkaProductMixin):
    url: str
    mongo_client: MongoClient

    def __post_init__(self):
        super().__init__()
        self.storage_file_name = self.get_storage_file_name_from_url(self.url)
        self.db_collection = self.mongo_client[self.db_name][self.collection_name]
        self.db_filter = {"product_url": self.url}
        self.object_key = f"{self.storage_bucket}/{self.storage_prefix}/{self.storage_file_name}"

    def parse(self):
        from pricera.rozetka.parsers.rozetka_product_parser import RozetkaProductParser

        file = self.load_file_from_s3(
            bucket=self.storage_bucket,
            prefix=self.storage_prefix,
            filename=self.storage_file_name,
        )

        try:
            parsed_data = RozetkaProductParser.parse(file)
            parsed_status = {f"pricera.{self.collector_name}.parse_status": "success"}
            logger.info("Successfully parsed rozetka product")
        except Exception as e:
            parsed_data, parsed_status = {}, {f"pricera.{self.collector_name}.parse_status": "failure"}
            logger.error("Error during rozetka product parsing", exc_info=e)

        self.db_collection.update_one(filter=self.db_filter, update={"$set": parsed_data | parsed_status}, upsert=True)

    @classmethod
    def get_parser(cls, message: dict, mongo_client: MongoClient) -> "RozetkaProductParser":
        payload = message["payload"]
        return cls(url=payload[cls.payload_key], mongo_client=mongo_client)
