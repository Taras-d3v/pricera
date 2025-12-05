from typing import ClassVar


class RozetkaProductMixin:
    payload_key: ClassVar[str] = "rozetka_product"
    storage_bucket: ClassVar[str] = "pricera-crawled-data"
    storage_prefix: ClassVar[str] = "rozetka_product/"
    collector_name: ClassVar[str] = "rozetka_product"
