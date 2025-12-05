from typing import ClassVar


class RozetkaProductMixin:
    payload_key: ClassVar[str] = "rozetka_product"
    storage_prefix: ClassVar[str] = "rozetka_product/"
    collection_name: ClassVar[str] = "rozetka_product"
    collector_name: ClassVar[str] = "rozetka_product"
