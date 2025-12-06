__all__ = ["ResponseObject", "HashedURL"]

from pydantic import BaseModel, Field, ConfigDict
import hashlib


class ResponseObject(BaseModel):
    url: str
    text: str
    status: int
    object_key: str

    model_config = ConfigDict(extra="allow")


class HashedURL(str):
    hash: str

    def __new__(cls, value: str):
        obj = super().__new__(cls, value)
        obj.hash = cls.get_hash(value)
        return obj

    @staticmethod
    def get_hash(value: str) -> str:
        return hashlib.sha256(value.encode()).hexdigest()

    @classmethod
    def from_value(cls, value: str) -> "HashedURL":
        return cls(value)

    @classmethod
    def from_values(cls, values: list[str]) -> list["HashedURL"]:
        return [cls(url) for url in values]
