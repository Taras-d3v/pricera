__all__ = ["ResponseObject", "HashedURL"]

from dataclasses import dataclass
from pydantic import BaseModel, Field
import hashlib


@dataclass
class ResponseObject:
    url: str
    text: str
    status: int
    object_key: str


class HashedURL(str):
    hash: str

    def __new__(cls, value: str):
        obj = super().__new__(cls, value)
        obj.hash = cls.get_hash(value)
        return obj

    @staticmethod
    def get_hash(url: str) -> str:
        return hashlib.sha256(url.encode()).hexdigest()

    @classmethod
    def from_url(cls, url: str) -> "HashedURL":
        return cls(url)

    @classmethod
    def from_urls(cls, urls: list[str]) -> list["HashedURL"]:
        return [cls(url) for url in urls]
