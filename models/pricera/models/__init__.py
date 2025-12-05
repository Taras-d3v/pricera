__all__ = ["ResponseObject", "URLWithHash"]

from dataclasses import dataclass
from pydantic import BaseModel, Field
import hashlib


@dataclass
class ResponseObject:
    url: str
    text: str
    status: int
    object_hash: str


class URLWithHash(BaseModel):
    url: str
    hash: str = Field(default=None)

    def __init__(self, **data):
        if "hash" not in data or data["hash"] is None:
            data["hash"] = hashlib.sha256(data["url"].encode()).hexdigest()
        super().__init__(**data)

    @classmethod
    def from_urls(cls, urls: list[str]) -> list["URLWithHash"]:
        return [cls(url=url) for url in urls]

    @classmethod
    def from_url(cls, url: str) -> "URLWithHash":
        return cls(url=url)
