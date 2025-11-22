import os
from typing import Optional
from pydantic import BaseModel
import json


class ParsedLine(BaseModel):
    url: str
    text: str
    status: int
    object_key: str

    @property
    def raw_data(self):
        return json.loads(self.text)


def get_env_value(env_name: str) -> Optional[str]:
    return os.getenv(env_name)


def get_rabbitmq_host() -> Optional[str]:
    return get_env_value("RABBIT_MQ_HOST")


def get_rabbitmq_user() -> Optional[str]:
    return get_env_value("RABBIT_MQ_USER")


def get_rabbitmq_password() -> Optional[str]:
    return get_env_value("RABBIT_MQ_PASSWORD")


def parse_line(line: str):
    raw_data = json.loads(line)
    return ParsedLine(
        url=raw_data["url"], text=raw_data["text"], status=raw_data["status"], object_key=raw_data["object_key"]
    )
