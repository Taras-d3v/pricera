__all__ = [
    "RabbitMQ",
    "FileBasedMessageConsumer",
    "BaseCollector",
    "load_file_from_sub_folder",
]

import argparse
from dataclasses import dataclass
from typing import Callable, Any

from .collectors import BaseCollector, FileBasedMessageConsumer, RabbitMQ
from .etl_pipeline import etl_pipeline
from .testing_utilities import load_file_from_sub_folder


def get_file_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", type=str, help="Path to the message file", required=False)
    args = parser.parse_args()
    return args


@dataclass
class MessageProcessor:
    pipeline: Callable  # crawler_pipeline
    factory: Any

    def process(self, message: dict) -> None:
        self.pipeline(message=message, factory=self.factory)


def launch_collector(
    queue: str,
    pipeline: Callable = etl_pipeline,
):
    message_processor = MessageProcessor(pipeline=pipeline, factory=None)
    args = get_file_args()
    if args.file:
        file_consumer: FileBasedMessageConsumer = FileBasedMessageConsumer(
            function=message_processor.process, file_path=args.file
        )
        file_consumer.consume()
    else:
        rabbitmq_consumer: RabbitMQ = RabbitMQ()
        rabbitmq_consumer.consume(function=message_processor.process, queue=queue)
