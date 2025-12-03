import argparse
import logging
from dataclasses import dataclass
from typing import Callable

from pymongo import MongoClient

from pricera.common.logger import set_logger
from pricera.common import FileBasedMessageConsumer, RabbitMQ, get_mongo_client
from pricera.common.pipelines import crawler_pipeline, parser_pipeline

logger = logging.getLogger("launcher")


PIPELINE_TO_QUEUE = {
    "crawl": "crawler_queue",
    "parse": "parser_queue",
}

PIPELINE_TO_FUNCTION = {
    "crawl": crawler_pipeline,
    "parse": parser_pipeline,
}


def get_launcher_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument("--file", type=str, help="Path to the message file")
    parser.add_argument("--rabbitmq", action="store_true", help="Enable RabbitMQ mode")
    parser.add_argument(
        "--pipeline_type",
        type=str,
        choices=list(PIPELINE_TO_FUNCTION.keys()),
        required=True,
        help="Type of pipeline to run",
    )
    args = parser.parse_args()
    validate_args(args)
    return args


def validate_args(args: argparse.Namespace) -> None:
    if args.file and args.rabbitmq:
        logger.error("You cannot use both file mode and RabbitMQ mode at the same time.")
        exit(1)

    if not args.file and not args.rabbitmq:
        logger.error("You must specify either --file or --rabbitmq.")
        exit(1)


@dataclass
class MessageProcessor:
    pipeline: Callable
    mongo_client: MongoClient

    def process(self, message: dict) -> None:
        self.pipeline(message=message, mongo_client=self.mongo_client)


def main() -> None:
    args = get_launcher_args()

    pipeline = PIPELINE_TO_FUNCTION[args.pipeline_type]
    with get_mongo_client() as mongo_client:
        processor = MessageProcessor(pipeline=pipeline, mongo_client=mongo_client)

        if args.file:
            logger.info(f"Starting file consumer for file: {args.file}")
            consumer = FileBasedMessageConsumer(
                function=processor.process,
                file_path=args.file,
            )
            consumer.consume()

        else:
            queue = PIPELINE_TO_QUEUE[args.pipeline_type]
            logger.info(f"Starting RabbitMQ consumer on queue: {queue}")

            consumer = RabbitMQ()
            consumer.consume(function=processor.process, queue=queue)


if __name__ == "__main__":
    set_logger()
    main()
