from pricera.common import FileBasedMessageConsumer, RabbitMQ
from pricera.common.pipelines import crawler_pipeline, parser_pipeline
import logging
import argparse
from dataclasses import dataclass
from typing import Callable

logger = logging.getLogger("launcher")


pipeline_type_to_queue_mapping = {
    "crawl": "crawler_queue",
    "parse": "parser_queue",
}

pipeline_type_to_pipeline_mapping = {
    "crawl": crawler_pipeline,
    "parse": parser_pipeline,
}


def get_launcher_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", type=str, help="Path to the message file", required=False)
    parser.add_argument("--rabbitmq", action="store_true", help="Enable RabbitMQ mode")
    parser.add_argument(
        "--pipeline_type", type=str, choices=["crawl", "parse"], help="Type of pipeline to run", required=False
    )

    args = parser.parse_args()
    return args


@dataclass
class MessageProcessor:
    pipeline: Callable  # crawler_pipeline

    def process(self, message: dict) -> None:
        self.pipeline(message=message)


if __name__ == "__main__":
    from pricera.common.logger import set_logger

    set_logger()

    args = get_launcher_args()

    if not args.pipeline_type:
        logger.error("No pipeline type specified in arguments. Exiting.")
        exit(1)

    if not args.file or args.rabbitmq:
        logger.error("No file path/mq broker was set in arguments. Exiting.")
        exit(1)

    pipeline = pipeline_type_to_pipeline_mapping[args.pipeline_type]
    message_processor = MessageProcessor(pipeline=pipeline)

    if args.file:
        file_consumer: FileBasedMessageConsumer = FileBasedMessageConsumer(
            function=message_processor.process, file_path=args.file
        )
        file_consumer.consume()

    else:
        queue = pipeline_type_to_queue_mapping[args.pipeline_type]
        rabbitmq_consumer: RabbitMQ = RabbitMQ()
        rabbitmq_consumer.consume(function=message_processor.process, queue=queue)
