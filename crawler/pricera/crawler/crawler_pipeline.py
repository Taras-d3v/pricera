from typing import Callable
from pricera.common import FileBasedMessageConsumer, MessageProcessor, RabbitMQ, get_file_args


def crawler_pipeline(crawler_factory, message: dict) -> None:
    collector = crawler_factory.get_crawler(message=message)
    collector.crawl()


def launch_crawler(factory, pipeline: Callable = crawler_pipeline, queue: str = "crawler_queue") -> None:
    message_processor = MessageProcessor(pipeline=pipeline, factory=factory)
    args = get_file_args()
    if args.file:
        file_consumer: FileBasedMessageConsumer = FileBasedMessageConsumer(
            function=message_processor.process, file_path=args.file
        )
        file_consumer.consume()
    else:
        rabbitmq_consumer: RabbitMQ = RabbitMQ()
        rabbitmq_consumer.consume(function=message_processor.process, queue=queue)
