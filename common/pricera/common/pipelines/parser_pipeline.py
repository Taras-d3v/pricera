from typing import Callable
from pricera.common import FileBasedMessageConsumer, MessageProcessor, RabbitMQ, get_file_args


def parser_pipeline(factory, message: dict) -> None:
    parser = factory.get_parser(message=message)
    parser.parse()


def launch_parser(factory, pipeline: Callable = parser_pipeline, queue: str = "parser_queue") -> None:
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
