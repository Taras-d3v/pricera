__all__ = ["FileBasedMessageConsumer", "RabbitMQ"]

import json
import os
import ssl
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable, Dict, Generator

from pika import BlockingConnection, ConnectionParameters, PlainCredentials, SSLOptions
from pricera.common.utilities import get_rabbitmq_host, get_rabbitmq_password, get_rabbitmq_user
from pricera.common.collectors.exceptions import MessageFileFormatError, MessageFileNotFoundError


class Consumer(ABC):
    """Abstract base class for message consumers."""

    @abstractmethod
    def consume(self, *args, **kwargs) -> None:
        """Process messages from a source."""


@dataclass
class FileBasedMessageConsumer:
    """Message consumer that reads from JSON files."""

    file_path: str
    function: Callable

    def __post_init__(self) -> None:
        """Validate file existence after initialization."""
        self._validate_file_exists()

    def _validate_file_exists(self) -> None:
        """Verify that the specified file exists and is accessible."""
        if not os.path.exists(self.file_path):
            raise MessageFileNotFoundError(f"Message file not found: {self.file_path}")
        if not os.path.isfile(self.file_path):
            raise MessageFileFormatError(f"Path is not a file: {self.file_path}")

    def _read_json_file(self) -> Generator[Dict[str, Any], None, None]:
        """
        Read and parse JSON data from file.

        Yields:
            Parsed JSON data as dictionary

        Raises:
            MessageFileFormatError: If file contains invalid JSON or cannot be read
        """
        try:
            with open(self.file_path, "r", encoding="utf-8") as file:
                content = file.read()
                data = json.loads(content)
                yield data
        except json.JSONDecodeError as error:
            raise MessageFileFormatError(f"Invalid JSON in {self.file_path}: {error}")
        except OSError as error:
            raise MessageFileFormatError(f"Error reading file {self.file_path}: {error}")

    def consume(self) -> None:
        """
        Process messages from the JSON file.

        Applies the configured function to each message read from the file.
        """
        for message in self._read_json_file():
            self.function(message)


@dataclass
class RabbitMQ:
    host: str = get_rabbitmq_host()
    user: str = get_rabbitmq_user()
    password: str = get_rabbitmq_password()
    port: int = 5671

    def __post_init__(self):
        self.connection = None
        self.channel = None

    def connect(self):
        """Establish connection to RabbitMQ"""
        ssl_options = SSLOptions(ssl.create_default_context(), self.host)
        connection_params = ConnectionParameters(
            host=self.host,
            port=self.port,
            credentials=PlainCredentials(self.user, self.password),
            ssl_options=ssl_options,
            heartbeat=600,
            blocked_connection_timeout=300,
            connection_attempts=3,
            retry_delay=5,
        )
        self.connection = BlockingConnection(connection_params)
        self.channel = self.connection.channel()
        # todo: investigate if we should use this
        # self.channel.basic_qos(prefetch_count=1)

    def publish(self, queue: str, message: bytes, durable: bool = True):
        """Publish message to specified queue"""
        if not self.connection or self.connection.is_closed:
            self.connect()

        self.channel.queue_declare(queue=queue, durable=durable)
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_publish(exchange="", routing_key=queue, body=message)
        print(f"Message sent to {queue}")

    def consume(self, queue: str, function: Callable):
        # """Start consuming messages from specified queue with manual acknowledgment"""
        # if not self.connection or self.connection.is_closed:
        #     self.connect()

        def wrapped_function(ch, method, properties, body):
            # Call the user's callback function
            try:
                function(body)
                # Manually acknowledge the message after successful processing
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception:
                print("error during the message processing, acknowledge")

        self.channel.basic_consume(
            queue=queue,
            on_message_callback=wrapped_function,
            auto_ack=False,  # Manual acknowledgment
        )
        print(f"Waiting for messages on {queue}...")
        self.channel.start_consuming()

    def close(self):
        """Close connection"""
        if self.connection and not self.connection.is_closed:
            self.connection.close()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
