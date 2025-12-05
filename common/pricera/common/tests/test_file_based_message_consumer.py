import os
import unittest

from pricera.common.collectors.consumers import FileBasedMessageConsumer
from pricera.common.collectors.exceptions import MessageFileNotFoundError


class TestFileBasedMessageConsumer(unittest.TestCase):
    def setUp(self):
        self.test_folder = os.path.dirname(__file__)
        self.messages_folder = os.path.join(self.test_folder, "messages")

    def test_initialization_with_nonexistent_file_raises_error(self):
        """Test that consumer raises MessageFileNotFoundError for non-existent files."""
        nonexistent_file = os.path.join(self.messages_folder, "nonexistent_file.json")

        with self.assertRaises(MessageFileNotFoundError):
            FileBasedMessageConsumer(file_path=nonexistent_file, function=lambda x: x)

    def test_read_json_file_returns_expected_messages(self):
        """Test that _read_json_file correctly parses and returns message data."""
        test_file_path = os.path.join(self.messages_folder, "test_message_1.json")
        consumer = FileBasedMessageConsumer(file_path=test_file_path, function=lambda x: x)

        # todo: add multi-line support
        messages = list(consumer._read_json_file())

        expected_messages = [{"payload": {"foo": ["bar_1", "bar_2"]}}]

        self.assertEqual(expected_messages, messages)
