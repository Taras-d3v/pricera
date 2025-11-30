import unittest
from unittest.mock import patch

import sys
from pricera.common.pipelines.launcher import get_launcher_args


class TestGetFileArgs(unittest.TestCase):
    # tests for file argument

    def test_file_argument_present(self):
        test_args = ["prog", "--file", "test.txt"]
        with patch.object(sys, "argv", test_args):
            args = get_launcher_args()
            self.assertEqual(args.file, "test.txt")

    def test_file_argument_absent(self):
        test_args = ["prog"]
        with patch.object(sys, "argv", test_args):
            args = get_launcher_args()
            self.assertIsNone(args.file)

    # tests for pipeline_type argument

    def test_pipeline_type_crawler_argument_present(self):
        test_args = ["prog", "--pipeline_type", "crawl"]
        with patch.object(sys, "argv", test_args):
            args = get_launcher_args()
            self.assertEqual(args.pipeline_type, "crawl")

    def test_pipeline_type_parser_argument_present(self):
        test_args = ["prog", "--pipeline_type", "parse"]
        with patch.object(sys, "argv", test_args):
            args = get_launcher_args()
            self.assertEqual(args.pipeline_type, "parse")

    def test_pipeline_type_argument_absent(self):
        test_args = ["prog"]
        with patch.object(sys, "argv", test_args):
            args = get_launcher_args()
            self.assertIsNone(args.pipeline_type)

    # tests for rabbitmq argument

    def test_rabbitmq_argument_present(self):
        test_args = ["prog", "--rabbitmq"]
        with patch.object(sys, "argv", test_args):
            args = get_launcher_args()
            self.assertTrue(args.rabbitmq)

    def test_rabbitmq_argument_absent(self):
        test_args = ["prog"]
        with patch.object(sys, "argv", test_args):
            args = get_launcher_args()
            self.assertFalse(args.rabbitmq)


if __name__ == "__main__":
    unittest.main()
