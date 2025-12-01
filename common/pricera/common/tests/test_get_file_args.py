import unittest
from unittest.mock import patch
import sys
from pricera.common.pipelines.launcher import get_launcher_args


class TestGetFileArgs(unittest.TestCase):
    # tests for file argument
    def test_file_argument_present(self):
        test_args = ["prog", "--file", "test.txt", "--pipeline_type", "crawl"]
        with patch.object(sys, "argv", test_args):
            args = get_launcher_args()
            self.assertEqual(args.file, "test.txt")
            self.assertEqual(args.pipeline_type, "crawl")

    def test_file_argument_absent_with_rabbitmq(self):
        test_args = ["prog", "--rabbitmq", "--pipeline_type", "crawl"]
        with patch.object(sys, "argv", test_args):
            args = get_launcher_args()
            self.assertIsNone(args.file)
            self.assertTrue(args.rabbitmq)

    # tests for pipeline_type argument
    def test_pipeline_type_crawler_argument_present(self):
        test_args = ["prog", "--pipeline_type", "crawl", "--rabbitmq"]
        with patch.object(sys, "argv", test_args):
            args = get_launcher_args()
            self.assertEqual(args.pipeline_type, "crawl")
            self.assertTrue(args.rabbitmq)

    def test_pipeline_type_parser_argument_present(self):
        test_args = ["prog", "--pipeline_type", "parse", "--file", "test.txt"]
        with patch.object(sys, "argv", test_args):
            args = get_launcher_args()
            self.assertEqual(args.pipeline_type, "parse")
            self.assertEqual(args.file, "test.txt")

    def test_pipeline_type_argument_absent_should_fail(self):
        # argparse itself will raise SystemExit for missing required argument
        test_args = ["prog", "--file", "test.txt"]
        with patch.object(sys, "argv", test_args):
            with self.assertRaises(SystemExit):
                get_launcher_args()

    # tests for rabbitmq argument
    def test_rabbitmq_argument_present(self):
        test_args = ["prog", "--rabbitmq", "--pipeline_type", "crawl"]
        with patch.object(sys, "argv", test_args):
            args = get_launcher_args()
            self.assertTrue(args.rabbitmq)
            self.assertEqual(args.pipeline_type, "crawl")

    def test_rabbitmq_argument_absent_with_file(self):
        test_args = ["prog", "--file", "test.txt", "--pipeline_type", "crawl"]
        with patch.object(sys, "argv", test_args):
            args = get_launcher_args()
            self.assertFalse(args.rabbitmq)
            self.assertEqual(args.file, "test.txt")

    # Test validation logic - patch exit to catch it
    def test_both_file_and_rabbitmq_should_fail(self):
        test_args = ["prog", "--file", "test.txt", "--rabbitmq", "--pipeline_type", "crawl"]
        with patch.object(sys, "argv", test_args):
            # This will get through argparse but fail in validate_args
            with self.assertRaises(SystemExit):
                get_launcher_args()

    def test_neither_file_nor_rabbitmq_should_fail(self):
        test_args = ["prog", "--pipeline_type", "crawl"]
        with patch.object(sys, "argv", test_args):
            with self.assertRaises(SystemExit):
                get_launcher_args()


if __name__ == "__main__":
    unittest.main()
