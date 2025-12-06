from pricera.models import HashedURL
import unittest
import hashlib


class TestHashedURL(unittest.TestCase):
    def test_url_with_hash(self):
        input_url = "https://example.com/product/12345"

        url_with_hash = HashedURL.from_value(input_url)
        self.assertEqual(url_with_hash, input_url)
        self.assertEqual(url_with_hash.hash, hashlib.sha256(input_url.encode()).hexdigest())

    def test_url_with_hash_list(self):
        input_urls = ["https://example.com/product/12345", "https://example.com/product/67890"]
        urls_with_hash = HashedURL.from_values(input_urls)
        for index, url in enumerate(input_urls):
            self.assertEqual(url, urls_with_hash[index])
            self.assertEqual(HashedURL.get_hash(url), urls_with_hash[index].hash)

    def test_custom_hash_function(self):
        class CustomHashedURL(HashedURL):
            @staticmethod
            def get_hash(value: str) -> str:
                return "customhashvalue"

        input_url = "https://example.com/product/12345"
        url_with_hash = CustomHashedURL.from_value(input_url)
        self.assertEqual(url_with_hash, input_url)
        self.assertEqual(url_with_hash.hash, "customhashvalue")


if __name__ == "__main__":
    unittest.main()
