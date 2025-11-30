import unittest
import os
from pricera.common import load_file_from_sub_folder
from pricera.rozetka.parsers import RozetkaProductParser


class TestRozetkaProductParser(unittest.TestCase):
    def test_parse(self):
        product_blob = load_file_from_sub_folder(
            test_file_path=os.path.abspath(__file__), filename="20-11-2025-rozetka-product.jsonl.gz"
        )
        expected_result = {
            "brand": "Apple",
            "category_title": "Мобільні телефони",
            "description": 'Екран (6.9", OLED (Super Retina XDR), 2868x1320) / Apple A19 '
            "Pro / основна потрійна камера: 48 Мп + 48 Мп + 48 Мп, "
            "фронтальна камера: 18 Мп / 256 ГБ вбудованої пам'яті / 3G / "
            "LTE / 5G / GPS / підтримка 2х СІМ-карт (Nano-SIM, eSIM) / iOS "
            "26",
            "images": [
                {"url": "https://content2.rozetka.com.ua/goods/images/original/594364394.jpg"},
                {"url": "https://content2.rozetka.com.ua/goods/images/original/594364395.jpg"},
            ],
            "old_price": 72999,
            "price": 69999,
            "review_rating": 5,
            "reviews": 55,
            "root_category_title": "Смартфони, ТВ і електроніка",
            "sell_status": "active",
            "seller": "Rozetka",
            "title": "Мобільний телефон Apple iPhone 17 Pro Max 256GB Cosmic Orange " "(MFYN4AF/A)",
        }
        self.assertEqual(expected_result, RozetkaProductParser.parse(product_blob))

    def test_get_product_id_from_url(self):
        url = "https://rozetka.com.ua/apple_iphone_17_pro_max_256gb_cosmic_orange_mfyn4af_a/p594364393/"
        expected_product_id = "594364393"
        self.assertEqual(expected_product_id, RozetkaProductParser.get_product_id_from_url(url))

        url_no_slash = "https://rozetka.com.ua/apple_iphone_17_pro_max_256gb_cosmic_orange_mfyn4af_a/p594364393"
        self.assertEqual(expected_product_id, RozetkaProductParser.get_product_id_from_url(url_no_slash))

        invalid_url = "https://rozetka.com.ua/apple_iphone_17_pro_max_256gb_cosmic_orange_mfyn4af_a/"
        with self.assertRaises(ValueError):
            RozetkaProductParser.get_product_id_from_url(invalid_url)


if __name__ == "__main__":
    unittest.main()
