import unittest
import os
from pricera.common import load_file_from_sub_folder
from pricera.hotline.parsers import HotlineItemCardParser


class TestHotlineItemCardParser(unittest.TestCase):
    def test_parse(self):
        product_blob = load_file_from_sub_folder(
            test_file_path=os.path.abspath(__file__), filename="26-11-2025-hotline-item-card.jsonl.gz"
        )
        expected_result = {
            "offers": [
                {
                    "item_name": "Apple iPhone 17 Pro Max 256GB Cosmic Orange (MFYN4)",
                    "item_url": "https://www.hotline.ua/go/price/13912134630/",
                    "shop_name": "itmag.ua",
                },
                {
                    "item_name": "Apple iPhone 17 Pro Max 256GB Cosmic Orange (MFYN4)",
                    "item_url": "https://www.hotline.ua/go/price/13912167099/",
                    "shop_name": "noteboochek.com.ua",
                },
                {
                    "item_name": "Apple Смартфон Apple iPhone 17 Pro Max 256GB Cosmic Orange (MFYN4)",
                    "item_url": "https://www.hotline.ua/go/price/13913196433/",
                    "shop_name": "pixophone.com",
                },
                {
                    "item_name": "Apple Смартфон Apple iPhone 17 Pro Max 256GB Cosmic Orange (MFYN4)",
                    "item_url": "https://www.hotline.ua/go/price/13916755086/",
                    "shop_name": "vr-store.com.ua",
                },
                {
                    "item_name": "Apple iPhone 17 Pro Max 256 Gb Cosmic Orange (MFYN4)",
                    "item_url": "https://www.hotline.ua/go/price/13918282091/",
                    "shop_name": "fopi.ua",
                },
                {
                    "item_name": "Apple Смартфон Apple iPhone 17 Pro Max 256GB Cosmic Orange (MFYN4)",
                    "item_url": "https://www.hotline.ua/go/price/13918438831/",
                    "shop_name": "pcshop.ua",
                },
                {
                    "item_name": "Apple iPhone 17 Pro Max 256GB Cosmic Orange (MFYN4)",
                    "item_url": "https://www.hotline.ua/go/price/13919069242/",
                    "shop_name": "buy.ua",
                },
                {
                    "item_name": "Apple iPhone 17 Pro Max 256GB Cosmic Orange (MFYN4)",
                    "item_url": "https://www.hotline.ua/go/price/13919089527/",
                    "shop_name": "platforma-ukraine.com.ua",
                },
                {
                    "item_name": "Apple Смартфон Apple iPhone 17 Pro Max 256GB Cosmic Orange (MFYN4)",
                    "item_url": "https://www.hotline.ua/go/price/13919232932/",
                    "shop_name": "smartmag.biz.ua",
                },
                {
                    "item_name": "Apple iPhone 17 Pro Max 256GB Cosmic Orange (MFYN4)",
                    "item_url": "https://www.hotline.ua/go/price/13920057826/",
                    "shop_name": "mrfix.ua",
                },
                {
                    "item_name": "Apple iPhone 17 Pro Max 256GB Cosmic Orange (MFYN4)",
                    "item_url": "https://www.hotline.ua/go/price/13920227016/",
                    "shop_name": "ifrukt.com",
                },
                {
                    "item_name": "Apple Смартфон Apple iPhone 17 Pro Max 256GB Cosmic Orange (MFYN4)",
                    "item_url": "https://www.hotline.ua/go/price/13920293046/",
                    "shop_name": "gadget-avenue.com.ua",
                },
                {
                    "item_name": "Apple Смартфон Apple iPhone 17 Pro Max 256GB Cosmic Orange (MFYN4)",
                    "item_url": "https://www.hotline.ua/go/price/13920667911/",
                    "shop_name": "expertonline.com.ua",
                },
                {
                    "item_name": "Apple iPhone 17 Pro Max 256GB Sim Cosmic Orange (MFYN4)",
                    "item_url": "https://www.hotline.ua/go/price/13921204503/",
                    "shop_name": "domowik.com.ua",
                },
                {
                    "item_name": "Apple Смартфон Apple iPhone 17 Pro Max 256 GB Cosmic Orange (MFYN4AF/A)",
                    "item_url": "https://www.hotline.ua/go/price/13921222817/",
                    "shop_name": "zhuk.ua",
                },
                {
                    "item_name": "Apple iPhone 17 Pro Max 256GB Cosmic Orange (MFYN4)",
                    "item_url": "https://www.hotline.ua/go/price/13921839199/",
                    "shop_name": "icenter.in.ua",
                },
                {
                    "item_name": "Apple iPhone 17 Pro Max 256GB Cosmic Orange MFYN4",
                    "item_url": "https://www.hotline.ua/go/price/13922987462/",
                    "shop_name": "zelen-store.com.ua",
                },
                {
                    "item_name": "Apple iPhone 17 Pro Max 256GB Cosmic Orange (MFYN4)",
                    "item_url": "https://www.hotline.ua/go/price/13923767923/",
                    "shop_name": "iPodrom.ua",
                },
                {
                    "item_name": "Apple iPhone 17 Pro Max 256Gb Cosmic Orange (MFYN4)",
                    "item_url": "https://www.hotline.ua/go/price/13923927319/",
                    "shop_name": "iStore.ua",
                },
                {
                    "item_name": "Apple iPhone 17 Pro Max 256GB Cosmic Orange (MFYN4)",
                    "item_url": "https://www.hotline.ua/go/price/13926328256/",
                    "shop_name": "egadget.com.ua",
                },
                {
                    "item_name": "Apple iPhone 17 Pro Max 256GB Cosmic Orange (MFYN4)",
                    "item_url": "https://www.hotline.ua/go/price/13927194072/",
                    "shop_name": "luckylink.kiev.ua",
                },
                {
                    "item_name": "Apple iPhone 17 Pro Max 256GB Cosmic Orange (MFYN4)",
                    "item_url": "https://www.hotline.ua/go/price/13927547088/",
                    "shop_name": "game-shop.com.ua",
                },
                {
                    "item_name": "Apple iPhone 17 Pro Max 256Gb Cosmic Orange (MFYN4)",
                    "item_url": "https://www.hotline.ua/go/price/13927639298/",
                    "shop_name": "gstore.ua",
                },
                {
                    "item_name": "Apple Смартфон Apple iPhone 17 Pro Max 256GB Cosmic Orange (MFYN4AF/A)",
                    "item_url": "https://www.hotline.ua/go/price/13927641555/",
                    "shop_name": "babuy.com.ua",
                },
                {
                    "item_name": "Apple iPhone 17 Pro Max 256GB Cosmic Orange (MFYN4)",
                    "item_url": "https://www.hotline.ua/go/price/13927738665/",
                    "shop_name": "didi.ua",
                },
                {
                    "item_name": "Apple iPhone 17 Pro Max 256Gb Cosmic Orange (MFYN4)",
                    "item_url": "https://www.hotline.ua/go/price/13927793134/",
                    "shop_name": "toiler.com.ua",
                },
                {
                    "item_name": "Apple Смартфон Apple iPhone 17 Pro Max 256GB Cosmic Orange (MFYN4)",
                    "item_url": "https://www.hotline.ua/go/price/13928266494/",
                    "shop_name": "q-techno.com.ua",
                },
                {
                    "item_name": "Apple Смартфон Apple iPhone 17 Pro Max 256GB Cosmic Orange MFYN4 [Cosmic Orange|256GB]",
                    "item_url": "https://www.hotline.ua/go/price/13928501151/",
                    "shop_name": "tehnokrat.ua",
                },
                {
                    "item_name": "Apple Смартфон Apple iPhone 17 Pro Max 256GB Cosmic Orange (MFYN4)",
                    "item_url": "https://www.hotline.ua/go/price/13928573587/",
                    "shop_name": "touch.com.ua",
                },
                {
                    "item_name": "Apple iPhone 17 Pro Max 256GB Cosmic Orange (MFYN4)",
                    "item_url": "https://www.hotline.ua/go/price/13928754840/",
                    "shop_name": "upps.ua",
                },
                {
                    "item_name": "Apple iPhone 17 Pro Max 256GB Cosmic Orange (MFYN4)",
                    "item_url": "https://www.hotline.ua/go/price/13929328003/",
                    "shop_name": "G-store.com.ua",
                },
                {
                    "item_name": "Apple iPhone 17 Pro Max 256GB Cosmic Orange (MFYN4)",
                    "item_url": "https://www.hotline.ua/go/price/13930270094/",
                    "shop_name": "appcover.com.ua",
                },
                {
                    "item_name": "Apple iPhone 17 Pro Max 256Gb Cosmic Orange (MFYN4)",
                    "item_url": "https://www.hotline.ua/go/price/13930353340/",
                    "shop_name": "itochka.com.ua",
                },
                {
                    "item_name": "Apple Смартфон Apple iPhone 17 Pro Max 256GB Cosmic Orange (MFYN4)",
                    "item_url": "https://www.hotline.ua/go/price/13931634575/",
                    "shop_name": "korealab.kiev.ua",
                },
                {
                    "item_name": "Apple Смартфон Apple iPhone 17 Pro Max 256GB Cosmic Orange (MFYN4)",
                    "item_url": "https://www.hotline.ua/go/price/13935436349/",
                    "shop_name": "evrotorg.com.ua",
                },
                {
                    "item_name": "Apple iPhone 17 Pro Max 256GB (Cosmic Orange) (MFYN4)",
                    "item_url": "https://www.hotline.ua/go/price/13946408269/",
                    "shop_name": "jabko.ua",
                },
                {
                    "item_name": "Apple Смартфон Apple iPhone 17 Pro Max 256GB Cosmic Orange (MFYN4)",
                    "item_url": "https://www.hotline.ua/go/price/13948390802/",
                    "shop_name": "grokholsky.com",
                },
                {
                    "item_name": "Apple Смартфон Apple iPhone 17 Pro Max 256GB Cosmic Orange (MFYN4) (1013100)",
                    "item_url": "https://www.hotline.ua/go/price/13958707531/",
                    "shop_name": "ipeople.ua",
                },
                {
                    "item_name": "Apple iPhone 17 Pro Max 256GB Cosmic Orange (MFYN4)",
                    "item_url": "https://www.hotline.ua/go/price/13962636534/",
                    "shop_name": "re-tech.com.ua",
                },
                {
                    "item_name": "Apple iPhone 17 Pro Max 256GB Cosmic Orange (MFYN4)",
                    "item_url": "https://www.hotline.ua/go/price/13965126341/",
                    "shop_name": "astore.org.ua",
                },
                {
                    "item_name": "Apple Смартфон Apple iPhone 17 Pro Max 256GB Cosmic Orange (MFYN4)",
                    "item_url": "https://www.hotline.ua/go/price/13966320318/",
                    "shop_name": "gstore.com.ua",
                },
                {
                    "item_name": "Apple iPhone 17 Pro Max 256GB Cosmic Orange (MFYN4)",
                    "item_url": "https://www.hotline.ua/go/price/13969165849/",
                    "shop_name": "imag.ua",
                },
                {
                    "item_name": "Apple iPhone 17 Pro Max 256GB Cosmic Orange (MFYN4)",
                    "item_url": "https://www.hotline.ua/go/price/13969575180/",
                    "shop_name": "allo.ua",
                },
                {
                    "item_name": "Apple iPhone 17 Pro Max 256GB Cosmic Orange (MFYN4)",
                    "item_url": "https://www.hotline.ua/go/price/13973665782/",
                    "shop_name": "matviez.com",
                },
                {
                    "item_name": "Apple Смартфон Apple iPhone 17 Pro Max 256GB Cosmic Orange (MFYN4)",
                    "item_url": "https://www.hotline.ua/go/price/13974446759/",
                    "shop_name": "freephone.in.ua",
                },
                {
                    "item_name": "Apple iPhone 17 Pro Max 256GB Cosmic Orange (MFYN4)",
                    "item_url": "https://www.hotline.ua/go/price/13974519007/",
                    "shop_name": "kvshop.com.ua",
                },
                {
                    "item_name": "APPLE iPhone 17 Pro Max, 256 ГБ, Cosmic Orange (MFYN4AF/A) (195950638912)",
                    "item_url": "https://www.hotline.ua/go/price/13975916454/",
                    "shop_name": "ispace.ua",
                },
                {
                    "item_name": "Apple Смартфон Apple iPhone 17 Pro Max 256GB Cosmic Orange (MFYN4)",
                    "item_url": "https://www.hotline.ua/go/price/13977629375/",
                    "shop_name": "justbuy.com.ua",
                },
                {
                    "item_name": "Apple iPhone 17 Pro Max 256GB Cosmic Orange (MFYN4)",
                    "item_url": "https://www.hotline.ua/go/price/13978105718/",
                    "shop_name": "xstore.ua",
                },
                {
                    "item_name": "Apple iPhone 17 Pro Max 256GB Cosmic Orange (MFYN4)",
                    "item_url": "https://www.hotline.ua/go/price/13980090897/",
                    "shop_name": "Y.ua",
                },
                {
                    "item_name": "Apple iPhone 17 Pro Max 256GB Cosmic Orange (MFYN4)",
                    "item_url": "https://www.hotline.ua/go/price/13980160922/",
                    "shop_name": "skay.ua",
                },
                {
                    "item_name": "Apple iPhone 17 Pro Max 256GB Cosmic Orange (MFYN4)",
                    "item_url": "https://www.hotline.ua/go/price/13980349732/",
                    "shop_name": "stls.store",
                },
                {
                    "item_name": "Apple Смартфон Apple iPhone 17 Pro Max 256GB Cosmic Orange (MFYN4)",
                    "item_url": "https://www.hotline.ua/go/price/13981059030/",
                    "shop_name": "openshop.ua",
                },
                {
                    "item_name": "Apple Смартфон Apple iPhone 17 Pro Max 256GB Cosmic Orange (MFYN4)",
                    "item_url": "https://www.hotline.ua/go/price/13981229531/",
                    "shop_name": "denika.ua",
                },
                {
                    "item_name": "Apple iPhone 17 Pro Max 256GB Cosmic Orange (MFYN4)",
                    "item_url": "https://www.hotline.ua/go/price/13981311322/",
                    "shop_name": "easymac.com.ua",
                },
                {
                    "item_name": "Apple Смартфон Apple iPhone 17 Pro Max 256GB Cosmic Orange (MFYN4)",
                    "item_url": "https://www.hotline.ua/go/price/13981349951/",
                    "shop_name": "avic.com.ua",
                },
            ],
            "title": "Apple iPhone 17 Pro Max 256GB Cosmic Orange (MFYN4)",
        }
        self.assertEqual(expected_result, HotlineItemCardParser.parse(product_blob))


# Add this to run with standard unittest
if __name__ == "__main__":
    unittest.main()
