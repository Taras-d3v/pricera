import unittest

from pricera.common import load_file_from_sub_folder
from pricera.hotline.parsers.listings_parser import HotlineListingsParser


class TestRozetkaProductParser(unittest.TestCase):
    def test_parse(self):
        test_file = load_file_from_sub_folder(filename="hotline_listings.html")
        expected_result = {
            "title": "Смартфон Apple iPhone 17 Pro Max 256GB Cosmic Orange (MFYN4)",
            "items": [
                {"name": "iTMag", "url": "https://hotline.ua/go/price/13912134630/"},
                {
                    "name": "Noteboochek",
                    "url": "https://hotline.ua/go/price/13912167099/",
                },
                {
                    "name": "PixoPhone",
                    "url": "https://hotline.ua/go/price/13913196433/",
                },
                {"name": "Just Buy", "url": "https://hotline.ua/go/price/13913979317/"},
                {"name": "VR STORE", "url": "https://hotline.ua/go/price/13916755086/"},
                {"name": "Fopi.ua", "url": "https://hotline.ua/go/price/13918282091/"},
                {"name": "iPeople", "url": "https://hotline.ua/go/price/13918394952/"},
                {
                    "name": "PCshop.UA",
                    "url": "https://hotline.ua/go/price/13918438831/",
                },
                {"name": "BUY.UA", "url": "https://hotline.ua/go/price/13919069242/"},
                {
                    "name": "Platforma Ukraine",
                    "url": "https://hotline.ua/go/price/13919089527/",
                },
                {
                    "name": "GRO (ex GROKHOLSKY)",
                    "url": "https://hotline.ua/go/price/13919094799/",
                },
                {"name": "SmartMag", "url": "https://hotline.ua/go/price/13919232932/"},
                {"name": "Mr.Fix", "url": "https://hotline.ua/go/price/13920057826/"},
                {
                    "name": "iFrukt.com",
                    "url": "https://hotline.ua/go/price/13920227016/",
                },
                {
                    "name": "Гаджет Авеню",
                    "url": "https://hotline.ua/go/price/13920293046/",
                },
                {
                    "name": "expertonline.com.ua",
                    "url": "https://hotline.ua/go/price/13920667911/",
                },
                {"name": "aStore", "url": "https://hotline.ua/go/price/13920944507/"},
                {
                    "name": "DomowikUA",
                    "url": "https://hotline.ua/go/price/13921204503/",
                },
                {
                    "name": "DENIKA.UA",
                    "url": "https://hotline.ua/go/price/13921217306/",
                },
                {"name": "ЖУК", "url": "https://hotline.ua/go/price/13921222817/"},
                {
                    "name": "iCenter.in.ua",
                    "url": "https://hotline.ua/go/price/13921839199/",
                },
                {
                    "name": "Zelen Store",
                    "url": "https://hotline.ua/go/price/13922987462/",
                },
                {
                    "name": "Sota Store",
                    "url": "https://hotline.ua/go/price/13923685511/",
                },
                {"name": "iPodrom", "url": "https://hotline.ua/go/price/13923767923/"},
                {
                    "name": "iStore.ua",
                    "url": "https://hotline.ua/go/price/13923927319/",
                },
                {"name": "Знайомі", "url": "https://hotline.ua/go/price/13925740297/"},
                {"name": "EGadget", "url": "https://hotline.ua/go/price/13926328256/"},
                {"name": "MOYO", "url": "https://hotline.ua/go/price/13926989833/"},
                {"name": "iMag.ua", "url": "https://hotline.ua/go/price/13927092793/"},
                {
                    "name": "LuckyLink.kiev.ua",
                    "url": "https://hotline.ua/go/price/13927194072/",
                },
                {
                    "name": "Yellow.ua",
                    "url": "https://hotline.ua/go/price/13927237130/",
                },
                {
                    "name": "GameShop.ua",
                    "url": "https://hotline.ua/go/price/13927547088/",
                },
                {
                    "name": "GSTORE.UA",
                    "url": "https://hotline.ua/go/price/13927639298/",
                },
                {"name": "BaBuy", "url": "https://hotline.ua/go/price/13927641555/"},
                {"name": "DiDi", "url": "https://hotline.ua/go/price/13927738665/"},
                {"name": "AppleFun", "url": "https://hotline.ua/go/price/13927745964/"},
                {
                    "name": "Toiler.com.ua",
                    "url": "https://hotline.ua/go/price/13927793134/",
                },
                {"name": "Q-TECHNO", "url": "https://hotline.ua/go/price/13928266494/"},
                {
                    "name": "Mobileplanet",
                    "url": "https://hotline.ua/go/price/13928322507/",
                },
                {
                    "name": "ТЕХНОКРАТ",
                    "url": "https://hotline.ua/go/price/13928501151/",
                },
                {"name": "Touch", "url": "https://hotline.ua/go/price/13928573587/"},
                {"name": "UPPS.UA", "url": "https://hotline.ua/go/price/13928754840/"},
                {"name": "G-STORE", "url": "https://hotline.ua/go/price/13929328003/"},
                {"name": "AppCover", "url": "https://hotline.ua/go/price/13930270094/"},
                {"name": "itochka", "url": "https://hotline.ua/go/price/13930353340/"},
                {"name": "YABLUKA", "url": "https://hotline.ua/go/price/13930651990/"},
                {"name": "Korealab", "url": "https://hotline.ua/go/price/13931634575/"},
                {"name": "Evrotorg", "url": "https://hotline.ua/go/price/13935436349/"},
                {"name": "HappyApp", "url": "https://hotline.ua/go/price/13938224871/"},
                {
                    "name": "kubik.store",
                    "url": "https://hotline.ua/go/price/13942777356/",
                },
                {
                    "name": "Інтернет магазин OpenShop",
                    "url": "https://hotline.ua/go/price/13945117246/",
                },
                {"name": "Elektron", "url": "https://hotline.ua/go/price/13945281326/"},
                {"name": "AVIC", "url": "https://hotline.ua/go/price/13945510886/"},
                {"name": "Ябко", "url": "https://hotline.ua/go/price/13946408269/"},
                {
                    "name": "AppleStore",
                    "url": "https://hotline.ua/go/price/13946665637/",
                },
                {
                    "name": "Скай (skay.ua)",
                    "url": "https://hotline.ua/go/price/13946716342/",
                },
                {"name": "Matviez", "url": "https://hotline.ua/go/price/13947495906/"},
            ],
        }
        self.assertEqual(expected_result, HotlineListingsParser.parse(test_file))


# Add this to run with standard unittest
if __name__ == "__main__":
    unittest.main()
