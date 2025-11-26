from pricera.common.utilities import parse_line
from pricera.models.hotline import HotlineItemCardModel, HotlineItemOfferModel
from bs4 import BeautifulSoup, Tag
import re


class HotlineItemCardParser:
    base_url = "https://www.hotline.ua"

    @classmethod
    def parse(cls, product_blob: dict) -> dict:
        parsed_line = parse_line(product_blob)
        soup = BeautifulSoup(parsed_line.text, "lxml")
        model = HotlineItemCardModel(title=cls.get_item_card_title(soup), offers=cls.parse_item_offers(soup))
        return model.model_dump()

    @staticmethod
    def get_item_offers(soup: BeautifulSoup) -> list[Tag]:
        item_general_offer_tag = soup.find("div", attrs={"id": "productOffersListContainer"})
        item_offer_tag = item_general_offer_tag.find("div", attrs={"class": False}, recursive=False)
        return item_offer_tag.find_all("div", attrs={"class": True}, recursive=False)

    @classmethod
    def parse_item_offer_url(cls, offer_tag: Tag) -> str:
        href_pattern = re.compile(r"^\/go\/price\/.+")
        item_path_tag = offer_tag.find("a", attrs={"data-eventcategory": "Pages Product Prices", "href": href_pattern})
        item_path = item_path_tag.get("href")
        return f"{cls.base_url}{item_path}"

    @staticmethod
    def parse_item_offer_shop_url(offer_tag: Tag) -> str:
        website_href_tag = offer_tag.find("div", attrs={"firm-website": True})
        return website_href_tag["firm-website"]

    @staticmethod
    def parse_item_offer_name(offer_tag: Tag) -> str:
        item_name_tag = offer_tag.find("div", attrs={"class": "text-wrapper"})
        return item_name_tag.text.strip()

    @classmethod
    def parse_item_offer(cls, offer_tag: Tag) -> HotlineItemOfferModel:
        model = HotlineItemOfferModel(
            shop_name=cls.parse_item_offer_shop_url(offer_tag),
            item_name=cls.parse_item_offer_name(offer_tag),
            item_url=cls.parse_item_offer_url(offer_tag),
        )
        return model

    @classmethod
    def parse_item_offers(cls, soup: BeautifulSoup) -> list[HotlineItemOfferModel]:
        return list(map(cls.parse_item_offer, cls.get_item_offers(soup)))

    @staticmethod
    def get_item_card_title(soup: BeautifulSoup) -> str:
        raw_title_text = soup.find("title").text.strip()

        card_title = re.match(pattern=r"^(.*?)\s*купити в інтернет-магазині", string=raw_title_text)
        return card_title.group(1).strip()
