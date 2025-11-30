from pricera.common.utilities import parse_line
from pricera.models.rozetka.product_model import RozetkaProductModel, ImageModel


class RozetkaProductParser:
    @classmethod
    def parse(cls, product_blob: dict) -> dict:
        parsed_line = parse_line(product_blob)
        raw_data = parsed_line.raw_data["data"][0]
        model = RozetkaProductModel(
            title=raw_data["title"],
            brand=raw_data["brand"],
            category_title=raw_data["category"]["title"],
            root_category_title=raw_data["category"]["root_category_title"],
            description=raw_data["docket"],
            images=cls.parse_images(raw_data["images"]),
            reviews=raw_data["comments_amount"],
            review_rating=raw_data["comments_mark"],
            price=raw_data["price"],
            old_price=raw_data["old_price"],
            sell_status=raw_data["status"],
            seller=raw_data["seller"]["title"],
        )
        return model.model_dump()

    @staticmethod
    def parse_images(images: dict) -> list[ImageModel]:
        result = []
        for key in ["main", "hover"]:
            result.append(ImageModel(url=images[key]))
        return result

    @staticmethod
    def get_product_id_from_url(url: str) -> str:
        import re

        match = re.search(r"/p(\d+)/?$", url)
        if not match:
            raise ValueError(f"Can't extract product id from url: {url}")
        return match.group(1)
