from pydantic import BaseModel


class HotlineItemOfferModel(BaseModel):
    shop_name: str
    item_name: str
    item_url: str


class HotlineItemCardModel(BaseModel):
    title: str
    offers: list[HotlineItemOfferModel] = []
