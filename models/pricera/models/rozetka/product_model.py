from pydantic import BaseModel


class ImageModel(BaseModel):
    url: str


class RozetkaProductModel(BaseModel):
    title: str
    brand: str

    category_title: str
    root_category_title: str
    description: str
    images: list[ImageModel]
    reviews: int
    review_rating: int

    price: int
    old_price: int
    sell_status: str
    seller: str
