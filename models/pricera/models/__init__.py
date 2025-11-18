__all__ = ["ResponseObject", "HotlineListingsModel", "HotlineItemOfferModel"]

from dataclasses import dataclass

from .hotline.listings_model import HotlineItemOfferModel, HotlineListingsModel


@dataclass
class ResponseObject:
    url: str
    text: str
    status: int
    object_key: str
