from typing import Any, Iterable

from scrapy.http import Response

from pricera.common.base_scrapy_spider import BaseSpider
from pricera.models import ResponseObject


class RozetkaProductSpider(BaseSpider):
    name = "rozetka_product_spider"

    custom_settings = {
        "DEFAULT_REQUEST_HEADERS": {
            "accept": "application/json, text/plain, */*",
            "accept-language": "en-US,en;q=0.9",
            "cache-control": "no-cache",
            "content-type": "application/json",
            "origin": "https://rozetka.com.ua",
            "pragma": "no-cache",
            "priority": "u=1, i",
            "sec-ch-ua": '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-site",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
        },
    }

    def __init__(self, start_urls: list[str], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_urls = start_urls

    def parse(self, response: Response, *args, **kwargs) -> Iterable[Any]:
        yield ResponseObject(
            url=response.url,
            text=response.text,
            status=response.status,
            chain_uuid=response.meta["chain_uuid"],
        )
