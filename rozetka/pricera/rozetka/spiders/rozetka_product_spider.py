from typing import Iterator

from scrapy.http import Response, Request

from pricera.common.scrapy import BaseSpider
from pricera.models import ResponseObject
from pricera.models import URLWithHash


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
        "ITEM_PIPELINES": {
            "pricera.common.scrapy.S3Pipeline": 300,
        },
        "DOWNLOAD_HANDLERS": {
            "http": "pricera.common.scrapy.PriceraImpersonateDownloadHandler",
            "https": "pricera.common.scrapy.PriceraImpersonateDownloadHandler",
        },
    }

    def __init__(self, start_urls: list[URLWithHash], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_urls = start_urls

    def start_requests(self):
        for url in self.start_urls:
            product_id = self.get_product_id_from_url(url.url)
            api_url = f"https://common-api.rozetka.com.ua/v1/api/product/details?country=UA&lang=ua&ids={product_id}"

            yield Request(
                url=api_url,
                callback=self.parse,
                meta={
                    "object_key": url.hash,
                },
            )

    def parse(self, response: Response, *args, **kwargs) -> Iterator[ResponseObject]:
        yield self.collect_response(response=response)

    @staticmethod
    def get_product_id_from_url(url: str) -> str:
        import re

        match = re.search(r"/p(\d+)/?$", url)
        if not match:
            raise ValueError(f"Can't extract product id from url: {url}")
        return match.group(1)
