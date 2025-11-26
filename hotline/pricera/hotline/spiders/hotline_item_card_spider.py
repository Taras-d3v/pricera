from typing import Iterator

from scrapy.http import Response

from pricera.common.base_scrapy_spider import BaseSpider
from pricera.models import ResponseObject


class HotlineItemCardSpider(BaseSpider):
    name = "hotline_item_card_spider"

    custom_settings = {
        "DEFAULT_REQUEST_HEADERS": {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "en-US,en;q=0.9,uk;q=0.8,ru;q=0.7,pt;q=0.6",
            "cache-control": "no-cache",
            "pragma": "no-cache",
            "priority": "u=0, i",
            "sec-ch-ua": '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "same-origin",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
        },
        "ITEM_PIPELINES": {
            "pricera.common.middlewares.S3Pipeline": 300,
        },
        "DOWNLOAD_HANDLERS": {
            "http": "pricera.common.middlewares.PriceraImpersonateDownloadHandler",
            "https": "pricera.common.middlewares.PriceraImpersonateDownloadHandler",
        },
    }

    def __init__(self, start_urls: list[str], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_urls = start_urls

    def parse(self, response: Response, *args, **kwargs) -> Iterator[ResponseObject]:
        yield self.collect_response(response=response)
