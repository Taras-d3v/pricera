from scrapy.http import Response
from pricera.models import ResponseObject
import scrapy
from scrapy import Spider


class BaseSpider(Spider):
    def __init__(self, storage_bucket: str, storage_prefix: str, *args, **kwargs):
        Spider.__init__(self, *args, **kwargs)
        self.storage_bucket: str = storage_bucket
        self.storage_prefix: str = storage_prefix

    def collect_response(self, response: Response) -> ResponseObject:
        return ResponseObject(
            url=response.url,
            text=response.text,
            status=response.status,
            object_key=response.meta["object_key"],
        )

    def start_requests(self):
        """Generate initial requests with chain UUIDs"""
        if hasattr(self, "start_urls"):
            for url in self.start_urls:
                yield scrapy.Request(
                    url=url,
                    callback=self.parse,
                    meta={
                        "object_key": url.hash,
                    },
                )
