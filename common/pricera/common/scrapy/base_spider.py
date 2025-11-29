from scrapy.http import Response
from pricera.models import ResponseObject
import scrapy
from scrapy import Spider


class BaseSpider(Spider):
    custom_settings = {
        "LOG_ENABLED": True,  # Disable logging for cleaner output
        "RETRY_TIMES": 3,  # Retry failed requests up to 3 times
    }

    def __init__(self, s3_prefix: str, s3_bucket: str, *args, **kwargs):
        Spider.__init__(self, *args, **kwargs)
        self.s3_bucket: str = s3_bucket
        self.s3_prefix: str = s3_prefix

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
                    url=url.url,
                    callback=self.parse,
                    meta={
                        "object_key": url.hash,
                    },
                )
