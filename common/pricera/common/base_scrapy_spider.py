import uuid
from collections import defaultdict

import scrapy
from scrapy import Spider


class BaseSpider(Spider):
    custom_settings = {
        "LOG_ENABLED": True,  # Disable logging for cleaner output
        "RETRY_TIMES": 3,  # Retry failed requests up to 3 times
    }

    def __init__(self, s3_bucket: str, s3_prefix: str, *args, **kwargs):
        Spider.__init__(self, *args, **kwargs)
        self.s3_bucket: str = s3_bucket
        self.s3_prefix: str = s3_prefix
        self.responses: defaultdict = defaultdict(list)

    def start_requests(self):
        """Generate initial requests with chain UUIDs"""
        if hasattr(self, "start_urls"):
            for url in self.start_urls:
                chain_uuid = str(uuid.uuid4())
                yield scrapy.Request(
                    url=url,
                    callback=self.parse,
                    meta={
                        "chain_uuid": chain_uuid,
                    },
                )
