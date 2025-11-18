# python
import io
import json
import logging
import os
from collections import defaultdict

import boto3
from scrapy import signals
from scrapy.exceptions import DropItem


class S3Pipeline:
    """
    Pipeline collects incoming items by `object_key` and, when the spider closes,
    uploads each request chain to a separate file in S3 in JSON Lines format.
    Each item is expected to contain the `object_key` field.
    Scrapy settings expected:
      - S3_BUCKET_NAME
      - S3_PREFIX
    """

    def __init__(self, bucket_name: str, prefix: str):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=self.aws_secret_access_key,
            aws_secret_access_key=self.aws_access_key_id,
            region_name=self.aws_region,
        )
        self.bucket: str = bucket_name
        self.prefix: str = prefix
        self.responses: defaultdict = defaultdict(list)

    @property
    def aws_access_key_id(self):
        return os.environ.get("AWS_ACCESS_KEY_ID")

    @property
    def aws_secret_access_key(self):
        return os.environ.get("AWS_SECRET_ACCESS_KEY")

    @property
    def aws_region(self):
        return os.environ.get("AWS_REGION")

    @classmethod
    def from_crawler(cls, crawler):
        spider = crawler.spider
        bucket_name = spider.s3_bucket
        prefix = spider.s3_prefix

        pipeline = cls(
            bucket_name=bucket_name,
            prefix=prefix,
        )

        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def process_item(self, item, spider):
        object_key = item.get("object_key")
        if not object_key:
            # If there's no object_key — log and drop the item; modify behaviour if needed
            raise DropItem("Missing object_key in item")

        # Convert the item to a serializable structure (usually a dict)
        try:
            serializable = dict(item)
        except Exception:
            # As a fallback — convert values to strings
            serializable = {k: str(v) for k, v in item.items()}

        self.responses[object_key].append(serializable)
        return item

    def spider_closed(self, spider, reason):
        """
        Called when the spider is closed — uploads each request chain to a separate file.
        """
        if not self.responses:
            self.logger.debug("No responses collected, skipping S3 upload.")
            return

        for object_key, items in self.responses.items():
            key = f"{self.prefix.rstrip('/')}/{object_key}"

            bio = io.BytesIO()
            for it in items:
                line = json.dumps(it, default=str, ensure_ascii=False)
                bio.write(line.encode("utf-8"))
                bio.write(b"\n")
            bio.seek(0)

            try:
                # Upload the object to S3
                self.s3.upload_fileobj(bio, self.bucket, key)
                self.logger.info("Uploaded chain %s to s3://%s/%s", object_key, self.bucket, key)
            except Exception as e:
                self.logger.error("Failed to upload chain %s: %s", object_key, e)

        # Clear accumulated data
        self.responses.clear()
