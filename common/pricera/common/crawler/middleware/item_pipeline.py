# python
import io
import json
import logging
import os
from collections import defaultdict
from datetime import datetime

import boto3
from scrapy import signals
from scrapy.exceptions import DropItem


class S3ChainUploadPipeline:
    """
    Pipeline collects incoming items by `chain_uuid` and, when the spider closes,
    uploads each chain to a separate file in S3 in JSON Lines format.
    Each item is expected to contain the `chain_uuid` field.
    Scrapy settings expected:
      - S3_BUCKET_NAME
      - S3_PREFIX
    """

    def __init__(self, bucket_name, prefix="chains/"):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=self.aws_secret_access_key,
            aws_secret_access_key=self.aws_access_key_id,
            region_name=self.aws_region,
        )
        self.bucket = bucket_name
        self.prefix = prefix or ""
        self.responses = defaultdict(list)

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
        settings = crawler.settings
        bucket_name = settings.get("S3_BUCKET_NAME")
        prefix = settings.get("S3_PREFIX")

        pipeline = cls(
            bucket_name=bucket_name,
            prefix=prefix,
        )

        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def process_item(self, item, spider):
        chain_uuid = item.get("chain_uuid")
        if not chain_uuid:
            # If there's no chain_uuid — log and drop the item; modify behaviour if needed
            raise DropItem("Missing chain_uuid in item")

        # Convert the item to a serializable structure (usually a dict)
        try:
            serializable = dict(item)
        except Exception:
            # As a fallback — convert values to strings
            serializable = {k: str(v) for k, v in item.items()}

        self.responses[chain_uuid].append(serializable)
        return item

    def spider_closed(self, spider, reason):
        """
        Called when the spider is closed — uploads each chain to a separate file.
        """
        if not self.responses:
            self.logger.debug("No responses collected, skipping S3 upload.")
            return

        for chain_uuid, items in self.responses.items():
            timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
            safe_uuid = chain_uuid or f"no-chain-{timestamp}"
            key = f"{self.prefix.rstrip('/')}/{safe_uuid}.jsonl"

            bio = io.BytesIO()
            for it in items:
                line = json.dumps(it, default=str, ensure_ascii=False)
                bio.write(line.encode("utf-8"))
                bio.write(b"\n")
            bio.seek(0)

            try:
                # Upload the object to S3
                self.s3.upload_fileobj(bio, self.bucket, key)
                self.logger.info("Uploaded chain %s to s3://%s/%s", safe_uuid, self.bucket, key)
            except Exception as e:
                self.logger.error("Failed to upload chain %s: %s", safe_uuid, e)

        # Clear accumulated data
        self.responses.clear()
