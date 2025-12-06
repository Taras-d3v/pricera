import io
import json
import logging
import os
from collections import defaultdict
from pricera.models import ResponseObject
import gzip
import boto3
from scrapy import signals
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Any


class S3Pipeline:
    MAX_UPLOAD_WORKERS = 10
    MAX_RETRY_ATTEMPTS = 2
    """
    Pipeline collects incoming items by `object_hash` and, when the spider closes,
    uploads each request chain to a separate file in S3 in JSON Lines format.
    Each item is expected to contain the `object_hash` field.
    Scrapy settings expected:
      - S3_BUCKET_NAME
      - S3_PREFIX
    """

    def __init__(self, bucket_name: str, prefix: str):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.aws_region_name,
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
    def aws_region_name(self):
        return os.environ.get("AWS_REGION_NAME")

    @classmethod
    def from_crawler(cls, crawler):
        spider = crawler.spider
        bucket_name = spider.storage_bucket
        prefix = spider.storage_prefix

        pipeline = cls(
            bucket_name=bucket_name,
            prefix=prefix,
        )

        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def process_item(self, item: ResponseObject, spider):
        self.responses[item.object_hash].append(item.model_dump())
        return item

    def spider_closed(self, spider, reason):
        """
        Called when the spider is closed — uploads each request chain to S3 using parallel uploads.
        """
        if not self.responses:
            self.logger.debug("No responses collected, skipping S3 upload.")
            return

        try:
            self.collect_response_statuses(spider=spider, responses=self.responses)
            success_count, failure_count = self._upload_all_chains_parallel()
            self._log_upload_summary(success_count, failure_count)
        except Exception as e:
            self.logger.error("Unexpected error during parallel upload: %s", e)
        finally:
            # Always clear responses to prevent memory leaks
            self.responses.clear()

    @staticmethod
    def get_response_status(response_item: list[dict]) -> str:
        if not response_item:
            return "failure"

        for item in response_item:
            if item["status"] != 200:
                return "failure"

        return "success"

    def collect_response_statuses(self, spider, responses: dict) -> None:
        for object_hash, items in responses.items():
            response_status = self.get_response_status(items)

            statuses = spider.crawler.stats.get_value("custom_status", {})
            statuses[object_hash] = response_status
            spider.crawler.stats.set_value("custom_status", statuses)

    def _upload_all_chains_parallel(self) -> tuple[int, int]:
        """
        Upload all chains in parallel using thread pool.

        Returns:
            tuple: (success_count, failure_count)
        """
        success_count = 0
        failure_count = 0

        with ThreadPoolExecutor(max_workers=self.MAX_UPLOAD_WORKERS, thread_name_prefix="s3_upload") as executor:
            # Submit all upload tasks
            future_to_key = {
                executor.submit(self._upload_single_chain, object_hash, items): object_hash
                for object_hash, items in self.responses.items()
            }

            # Process results as they complete
            for future in as_completed(future_to_key):
                object_hash = future_to_key[future]
                try:
                    future.result()
                    success_count += 1
                except Exception as e:
                    failure_count += 1
                    self.logger.error("Chain %s failed to upload: %s", object_hash, e)

        return success_count, failure_count

    def _upload_single_chain(self, object_hash: str, items: List[Dict[str, Any]]) -> None:
        """
        Upload a single chain to S3 with retry logic.

        Args:
            object_hash: Unique identifier for the chain
            items: List of items to upload

        Raises:
            Exception: If upload fails after all retry attempts
        """
        s3_key = f"{self.prefix.rstrip('/')}/{object_hash}.jsonl.gz"

        for attempt in range(self.MAX_RETRY_ATTEMPTS + 1):
            try:
                bio = self._create_gzipped_stream(items, object_hash)
                self._upload_to_s3(bio, s3_key, object_hash)
                return  # Success - exit method

            except Exception as e:
                if attempt == self.MAX_RETRY_ATTEMPTS:
                    self.logger.error(
                        "Final upload failure for chain %s after %d attempts: %s", object_hash, attempt + 1, e
                    )
                    raise  # Re-raise after final attempt
                else:
                    self.logger.warning(
                        "Upload attempt %d failed for chain %s, retrying: %s", attempt + 1, object_hash, e
                    )

    def _create_gzipped_stream(self, items: List[Dict[str, Any]], object_hash: str) -> io.BytesIO:
        """
        Create gzipped JSON Lines stream from items.

        Args:
            items: List of dictionaries to compress
            object_hash: Identifier for filename

        Returns:
            io.BytesIO: Buffer containing gzipped data
        """
        bio = io.BytesIO()

        try:
            with gzip.GzipFile(fileobj=bio, mode="wb", filename=f"{object_hash}.jsonl") as gz:
                for item in items:
                    line = json.dumps(item, default=str, ensure_ascii=False) + "\n"
                    gz.write(line.encode("utf-8"))

            bio.seek(0)
            return bio

        except Exception as e:
            bio.close()  # Clean up on error
            raise Exception(f"Failed to create gzip stream for {object_hash}: {e}")

    def _upload_to_s3(self, bio: io.BytesIO, s3_key: str, object_hash: str) -> None:
        """
        Upload bytes buffer to S3.

        Args:
            bio: BytesIO buffer containing data to upload
            s3_key: Full S3 key path
            object_hash: Original object identifier for logging

        Raises:
            Exception: If S3 upload fails
        """
        extra_args = {"ContentType": "application/gzip"}

        try:
            self.s3.upload_fileobj(bio, self.bucket, s3_key, ExtraArgs=extra_args)
            self.logger.debug("Successfully uploaded chain %s to s3://%s/%s", object_hash, self.bucket, s3_key)
        finally:
            # Always close the buffer to free memory
            bio.close()

    def _log_upload_summary(self, success_count: int, failure_count: int) -> None:
        """
        Log summary of upload operation.

        Args:
            success_count: Number of successful uploads
            failure_count: Number of failed uploads
        """
        total_chains = len(self.responses)

        if failure_count == 0:
            self.logger.info("All %d chains successfully uploaded to S3", total_chains)
        else:
            self.logger.error(
                "Upload completed: %d successful, %d failed out of %d total chains",
                success_count,
                failure_count,
                total_chains,
            )
