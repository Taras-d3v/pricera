from typing import Union, List, ClassVar
from scrapy import signals
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import boto3
import io
import gzip
from botocore.exceptions import ClientError
from pricera.models import HashedURL
import logging
from twisted.python.failure import Failure

logger = logging.getLogger("base_collector")


class ScrapyConfigurationMixin:
    def __init__(self):
        self.spider_instance = None

    def process_scrapy_spider(
        self,
        spider_cls,
        start_urls: list[HashedURL],
        storage_bucket: str,
        storage_prefix: str,
        proxy_config=None,
        **kwargs,
    ):
        process = CrawlerProcess(get_project_settings())
        crawler = process.create_crawler(spider_cls)

        def handle_spider_opened(spider):
            self.spider_instance = spider

        def handle_spider_error(failure: Failure, response, spider):
            self.spider_error = failure.value
            logger.error(
                "Spider error occurred",
                extra={"exception": repr(self.spider_error), "url": getattr(response, "url", None)},
            )

        crawler.signals.connect(handle_spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(handle_spider_error, signal=signals.spider_error)

        try:
            process.crawl(
                crawler,
                start_urls=start_urls,
                proxy_config=proxy_config,
                storage_bucket=storage_bucket,
                storage_prefix=storage_prefix,
                **kwargs,
            )

            process.start()
        except Exception as e:
            logger.exception("Scrapy failed to start")
            self.spider_error = e

        return self.spider_instance


class BaseCollector(ScrapyConfigurationMixin):
    storage_bucket: ClassVar[str] = "pricera-crawled-data"
    is_synchronous: ClassVar[bool] = True
    db_name: ClassVar[str] = "pricera"

    @staticmethod
    def get_storage_file_name_from_url(url: str) -> str:
        url_with_hash = HashedURL.from_value(url)
        return f"{url_with_hash.hash}.jsonl.gz"

    @classmethod
    def prepare_urls(cls, urls: list[str]) -> list[HashedURL]:
        return HashedURL.from_values(urls)

    def crawl(self):
        raise NotImplementedError

    def update_crawl_status(self, *args, **kwargs):
        raise NotImplementedError

    def parse(self, *args, **kwargs) -> dict:
        raise NotImplementedError

    @classmethod
    def get_parser(cls, *args, **kwargs) -> "BaseCollector":
        raise NotImplementedError

    @classmethod
    def get_crawler(cls, *args, **kwargs) -> "BaseCollector":
        raise NotImplementedError

    @staticmethod
    def load_file_from_s3(
        bucket: str,
        prefix: str,
        filename: str,
    ) -> Union[str, List[dict]]:
        """
        Loads a file from S3 bucket. Supports both regular files and gzipped JSONL files.

        :param bucket: S3 bucket name
        :param prefix: S3 prefix/folder path
        :param filename: name of the file (e.g., "20-11-2025-rozetka-product.jsonl.gz")
        :return: file content as string or list of dicts (auto-detects gzipped files by .gz extension)
        """

        key = f"{prefix.rstrip('/')}/{filename}"

        try:
            s3 = boto3.client("s3")
            # Download file from S3
            response = s3.get_object(Bucket=bucket, Key=key)

            logger.info("Successfully loaded file from S3", extra={"s3_bucket": bucket, "s3_key": key})

            file_content = response["Body"].read()

            # Auto-detect gzipped files by extension
            if filename.endswith(".gz"):
                bio = io.BytesIO(file_content)

                with gzip.GzipFile(fileobj=bio, mode="rb") as gz_file:
                    decompressed_data = gz_file.read().decode("utf-8")
                    return decompressed_data
            else:
                return file_content.decode("utf-8")

        except ClientError as exception:
            error_code = exception.response["Error"]["Code"]
            if error_code == "NoSuchKey":
                logger.error("File not found in S3", extra={"s3_bucket": bucket, "s3_key": key})
                raise FileNotFoundError(f"File not found in S3: s3://{bucket}/{key}")
            else:
                logger.error(
                    "Error during loading file from s3", exc_info=exception, extra={"s3_bucket": bucket, "s3_key": key}
                )
                raise IOError(f"S3 error loading file s3://{bucket}/{key}: {exception}")
