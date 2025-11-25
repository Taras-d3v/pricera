from typing import Union, List
from scrapy import signals
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import boto3
import io
import gzip
from botocore.exceptions import ClientError
from pricera.models import URLWithHash


class ScrapyConfigurationMixin:
    def __init__(self):
        self.spider_instance = None

    def process_scrapy_spider(
        self, spider_cls, start_urls: list[URLWithHash], s3_bucket: str, s3_prefix: str, proxy_config=None, **kwargs
    ):
        process = CrawlerProcess(get_project_settings())
        crawler = process.create_crawler(spider_cls)

        def handle_spider_opened(spider):
            self.spider_instance = spider

        crawler.signals.connect(handle_spider_opened, signal=signals.spider_opened)

        process.crawl(
            crawler,
            start_urls=start_urls,
            proxy_config=proxy_config,
            s3_bucket=s3_bucket,
            s3_prefix=s3_prefix,
            **kwargs,
        )

        process.start()

        return self.spider_instance


class BaseCollector(ScrapyConfigurationMixin):
    @classmethod
    def prepare_urls(cls, urls: list[str]) -> list[URLWithHash]:
        return URLWithHash.from_urls(urls)

    def crawl(self):
        raise NotImplemented

    def parse(self, *args, **kwargs) -> dict:
        raise NotImplemented

    @classmethod
    def get_collector(cls, *args, **kwargs) -> "BaseCollector":
        raise NotImplemented

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
                # todo: replace with proper logging
                print(f"File not found in S3: s3://{bucket}/{key}")
                raise FileNotFoundError(f"File not found in S3: s3://{bucket}/{key}")
            else:
                # todo: replace with proper logging
                print(f"S3 error: {exception}")
                raise IOError(f"S3 error loading file s3://{bucket}/{key}: {exception}")
