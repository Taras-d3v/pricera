from typing import List, Union
import boto3
from botocore.exceptions import ClientError
import io
import gzip
import json


class BaseParser:
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
                return BaseParser._read_gzipped_content(file_content, filename)
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

    @staticmethod
    def _read_gzipped_content(compressed_data: bytes, filename: str) -> Union[str, List[dict]]:
        """
        Read gzipped content and return appropriate format based on file type.

        :param compressed_data: gzipped data as bytes
        :param filename: original filename to determine content type
        :return: string for text files, list of dicts for JSONL files
        """
        # Decompress the data
        bio = io.BytesIO(compressed_data)

        with gzip.GzipFile(fileobj=bio, mode="rb") as gz_file:
            decompressed_data = gz_file.read().decode("utf-8")
            return decompressed_data

        # # Check if it's JSONL format (multiple JSON objects, one per line)
        # if filename.endswith('.jsonl.gz') or filename.endswith('.json.gz'):
        #     return BaseParser._parse_jsonl_content(decompressed_data)
        # else:
        #     # For other gzipped files, return as text
        #     return decompressed_data

    @staticmethod
    def _parse_jsonl_content(jsonl_content: str) -> List[dict]:
        """
        Parse JSONL content into a list of dictionaries.

        :param jsonl_content: JSONL content as string
        :return: list of parsed JSON objects
        """
        items = []
        for line in jsonl_content.strip().split("\n"):
            if line.strip():  # Skip empty lines
                items.append(json.loads(line.strip()))
        return items
