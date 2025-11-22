from typing import List, Union
import boto3
from botocore.exceptions import ClientError
import io
import gzip


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
