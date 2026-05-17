import csv
import json
from typing import Any

import boto3  # type: ignore
from botocore.client import BaseClient
from prefect.blocks.system import Secret


class SpacesClient:
    _client: BaseClient | None = None
    BUCKET_NAME = "peaky76"

    @classmethod
    def _create(cls) -> BaseClient:
        session = boto3.session.Session()
        spaces_key: Any = Secret.load("spaces-key")
        spaces_secret: Any = Secret.load("spaces-secret")
        return session.client(
            "s3",
            region_name="ams3",
            endpoint_url="https://ams3.digitaloceanspaces.com",
            aws_access_key_id=spaces_key.get(),
            aws_secret_access_key=spaces_secret.get(),
        )

    @classmethod
    def get(cls) -> BaseClient:
        if cls._client is None:
            cls._client = cls._create()
        return cls._client

    @classmethod
    def get_files(cls, dirname, modified_after=None):
        continuation_token = ""
        client = cls.get()
        while True:
            # NOSONAR - ExpectedBucketOwner is AWS S3 specific, not supported by DigitalOcean Spaces
            response = client.list_objects_v2(
                Bucket=cls.BUCKET_NAME,
                Prefix=dirname,
                ContinuationToken=continuation_token,
            )
            files = response.get("Contents", [])
            continuation_token = response.get("NextContinuationToken")

            def within_date(x):
                return modified_after is None or x.get("LastModified") > modified_after

            yield from [
                key
                for file in files
                if "." in (key := file.get("Key")) and within_date(file)
            ]
            if not response.get("IsTruncated"):
                break

    @classmethod
    def stream_file(cls, file_path):
        client = cls.get()
        obj = client.get_object(Bucket=cls.BUCKET_NAME, Key=file_path)
        return obj["Body"].read()

    @classmethod
    def read_file(cls, file_path):
        file_type = file_path.split(".")[-1]
        output = {
            "csv": lambda x: list(csv.reader(x.splitlines())),
            "json": lambda x: json.loads(x),
        }
        stream = cls.stream_file(file_path).decode("utf-8")
        return output[file_type](stream)

    @classmethod
    def write_file(cls, content, filename):
        client = cls.get()
        if isinstance(content, str):
            content = content.encode("utf-8")
        client.put_object(
            Bucket=cls.BUCKET_NAME, Key=filename, Body=content, ACL="private"
        )

    @classmethod
    def delete_file(cls, filename):
        client = cls.get()
        client.delete_object(Bucket=cls.BUCKET_NAME, Key=filename)

    @classmethod
    def edit_json_file(cls, filename, edit_func):
        data = cls.read_file(filename)
        modified_data = edit_func(data)
        json_content = json.dumps(modified_data, indent=2, ensure_ascii=False)
        cls.write_file(json_content, filename)

        return modified_data
