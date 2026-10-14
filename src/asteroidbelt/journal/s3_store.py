"""
S3Store keeps blobs in an S3 bucket. Requires the boto3 package.
"""

from asyncio import to_thread

from asteroidbelt.journal.store import Store


class S3Store(Store):
    """
    Claim check storage on S3 or any compatible endpoint. Static
    credentials and TLS verification (a CA bundle path, or False
    to disable) are explicit arguments; anything else boto3
    accepts, endpoint_url and region_name included, passes through
    the constructor untouched. Or hand in a ready boto3 client.
    """

    def __init__(
        self,
        bucket: str,
        prefix: str = "",
        access_key: str | None = None,
        secret_key: str | None = None,
        verify: bool | str | None = None,
        client=None,
        **options,
    ) -> None:
        self.bucket = bucket
        self.prefix = prefix
        self.access_key = access_key
        self.secret_key = secret_key
        self.verify = verify
        self.s3 = client
        self.options = options

    async def open(self) -> None:
        if self.s3 is not None:
            return
        import boto3

        self.s3 = await to_thread(
            boto3.client,
            "s3",
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            verify=self.verify,
            **self.options,
        )

    def locate(self, key: str) -> str:
        return f"{self.prefix}{key}"

    def fetch(self, key: str) -> bytes | None:
        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=self.locate(key))
        except self.s3.exceptions.NoSuchKey:
            return None
        return response["Body"].read()

    async def put(self, key: str, blob: bytes) -> None:
        await to_thread(
            self.s3.put_object, Bucket=self.bucket, Key=self.locate(key), Body=blob
        )

    async def get(self, key: str) -> bytes | None:
        return await to_thread(self.fetch, key)

    async def delete(self, key: str) -> None:
        await to_thread(
            self.s3.delete_object, Bucket=self.bucket, Key=self.locate(key)
        )
