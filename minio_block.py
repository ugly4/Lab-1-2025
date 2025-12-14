from prefect.blocks.core import Block
from pydantic import SecretStr
from minio import Minio

class MinIOBlock(Block):
    _block_type_name = "MinIO Credentials"
    endpoint: str
    access_key: SecretStr
    secret_key: SecretStr
    bucket: str

    def get_client(self):
        return Minio(
            self.endpoint,
            access_key=self.access_key.get_secret_value(),
            secret_key=self.secret_key.get_secret_value(),
            secure=False,
        )