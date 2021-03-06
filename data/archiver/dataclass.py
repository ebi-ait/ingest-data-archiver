from typing import List, Dict
from dataclasses import dataclass
import json
import datetime


class DataArchiverRequestParseExpection(Exception):
    pass

@dataclass
class DataArchiverRequest:
    sub_uuid: str
    files: List[str]
    stream: bool 

    @staticmethod
    def from_dict(data: Dict) -> 'DataArchiverRequest':
        try:
            return DataArchiverRequest(data["sub_uuid"],
                                     data["files"] if "files" in data else [],
                                     data["stream"] if "stream" in data else True)
        except (KeyError, TypeError) as e:
            raise DataArchiverRequestParseExpection(e)


@dataclass
class FileResult:
    uuid: str
    file_name: str
    cloud_url: str
    size: int
    compressed: bool
    md5: str
    ena_upload_path: str
    success: bool
    error: str

    def __init__(self, uuid, file_name, cloud_url, size=0, compressed=False, md5=None, ena_upload_path=None, success=True, error=None):
        self.uuid = uuid
        self.file_name = file_name
        self.cloud_url = cloud_url
        self.size = size                # s3 size to be more accurate, not size from file metadata
        self.compressed = compressed    # true is file is compressed during archiving
        self.md5 = md5                  # calculated md5 checksum
        self.ena_upload_path = ena_upload_path    # ena upload area sub directory. default is root.
        self.success = success
        self.error = error

    @classmethod
    def from_file(cls, file):
        return cls(file["uuid"], file["file_name"], file["cloud_url"])

    @classmethod
    def not_found_error(cls, uuid):
        return cls(uuid, file_name=None, cloud_url=None, success=False, error="File not found in Ingest.")


@dataclass
class DataArchiverResult:
    sub_uuid: str
    success: bool
    error: str
    files: List[FileResult]

    def __init__(self, sub_uuid, success=True, error=None, files=[]):
        self.sub_uuid = sub_uuid
        self.success = success
        self.error = error
        self.files = files

    def to_dict(self):
        return json.loads(json.dumps(self, default=lambda o: o.__dict__))

    def update_status(self):
        not_success = 0
        for file in self.files:
            self.success = self.success and file.success
            if not file.success:
                not_success += 1
        if not_success > 0:
            self.error = f"{not_success} file(s) failed to archived."
        


@dataclass
class QueueConfig:
    name: str
    routing_key: str
    exchange: str
    exchange_type: str
    retry: bool
    retry_policy: dict


@dataclass
class AmqpConnConfig:
    host: str
    port: int

    def broker_url(self):
        return f'amqp://{self.host}:{str(self.port)}'
