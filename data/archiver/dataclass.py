import json
from dataclasses import dataclass, field
from typing import List, Dict


class DataArchiverRequestParseException(Exception):
    pass

@dataclass
class DataArchiverRequest:
    sub_uuid: str
    files: List[str] = field(default_factory=list)
    stream: bool = field(default=True)

@dataclass
class FileResult:
    uuid: str
    file_name: str
    cloud_url: str
    # s3 size to be more accurate, not size from file metadata
    size: int = field(default=0)
    # true if file is compressed during archiving
    compressed: bool = field(default=False)
    # calculated md5 checksum
    md5: str = field(default=None)
    # ena upload area sub directory. default is root.
    ena_upload_path: str = field(default=None)
    success: bool = field(default=True)
    error: str = field(default=None)

    @classmethod
    def from_file(cls, file):
        return cls(file["uuid"], file["file_name"], file["cloud_url"])

    @classmethod
    def not_found_error(cls, uuid):
        return cls(uuid, file_name='', cloud_url='', success=False, error="File not found in Ingest.")


@dataclass
class DataArchiverResult:
    sub_uuid: str
    success: bool = field(default=True)
    error: str = field(default=None)
    files: List[FileResult] = field(default_factory=list)

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
