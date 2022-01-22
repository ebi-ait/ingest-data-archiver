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
            print(e)
            raise DataArchiverRequestParseExpection(e)


@dataclass
class FileResult:
    file_name: str
    cloud_url: str
    size: int
    md5: str
    success: bool
    error: str

    def __init__(self, file_name, cloud_url, size=0, md5=None, success=True, error=None):
        self.file_name = file_name
        self.cloud_url = cloud_url
        self.size = size
        self.md5 = md5
        self.success = success
        self.error = error


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
