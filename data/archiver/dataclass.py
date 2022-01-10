from typing import List, Dict
from dataclasses import dataclass


class DataArchiverRequestParseExpection(Exception):
    pass

@dataclass
class DataArchiverRequest:
    sub_uuid: str
    files: List[str]

    @staticmethod
    def from_dict(data: Dict) -> 'DataArchiverRequest':
        try:
            return DataArchiverRequest(data["sub_uuid"],
                                     data["files"] if "files" in data else [])
        except (KeyError, TypeError) as e:
            print(e)
            raise DataArchiverRequestParseExpection(e)


@dataclass
class FileResult:
    file_name: str
    md5: str
    success: bool


@dataclass
class DataArchiverResult:
    sub_uuid: str
    success: bool
    files: List[FileResult]