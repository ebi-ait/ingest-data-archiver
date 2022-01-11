from typing import List, Dict
from dataclasses import dataclass
import json


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
    error: str


@dataclass
class DataArchiverResult:
    sub_uuid: str
    success: bool
    error: str
    files: List[FileResult]

    @staticmethod
    def empty(req: DataArchiverRequest):
        res_files = []
        if req.files:
            for file in req.files:
                res_files.append(FileResult(file, None, True, None))
        return DataArchiverResult(req.sub_uuid, True, None, res_files)

    def update_file_result(self, file_name, md5, success, error):
        for file in self.files:
            if file.file_name == file_name:
                file.md5 = md5
                file.success = success
                file.error = error
                break

    @staticmethod
    def error_res(req, err):
        return DataArchiverResult(req.sub_uuid, False, err, [])

    def to_dict(self):
        return json.loads(json.dumps(self, default=lambda o: o.__dict__))
