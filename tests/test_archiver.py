import unittest
import warnings
from data.archiver.archiver import Archiver
from data.archiver.dataclass import DataArchiverRequest

class TestArchiver(unittest.TestCase):

    def setUp(self):
        #warnings.filterwarnings("ignore", category=ResourceWarning, message="unclosed.*<ssl.SSLSocket.*>")
        self.archiver = Archiver()
        super().setUp()

    def test_archive(self):
        req = DataArchiverRequest.from_dict({"sub_uuid": "043f8f2f-1e99-434f-9a3c-dc14441e8a66", "files": []})
        self.archiver.start(req)
        ## TODO assertions

    def tearDown(self):
        self.archiver.close()

if __name__ == '__main__':
    unittest.main()