import unittest
import warnings
from data.archiver.stream import S3FTPStreamer
from data.archiver.dataclass import DataArchiverResult, FileResult

class TestArchiver(unittest.TestCase):

    def setUp(self):
        #warnings.filterwarnings("ignore", category=ResourceWarning, message="unclosed.*<ssl.SSLSocket.*>")
        self.streamer = S3FTPStreamer()
        super().setUp()

    def _test_streamer(self):
        small = FileResult("4861STDY7771115.mtx", "s3://org-hca-data-archive-upload-dev/bc9fbf9e-1d00-4605-aa39-008cb56f5d2e/4861STDY7771115.mtx") # 82.5MB
        smaller = FileResult("MRC_Endo8715415.mtx", "s3://org-hca-data-archive-upload-dev/bc9fbf9e-1d00-4605-aa39-008cb56f5d2e/MRC_Endo8715415.mtx") # 17.1MB
        smallest = FileResult("4861STDY7771123_cells.tsv", "s3://org-hca-data-archive-upload-dev/bc9fbf9e-1d00-4605-aa39-008cb56f5d2e/4861STDY7771123_cells.tsv") # 1.8MB

        res = DataArchiverResult("uuid", True, None, [small, smaller, smallest])
        S3FTPStreamer().start(res)
        ## TODO assertions

    def tearDown(self):
        self.streamer.close()

if __name__ == '__main__':
    unittest.main()