import os
import unittest
from src.main import main
from src.producer import Producer
base_path = os.path.normpath(os.path.dirname(__file__) + os.path.join('/dummy_files'))
files = [[base_path + os.path.join('/invoices_2011.csv'), "csv", "invoices"],
         [base_path + os.path.join('/invoices_2009.json'), "json", "invoices"]]


class TestMain(unittest.TestCase):
    producer = None

    @classmethod
    def setUpClass(cls):
        """
        setting up (once) the producer object
        """
        cls.producer = Producer()
        cls.producer.declare()

    def test_main(self):
        """
        1. testing the whole main run, checking all parameters and expecting All Done
        2. testing the main run with a bad file path (skipping one)
        3. testing the main run with a bad file type (skipping one)
        4. testing the main run with a bad table name (closing run)
        """
        results = main(0.1, files)
        # 1
        self.assertEqual(results, "All Done Successfully")
        results = main(0.1, get_files_bad_file_path())
        # 2
        self.assertIn("skipping to next", results)
        results = main(0.1, get_files_bad_type())
        # 3
        self.assertIn("skipping to next", results)
        results = main(0.1, get_files_bad_name_table())
        # 4
        self.assertIn("closing app. . .", results)

    @classmethod
    def tearDownClass(cls):
        """
        clearing the queue out and closing the producer channel
        """
        cls.producer.channel.queue_purge(queue='files_to_database')
        cls.producer.channel.close()


def get_files_bad_type():
    _files = files.copy()
    _files[0][1] += "3"
    return _files


def get_files_bad_file_path():
    _files = files.copy()
    _files[0][0] += "23"
    return _files


def get_files_bad_name_table():
    _files = files.copy()
    _files[0][2] += "31"
    return _files




