import os
import unittest
from src.main import create_metadata, check_path, check_type, check_same_table_list


def get_path_list() -> list:
    return [["C:/Users/barel/Desktop/Files/invoices_2009.json", "JSON", "invoices"],
            ["C:/Users/barel/Desktop/Files/invoices_2010.json", "JSON", "invoices"],
            ["C:/Users/barel/Desktop/Files/invoices_2011.json", "JSON", "invoices"],
            ["C:/Users/barel/Desktop/Files/invoices_2012.csv", "CSV", "invoices"],
            ["C:/Users/barel/Desktop/Files/invoices_2013.csv", "CSV", "invoices"]]


def get_bad_path_list() -> list:
    # hurting the data with random string
    return [["C:/Users/barel/Desktop/Files/invoices_2009.json", "JSON", "invoice"],
            ["C:/Users/barel/Desktop/Files/invoices_2010.json", "JSON", "invoices"],
            ["C:/Users/barel/Desktop/Files/invoices_2011.json", "JSON", "invoices"],
            ["C:/Users/barel/Desktop/Files/invoices_2012.csv", "CSV", "invoice"],
            ["C:/Users/barel/Desktop/Files/invoices_2013.csv", "CSV", "invoices"]]


class TestMain(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """
        creating 1 good list of items and 1 good path
        """
        cls.data = ["C:/Users/barel/Desktop/Files/invoices_2009.json", "json", "invoices"]
        cls.path = os.path.normpath(cls.data[0])

    def test_create_metadata(self):
        """
        checking the results of create_metadata is indeed bytes
        """
        results = create_metadata(self.data[0], self.data[1], self.data[2])
        self.assertIs(type(results), bytes)

    def test_check_type(self):
        """
        1. obtaining a bad list of items and checking the return is false
        2. same with good list to get true
        """
        bad_data = get_bad_type(self.data.copy())
        self.assertFalse(check_type(bad_data[0], bad_data[1]))
        self.assertTrue(check_type(self.data[0], self.data[1]))

    def test_check_path(self):
        """
        1. obtaining a bad path and checking the return is false
        2. same with good path to get true
        """
        bad_data = get_bad_path(self.path)
        self.assertFalse(check_path(bad_data))
        self.assertTrue(check_path(self.path))

    def test_check_same_table_name(self):
        """
        1. obtaining a bad list of lists and checking the return is false
        2. same with good list of lists to get true
        """
        self.assertTrue(check_same_table_list(get_path_list()))
        self.assertFalse(check_same_table_list(get_bad_path_list()))


# setup methods
def get_bad_type(item: list) -> list:
    # hurting the data with random string
    item[1] += "3"
    return item


def get_bad_path(item: str) -> str:
    # hurting the data with random string
    item += "21"
    return item


if __name__ == '__main__':
    unittest.main()
