import os
import time
import shutil
from src.producer import Producer
from src.database_handler import database_path
path_list = [
        ["C:/Users/barel/Desktop/Files/invoices_2009.json", "json", "invoices"],
        ["C:/Users/barel/Desktop/Files/invoices_2010.json", "json", "invoices"],
        ["C:/Users/barel/Desktop/Files/invoices_2011.json", "json", "invoices"],
        ["C:/Users/barel/Desktop/Files/invoices_2012.csv", "csv", "invoices"],
        ["C:/Users/barel/Desktop/Files/invoices_2013.csv", "csv", "invoices"]
    ]


def main(sleep_amount: float, _path_list: list) -> str:
    """
    passed a list of bytes including 'path', 'type' and 'table_name'
    to activate the first module - Producer
    :param sleep_amount: number of seconds to sleep between iterations
    :param _path_list: strings list of lists
    :return: str
    """

    producer = Producer()

    if check_same_table_list(_path_list):
        producer.declare()
        for item in _path_list:
            if check_path(item[0]):
                if check_type(item[0], item[1]):
                    producer.publish(create_metadata(item[0], item[1], item[2]))
                    # waiting 4 seconds between runs - to see the update in progress
                    if item != _path_list[-1]:
                        time.sleep(sleep_amount)
                else:
                    return f"{item[0].split('.')[-1]}, {item[1].lower()} - " \
                           f"there is differences between file types, skipping to next"
            else:
                return f"{item[0]} - is not a valid path, skipping to next"
        producer.close()
    else:
        return "Not all tables in list are the same as should, closing app. . ."
    # cleanup()
    return "All Done Successfully"


# Cleanup function:
# ----------------
# This cleanup supposed to clean the database assets in order to
# keep the developers environment clean, I commented this function due to concurrency issue.

# The database cursor is a sub-process that is using the database during the run, and in this function we
# are deleting the database directory after the run, the reason sleep() is needed is because
# there's a "race" between the cleanup() and the cursor.close()
# def cleanup():
#     time.sleep(6)
#     shutil.rmtree(os.path.dirname(database_path))


def check_same_table_list(_path_list: list) -> bool:
    """
    checking inside list that all table names are the same
    :param _path_list: list of path, type, table_name
    :return: boolean - true if all table names are the same
    """
    return [item[-1] for item in _path_list].count(_path_list[0][-1]) == len(_path_list)


def check_type(path: str, file_type: str) -> bool:
    """
    checking if the file type is corresponding to the data fed
    :param path: file type (.json/.csv)
    :param file_type: JSON/CSV
    :return: boolean - true if the same as the file
    """
    return path.split(".")[-1] == file_type


def check_path(path: str) -> bool:
    """
    checking if the filepath is a real path
    :param path: filepath
    :return: boolean - true if filepath is valid
    """
    return os.path.isfile(path)


def create_metadata(path: str, file_type: str, name: str) -> bytes:
    """
    creating bytes strings with the os module for the file path
    using the os module and the normpath method for matching all operating systems
    :param path: string of the file path
    :param file_type: CSV/JSON
    :param name: name of the table in the database
    :return: string bytes of combined variables
    """
    return bytes(f'{os.path.normpath(path)} {file_type} {name}', encoding='utf-8')


if __name__ == '__main__':
    main(4, path_list)
