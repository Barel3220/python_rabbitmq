import os
import time
from producer import Producer
from database_handler import database_path


def main():
    """
    holding a list of bytes including 'path', 'type' and 'table_name'
    to activate the first module - Producer
    """
    print(database_path)
    producer = Producer()

    string_bytes_list = [
        ["C:/Users/barel/Desktop/Files/invoices_2009.json", "JSON", "invoices"],
        ["C:/Users/barel/Desktop/Files/invoices_2010.json", "JSON", "invoices"],
        ["C:/Users/barel/Desktop/Files/invoices_2011.json", "JSON", "invoices"],
        ["C:/Users/barel/Desktop/Files/invoices_2012.csv", "CSV", "invoices"],
        ["C:/Users/barel/Desktop/Files/invoices_2013.csv", "CSV", "invoices"]
    ]

    producer.declare()
    for item in string_bytes_list:
        if check_path(item[0]):
            producer.publish(create_metadata(item[0], item[1], item[2]))
            # waiting 4 seconds between runs - to see the update in progress
            time.sleep(4)
        else:
            print(f"{item[0]} - is not a valid path, skipping to next")
    producer.close()


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
    main()
