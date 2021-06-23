import os
import time
from src.producer import Producer


def main():
    """
    holding a list of bytes including 'path', 'type' and 'table_name'
    to activate the first module - Producer
    """
    producer = Producer()

    path_list = [
        ["C:/Users/barel/Desktop/Files/invoices_2009.json", "JSON", "invoices"],
        ["C:/Users/barel/Desktop/Files/invoices_2010.json", "JSON", "invoices"],
        ["C:/Users/barel/Desktop/Files/invoices_2011.json", "JSON", "invoices"],
        ["C:/Users/barel/Desktop/Files/invoices_2012.csv", "CSV", "invoices"],
        ["C:/Users/barel/Desktop/Files/invoices_2013.csv", "CSV", "invoices"]
    ]

    if check_same_table_list(path_list):
        producer.declare()
        for item in path_list:
            if check_path(item[0]):
                if check_type(item[0], item[1]):
                    producer.publish(create_metadata(item[0], item[1], item[2]))
                    # waiting 4 seconds between runs - to see the update in progress
                    time.sleep(4)
                else:
                    print(f"{item[0].split('.')[-1]}, {item[1].lower()} "
                          f"- there is differences between file types, skipping to next")
            else:
                print(f"{item[0]} - is not a valid path, skipping to next")
        producer.close()
    else:
        print("Not all tables in list are the same as should, closing app. . .")
        return

    return "All Done Successfully"


def check_same_table_list(path_list: list) -> bool:
    """
    checking inside list that all table names are the same
    :param path_list: list of path, type, table_name
    :return: boolean - true if all table names are the same
    """
    return [item[-1] for item in path_list].count(path_list[0][-1]) == len(path_list)


def check_type(path: str, file_type: str) -> bool:
    """
    checking if the file type is corresponding to the data fed
    :param path: file type (.json/.csv)
    :param file_type: JSON/CSV
    :return: boolean - true if the same as the file
    """
    return path.split(".")[-1] == file_type.lower()


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
