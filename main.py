from producer import Producer
import os


def main():
    """
    holding a list of bytes including 'path', 'type' and 'table_name'
    to activate the first module - Producer
    """
    producer = Producer()

    list_of_files = [
        b"C:/Users/barel/Desktop/Files/invoices_2009.json JSON invoices_2009",
        b"C:/Users/barel/Desktop/Files/invoices_2010.json JSON invoices_2010",
        b"C:/Users/barel/Desktop/Files/invoices_2011.json JSON invoices_2011",
        b"C:/Users/barel/Desktop/Files/invoices_2012.csv CSV invoices_2012",
        b"C:/Users/barel/Desktop/Files/invoices_2013.csv CSV invoices_2013"
    ]

    producer.declare()
    producer.publish(create_metadata("C:/Users/barel/Desktop/Files/invoices_2012.csv", "CSV", "invoices_2012"))
    producer.close()


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
