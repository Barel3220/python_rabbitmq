import os
import pandas
import sqlite3

database_path = os.path.abspath(os.path.dirname(__file__) + os.path.join('/database/invoices.db'))


class DatabaseHandler:
    def __init__(self, db_path):
        """
        initiating the database object, no database yet
        :type db_path: str
        """
        self.path = db_path
        self.connection = None

    def get_path(self):
        """
        :return: path of the database
        """
        return self.path

    def connect(self):
        """
        trying to connect to the database
        :return: -1 in case of exception
        """
        try:
            self.connection = sqlite3.connect(self.path)
            print("Database Connection Established")
            return self.connection
        except sqlite3.Error as e:
            print("Connection failed: ", e.args[0])
            return -1

    def close(self):
        """
        checking if a connection is already existing, then if so, closing it
        """
        if self.connection:
            self.connection.close()
            print("Database Connection Closed")

    def select(self, query: str):
        """
        getting a cursor, executing the query, closing the connection then returning the data
        :param query: select query from the database
        :return: list
        """
        cursor = self.connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        print(f"Extracted {len(results)} Records from the Database")
        self.close()
        return results

    def insert_many(self, query: str, data):
        """
        getting a cursor, executing the query, committing the data,
        closing the connection then returning the data
        :param query: insert query, already with the table name embedded in it
        :param data: iterable for the executemany function
        :type data: list/_reader,
        :return:
        """
        cursor = self.connection.cursor()
        cursor.executemany(query, data)
        results = cursor.fetchall()
        self.connection.commit()
        print(f"Inserted {len(results)} Records to the Database")
        self.close()
        return results

    def to_dataframe(self, headers: list, db_name: str):
        """
        getting a list of str of the column names, then returning the dataframe from the pandas method
        :param headers: list of the columns the user want to get back
        :param db_name: table name
        :return: pandas.Dataframe
        """
        str_from_headers = ', '.join(headers)
        query = '''SELECT ''' + str_from_headers + ''' FROM ''' + db_name
        return pandas.read_sql_query(query, self.connection)

    def create_table(self, query: str):
        """
        creating table in the database
        :param query: create query with the table name embedded
        """
        cursor = self.connection.cursor()
        cursor.execute(query)
        print("1 Table Created Successfully")

    def clear_table(self, query: str):
        """
        clearing all rows from table in database
        :param query: delete query with table name embedded
        """
        cursor = self.connection.cursor()
        cursor.execute(query)
        print("Deleted All Rows")
