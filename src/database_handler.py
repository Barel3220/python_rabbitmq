import os
import pandas
import sqlite3
database_path = os.path.normpath(os.path.dirname(__file__) + os.path.join('/database/invoices.db'))


class DatabaseHandler:
    def __init__(self, db_path: str):
        """
        initiating the database object
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
            return 0

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
        :return: list, 0 in case of exception
        """
        try:
            cursor = self.connection.cursor()
            results = cursor.execute(query).fetchall()
            print(f"Extracted {len(results)} Records from the Database")
            self.close()
            return results
        except sqlite3.OperationalError as e:
            print("Selection failed: ", e.args[0])
            return 0

    def insert_many(self, query: str, data: list):
        """
        using 'with' clause, executing the query, (data is committed by 'with'),
        closing the connection then returning finished string
        :param query: insert query, already with the table name embedded in it
        :param data: iterable for the executemany function
        :return: str, 0 in case of exception
        """
        try:
            # if the 'with' block succeed the data is committed
            # otherwise a rollback is called
            with self.connection as cursor:
                cursor.executemany(query, data)
            print(f"Inserted {len(data)} Records to the Database")
            self.close()
            return f"Inserted {len(data)} Records"
        except sqlite3.OperationalError as e:
            print("Insertion failed: ", e.args[0])
            return 0

    def to_dataframe(self, headers: list, table_name: str):
        """
        getting a string list of column names, then returning the dataframe from the pandas method
        :param headers: list of the columns the user want to get back
        :param table_name: table name
        :return: pandas.Dataframe, 0 in case of exception
        """
        try:
            str_from_headers = ', '.join(headers)
            query = '''SELECT ''' + str_from_headers + ''' FROM ''' + table_name
            return pandas.read_sql_query(query, self.connection)
        except pandas.io.sql.DatabaseError as e:
            print("pandas.read_sql_query failed: ", e.args[0])
            return 0

    def create_table(self, query: str):
        """
        creating table in the database
        :param query: create query with the table name embedded
        :return: str, 0 in case of exception
        """
        try:
            with self.connection as cursor:
                cursor.execute(query)
            return "1 Table Created Successfully"
        except sqlite3.OperationalError as e:
            print("Creation failed: ", e.args[0])
            return 0

    def clear_table(self, query: str):
        """
        clearing all rows from table in database
        :param query: delete query with table name embedded
        :return: str, 0 in case of exception
        """
        try:
            with self.connection as cursor:
                cursor.execute(query)
            return "Deleted All Rows"
        except sqlite3.OperationalError as e:
            print("Deletion failed: ", e.args[0])
            return 0
