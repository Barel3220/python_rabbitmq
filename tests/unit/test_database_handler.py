import os
import pandas
import unittest
from src.database_handler import DatabaseHandler

database_path = os.path.normpath(os.path.pardir + os.path.join('/dummy_database/dummy.db'))
table_name = "invoices_dummy"


class TestDatabaseHandler(unittest.TestCase):
    database_connection = None

    @classmethod
    def setUpClass(cls):
        """
        setting up (once) the database_handler object
        """
        cls.database_connection = DatabaseHandler(database_path)
        setupclass_create_table(cls.database_connection)

    def setUp(self):
        """
        setting up a database_handler object with the dummy database path
        and connecting it
        """
        self.database_connection.connect()

    def tearDown(self):
        """
        tearing down the table, and closing the connection
        """
        # in case one of the methods closed the connection
        self.database_connection.connect()
        self.database_connection.clear_table('''DELETE FROM ''' + table_name)
        self.database_connection.close()

    def test_select(self):
        """
        1. selecting count of rows to check correct
        2. selecting specific value to check correct
        3. checking the type of the value from 2 is the correct one
        4. raising an exception due to typeerror,
                the sqlite3 exception is thrown and caught in database handler
        """
        insert_dummy_data(self.database_connection)
        results = self.database_connection.select('''SELECT COUNT(*) FROM ''' + table_name)[0][0]
        # 1
        self.assertEqual(results, 3)
        self.database_connection.connect()
        results = self.database_connection.select('''SELECT Total FROM ''' + table_name
                                                  + ''' WHERE InvoiceId==1''')[0][0]
        # 2
        self.assertEqual(results, 2825.3)
        # 3
        self.assertIs(type(results), float)
        self.database_connection.connect()
        # 4
        # 'int' object is not subscriptable
        with self.assertRaises(TypeError):
            results = self.database_connection.select('''SELECT Total FROM ''' + table_name
                                                      + '''WHERE InvoiceId == 1''')[0][0]
            # print just for the purpose of using the variable 'results'
            print(results)

    def test_insert_many(self):
        """
        1. inserting good data and expecting 3 rows
        2. inserting bad data, the exception is caught and -1 is returned from database_handler
        """
        query, data = set_insert_many()
        results = self.database_connection.insert_many(query, data)
        # 1
        self.assertEqual(results, "Inserted 3 Records")
        self.database_connection.connect()
        query = set_bad_insert_many()
        results = self.database_connection.insert_many(query, data)
        # 2
        self.assertNotEqual(results, "Inserted 3 Records")

    def test_to_dataframe(self):
        """
        1. inserting good data and trying to retrieve some columns and turn it into dataframe
        2. checking type of dataframe is indeed pandas.Dataframe
        3. creating exception which caught and checking results is not equal to 3 (is -1)
        """
        insert_dummy_data(self.database_connection)
        headers = ['CustomerId', 'InvoiceDate', 'Total']
        dataframe = self.database_connection.to_dataframe(headers, table_name)
        # 1
        self.assertEqual(len(dataframe), 3)
        # 2
        self.assertIs(type(dataframe), pandas.DataFrame)
        headers = ['CustomId', 'InvoiceDate', 'Total']
        dataframe = self.database_connection.to_dataframe(headers, table_name)
        # 3
        self.assertNotEqual(dataframe, 3)

    def test_create_table(self):
        """
        1. trying to create a dummy table
        2. trying to create again same table, exception get caught
        """
        results = self.database_connection.create_table(create_table_query())
        # 1
        self.assertEqual(results, "1 Table Created Successfully")
        results = self.database_connection.create_table(create_table_query())
        # 2
        self.assertNotEqual(results, "1 Table Created Successfully")
        self.database_connection.clear_table('''DROP TABLE IF EXISTS dummy''')

    def test_clear_table(self):
        """
        1. inserting dummy data. then trying to delete all rows
        2. trying to delete all rows with syntax error, exception get caught
        """
        insert_dummy_data(self.database_connection)
        results = self.database_connection.clear_table('''DELETE FROM ''' + table_name)
        # 1
        self.assertEqual(results, "Deleted All Rows")
        insert_dummy_data(self.database_connection)
        results = self.database_connection.clear_table('''DELETE FROM''' + table_name)
        # 2
        self.assertNotEqual(results, "Deleted All Rows")

    @classmethod
    def tearDownClass(cls):
        """
        dropping table at the end of all tests
        """
        cls.database_connection.connect()
        cls.database_connection.clear_table('''DROP TABLE ''' + table_name)
        cls.database_connection.close()


# setup methods
def create_table_query():
    return '''CREATE TABLE dummy 
        (InvoiceId INTEGER, CustomerId INTEGER,
        InvoiceDate TEXT, Total FLOAT)'''


def setupclass_create_table(database_connection):
    """
    creating a table for all database testing
    :param database_connection: connection for connect, create and close
    """
    database_connection.connect()
    database_connection.create_table('''CREATE TABLE IF NOT EXISTS ''' + table_name + ''' (
        InvoiceId INTEGER, CustomerId INTEGER, InvoiceDate TEXT, Total FLOAT,
        UNIQUE (InvoiceId, CustomerId, InvoiceDate, Total) ON CONFLICT IGNORE)''')
    database_connection.close()


def insert_dummy_data(database_connection):
    """
    creating dummy data and inserting it to database for selecting it inn the test,
    connecting to the database after insertion to due closing inside
    :param database_connection: connection for connect, insert and open again
    """
    query = '''INSERT INTO ''' + table_name + ''' VALUES (?, ?, ?, ?)'''
    data = [[1, 23, '2009-01', 2825.3],
            [2, 54, '2009-02', 4266.3],
            [3, 27, '2009-05', 6221.3]]
    database_connection.insert_many(query, data)
    database_connection.connect()


def set_insert_many():
    """
    creating dummy data and returning it for testing the insertion
    """
    query = '''INSERT INTO ''' + table_name + ''' VALUES (?, ?, ?, ?)'''
    data = [[1, 23, '2009-01', 2825.3],
            [2, 54, '2009-02', 4266.3],
            [3, 27, '2009-05', 6221.3]]
    return query, data


def set_bad_insert_many():
    """
    creating bad dummy data and returning it for testing the insertion
    """
    return '''INSERT INTO ''' + table_name + '''VALUES (?, ?, ?, ?)'''


if __name__ == '__main__':
    unittest.main()
