import os
import numpy
import pandas
import unittest
import src.processing as processing
from src.database_handler import DatabaseHandler

database_path = os.path.normpath(os.path.pardir + os.path.join('/dummy_database/dummy.db'))
figure_path = os.path.normpath(os.path.pardir + os.path.join('/dummy_database/dummy_figure.html'))
table_name = "invoices_dummy"
base_path = os.path.normpath(os.path.dirname(__file__) + os.path.join('/dummy_files'))
files = [[base_path + os.path.join('/invoices_2009.json'), "json", "invoices"],
         [base_path + os.path.join('/invoices_2011.csv'), "csv", "invoices"],
         [base_path + os.path.join('/bad_invoices_2009.json'), "json", "invoices"]]


class TestProcessing(unittest.TestCase):
    database_connection = None

    @classmethod
    def setUpClass(cls):
        """
        setting up (once) the database_handler object
        """
        cls.database_connection = DatabaseHandler(database_path)
        cls.database_connection.connect()
        processing.create_table_if_not_exist(cls.database_connection, table_name)
        cls.database_connection.close()

    def setUp(self):
        """
        adding an assertion for testing dataframe equality
        setting up a database_handler object with the dummy database path
        and connecting it
        """
        self.addTypeEqualityFunc(pandas.DataFrame, self.assertDataframeEqual)
        self.database_connection.connect()

    def tearDown(self):
        """
        tearing down the table, and closing the connection
        """
        # in case one of the methods closed the connection
        self.database_connection.connect()
        self.database_connection.clear_table('''DELETE FROM ''' + table_name)
        self.database_connection.close()

    def test_process_file(self):
        """
        data is getting here after already tested in main (path, type, table_name)
        1. testing process_file process for json, establishing, creating, reading file, sorting, inserting
        2. testing correct amount in database of first insertion (4)
        3. testing process_file process for csv, establishing, creating, reading file, sorting, inserting
        4. testing correct amount in database of second insertion (8)
        5. trying to open a file with the wrong type, database_handler is catching the exception
        """
        # 1
        self.assertEqual(get_file_reply(files[0][0], files[0][1]), "Inserted 4 Records")
        results = self.database_connection.select('''SELECT COUNT(*) FROM ''' + table_name)[0][0]
        # 2
        self.assertEqual(results, 4)
        # csv, renewing connection
        self.database_connection.connect()
        # 3
        self.assertEqual(get_file_reply(files[1][0], files[1][1]), "Inserted 4 Records")
        results = self.database_connection.select('''SELECT COUNT(*) FROM ''' + table_name)[0][0]
        # 4
        self.assertEqual(results, 8)
        self.database_connection.connect()
        # 5
        self.assertFalse(get_file_reply(files[0][0], files[1][1]))

    def test_build_dataframe(self):
        """
        1.2.3 checking data type is correct
        4. testing sum of columns is correct
        5. (added assertion) testing dataframes are the same, one manufactured
        6. clearing the database and inserting altered data - CustomerId duplicate, checking difference in Count (3-4)
        7. testing raising an exception of AssertionError with dataframe and alt_dataframe
        8. testing Sum of both alt_dataframe and dataframe are the same
        """
        insert_good_data()
        dataframe = get_dataframe()
        # 1 2 3
        self.assertIs(type(dataframe['Total'][0]), numpy.float64)
        self.assertIs(type(dataframe['InvoiceDate'][0]), str)
        self.assertIs(type(dataframe['Count'][0]), numpy.int64)
        # 4
        self.assertEqual(dataframe['Total'][0], 8198.79)
        # 5
        self.assertDataframeEqual(dataframe, get_equal_dataframe())
        alt_dataframe = get_alter_dataframe(self.database_connection)
        # 6
        self.assertNotEqual(alt_dataframe['Count'][0], dataframe['Count'][0])
        # 7
        with self.assertRaises(AssertionError):
            self.assertDataframeEqual(alt_dataframe, dataframe)
        # 8
        self.assertEqual(dataframe['Total'][0], alt_dataframe['Total'][0])

    def test_build_graph(self):
        """
        1. testing the build_graph method returns the correct string, and waiting for file to open (less than 1 sec)
        """
        insert_good_data()
        dataframe = get_dataframe()
        results = processing.build_graph(dataframe, figure_path, False)
        # 1
        self.assertEqual(results, "Updated html File and Opened it")

    def assertDataframeEqual(self, df1, df2, msg='Dataframes are NOT equal'):
        """
        adding assertion test to this class
        :param df1: first dataframe
        :param df2: second dataframe
        :param msg: error message which will be displayed in case of exception
        """
        try:
            pandas.testing.assert_frame_equal(df1, df2)
        except AssertionError as e:
            raise self.failureException(msg) from e

    @classmethod
    def tearDownClass(cls):
        """
        dropping table at the end of all tests
        """
        cls.database_connection.connect()
        cls.database_connection.clear_table('''DROP TABLE ''' + table_name)
        cls.database_connection.close()


def get_equal_dataframe():
    data = {
        'InvoiceDate': ['2009-01', '2012-01'],
        'Count': [4, 4],
        'Total': [8198.79, 5323.15]
    }
    return pandas.DataFrame(data, columns=['InvoiceDate', 'Count', 'Total'])


def get_file_reply(file_path, file_type):
    database_connection = processing.establish_connection(database_path)
    processing.create_table_if_not_exist(database_connection, table_name)
    insert_many_query = processing.get_insert_many_query(table_name)
    dataframe = None
    try:
        if "json" in file_type:
            dataframe = pandas.read_json(file_path)
        elif "csv" in file_type:
            dataframe = pandas.read_csv(file_path)
    except pandas.errors.ParserError as e:
        print("Insertion failed: ", e.args[0])
        return 0
    dataframe = processing.order_headers(dataframe)
    return database_connection.insert_many(insert_many_query, dataframe.values.tolist())


def get_dataframe():
    database_connection = processing.establish_connection(database_path)
    dataframe = database_connection.to_dataframe(['CustomerId', 'InvoiceDate', 'Total'], table_name)
    database_connection.close()
    dataframe = processing.get_invoice_date_fixed(dataframe)
    analyze_dataframe = dataframe.copy()
    total_sum_dataframe = processing.get_column_sum(analyze_dataframe)

    customer_count_dataframe = processing.drop_duplicates(analyze_dataframe)
    customer_count_dataframe = processing.get_column_count(customer_count_dataframe)
    return customer_count_dataframe.merge(total_sum_dataframe, how='inner', on='InvoiceDate')


def insert_good_data():
    get_file_reply(files[0][0], files[0][1])
    get_file_reply(files[1][0], files[1][1])


def insert_bad_data():
    get_file_reply(files[2][0], files[2][1])


def get_alter_dataframe(database_connection):
    # clearing for bad insertion
    database_connection.clear_table('''DELETE FROM ''' + table_name)
    insert_bad_data()
    return get_dataframe()
