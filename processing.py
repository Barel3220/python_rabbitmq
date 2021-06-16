import csv
import pandas
from database_handler import DatabaseHandler, database_path


def process_file(file_path: str, file_type: str, db_name: str):
    """
    getting the file path, file type, and table name
    opening the file then inserting all the data into the database
    :param file_path: filepath normal to the operating system
    :param file_type: CSV/JSON
    :param db_name: table name
    """
    file = open(file_path)

    database_connection = DatabaseHandler(db_path=database_path)
    database_connection.connect()

    insert_many_query = '''INSERT INTO ''' + db_name + ''' VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'''

    if "JSON" in file_type:
        dataframe = pandas.read_json(file)

        # rearranging the columns to match the rest of the files
        dataframe = dataframe[['InvoiceId', 'CustomerId', 'InvoiceDate',
                               'BillingAddress', 'BillingCity', 'BillingState',
                               'BillingCountry', 'BillingPostalCode', 'Total']]

        database_connection.insert_many(insert_many_query, dataframe.values.tolist())

    elif "CSV" in file_type:
        csv_reader = csv.reader(file, delimiter=',')

        # skipping the first row (headers)
        next(csv_reader)

        database_connection.insert_many(insert_many_query, csv_reader)

    # TODO: append to database and make only one global table

    # printing
    database_connection.connect()
    results = database_connection.select('''SELECT * FROM ''' + db_name)
    for item in results:
        print(item)


def build_dataframe(db_name):
    """
    establishing connection to database, and requesting dataframe of the data
    then processing it and preparing it for the graph
    :param db_name: database name for the query
    :type db_name: str
    """
    database_connection = DatabaseHandler(db_path=database_path)
    database_connection.connect()
    dataframe = database_connection.to_dataframe(['CustomerId', 'InvoiceDate', 'Total'], db_name)
    database_connection.close()

    dataframe.loc[:, 'InvoiceDate'] = pandas.to_datetime(dataframe['InvoiceDate']).dt.strftime('%Y-%m')

    analyze_dataframe = dataframe.copy()
    total_sum_dataframe = analyze_dataframe.groupby(['InvoiceDate'])['Total'].sum()

    customer_count_dataframe = analyze_dataframe.drop_duplicates(subset=['CustomerId', 'InvoiceDate'])
    customer_count_dataframe = customer_count_dataframe.groupby(['InvoiceDate']).size().reset_index(name="Count")

    return customer_count_dataframe.merge(total_sum_dataframe, how='inner', on='InvoiceDate')
