import os
import csv
import pandas
import plotly.graph_objs as go
from plotly.subplots import make_subplots
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
    create_table_if_not_exist(database_connection, db_name)

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


def create_table_if_not_exist(database_connection, db_name):
    query = '''CREATE TABLE IF NOT EXISTS ''' + db_name + ''' (InvoiceId integer,
            CustomerId integer, InvoiceDate text, BillingAddress text,
            BillingCity text, BillingState text, BillingCountry text,
            BillingPostalCode text, Total float,
            UNIQUE (InvoiceId, CustomerId, InvoiceDate, BillingAddress, BillingCity, BillingState,
                    BillingCountry, BillingPostalCode, Total) ON CONFLICT IGNORE)'''
    database_connection.create_table(query)


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


def build_graph(graph_dataframe):
    """
    getting the dataframe from graph_consumer and
    extracting the columns to present them properly in browser
    :param graph_dataframe: dataframe['InvoiceDate', 'Count', 'Total']
    """
    dates = graph_dataframe['InvoiceDate'].tolist()
    counts = graph_dataframe['Count'].tolist()
    totals = graph_dataframe['Total'].tolist()

    figure = make_subplots(rows=2, cols=1,
                           subplot_titles=("Totals per Month", "Counts per New Customers"),
                           shared_xaxes=True)
    figure.append_trace(go.Scatter(x=dates, y=totals, name="Totals"), row=1, col=1)
    figure.append_trace(go.Bar(x=dates, y=counts, name="Counts"), row=2, col=1)
    figure.update_layout(xaxis=dict(tickmode='array', tickvals=dates, ticktext=dates),
                         xaxis2=dict(tickmode='array', tickvals=dates, ticktext=dates))

    figure.write_html(os.path.abspath(os.path.dirname(__file__) +
                                      os.path.join('/database/figure.html')), auto_open=True)
