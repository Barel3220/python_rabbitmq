import os
import pandas
import plotly.graph_objs as go
from plotly.subplots import make_subplots
from src.database_handler import DatabaseHandler, database_path
figure_path = os.path.normpath(os.path.dirname(__file__) + os.path.join('/database/figure.html'))


def process_file(file_path: str, file_type: str, table_name: str):
    """
    getting the file path, file type, and table name
    opening the file then inserting all the data into the database
    :param file_path: filepath normal to the operating system
    :param file_type: CSV/JSON
    :param table_name: table name
    """
    database_connection = establish_connection(database_path)
    create_table_if_not_exist(database_connection, table_name)

    insert_many_query = get_insert_many_query(table_name)

    dataframe = None
    if "JSON" == file_type:
        dataframe = pandas.read_json(file_path)

    elif "CSV" == file_type:
        dataframe = pandas.read_csv(file_path)

    dataframe = order_headers(dataframe)
    results = database_connection.insert_many(insert_many_query, dataframe.values.tolist())

    database_connection.close()
    return results


def build_dataframe(table_name):
    """
    establishing connection to database, and requesting dataframe of the data
    then processing it and preparing it for the graph
    :param table_name: database name for the query
    :type table_name: str
    """
    database_connection = establish_connection(database_path)
    dataframe = database_connection.to_dataframe(['CustomerId', 'InvoiceDate', 'Total'], table_name)
    database_connection.close()

    dataframe = get_invoice_date_fixed(dataframe)

    analyze_dataframe = dataframe.copy()
    total_sum_dataframe = get_column_sum(analyze_dataframe)

    customer_count_dataframe = drop_duplicates(analyze_dataframe)
    customer_count_dataframe = get_column_count(customer_count_dataframe)

    return customer_count_dataframe.merge(total_sum_dataframe, how='inner', on='InvoiceDate')


def build_graph(graph_dataframe, _figure_path):
    """
    getting the dataframe from graph_consumer and
    extracting the columns to present them properly in browser
    :param _figure_path: path to figure file in database folder
    :param graph_dataframe: dataframe['InvoiceDate', 'Count', 'Total']
    """
    dates, counts, totals = get_columns(graph_dataframe)
    figure = get_figure(dates, counts, totals)
    write_html(figure, _figure_path)


def graph_consumer_callback(channel, method, properties, body):
    """
    when the queue is receiving data the callback method is invoked
    :param channel: channel of communication
    :type channel: pika.channel.BlockingChannel
    :param method: used to acknowledge the message
    :type method: pika.spec.Basic.Deliver
    :param properties: user-defined properties on the message
    :type properties: pika.spec.BasicProperties
    :type body: bytes
    """
    table_name = body.decode()
    print(f"Graph Consumer received the name of the database: {table_name}")
    graph_dataframe = build_dataframe(table_name)
    build_graph(graph_dataframe, figure_path)


def write_html(figure, _figure_path):
    figure.write_html(_figure_path, auto_open=True)


def get_figure(dates, counts, totals):
    figure = make_subplots(rows=2, cols=1,
                           subplot_titles=("Totals per Month", "Counts per New Customers"),
                           shared_xaxes=True)
    figure.append_trace(go.Scatter(x=dates, y=totals, name="Totals"), row=1, col=1)
    figure.append_trace(go.Bar(x=dates, y=counts, name="Counts"), row=2, col=1)
    figure.update_layout(xaxis2=dict(tickmode='array', tickvals=dates, ticktext=dates))
    return figure


def get_columns(graph_dataframe):
    return graph_dataframe['InvoiceDate'].tolist(), \
           graph_dataframe['Count'].tolist(), \
           graph_dataframe['Total'].tolist()


def get_column_count(customer_count_dataframe):
    return customer_count_dataframe.groupby(['InvoiceDate']).size().reset_index(name="Count")


def get_column_sum(analyze_dataframe):
    return analyze_dataframe.groupby(['InvoiceDate'])['Total'].sum()


def drop_duplicates(analyze_dataframe):
    return analyze_dataframe.drop_duplicates(subset=['CustomerId', 'InvoiceDate'])


def get_invoice_date_fixed(dataframe):
    try:
        dataframe.loc[:, 'InvoiceDate'] = pandas.to_datetime(dataframe['InvoiceDate']).dt.strftime('%Y-%m')
        return dataframe
    except ValueError as e:
        print("to_datetime Error: ", e.args[0])


def get_insert_many_query(table_name):
    return '''INSERT INTO ''' + table_name + ''' VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'''


def get_opened_file(file_path):
    return open(file_path)


def establish_connection(_database_path):
    database_connection = DatabaseHandler(db_path=_database_path)
    database_connection.connect()
    return database_connection


def create_table_if_not_exist(database_connection, table_name):
    query = '''CREATE TABLE IF NOT EXISTS ''' + table_name + ''' (InvoiceId integer,
            CustomerId integer, InvoiceDate text, BillingAddress text,
            BillingCity text, BillingState text, BillingCountry text,
            BillingPostalCode text, Total float,
            UNIQUE (InvoiceId, CustomerId, InvoiceDate, BillingAddress, BillingCity, BillingState,
                    BillingCountry, BillingPostalCode, Total) ON CONFLICT IGNORE)'''
    database_connection.create_table(query)


def order_headers(dataframe):
    # rearranging the columns to match the rest of the files
    return dataframe[['InvoiceId', 'CustomerId', 'InvoiceDate',
                      'BillingAddress', 'BillingCity', 'BillingState',
                      'BillingCountry', 'BillingPostalCode', 'Total']]
