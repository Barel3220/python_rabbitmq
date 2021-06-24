import os
import pandas
import plotly.graph_objs as go
from plotly.subplots import make_subplots
from src.database_handler import DatabaseHandler, database_path
figure_path = os.path.normpath(os.path.dirname(__file__) + os.path.join('/database/figure.html'))


def process_file(file_path: str, file_type: str, table_name: str):
    """
    if database directory isn't exists, creating it
    getting the file path, file type, and table name
    opening the file then inserting all the data into the database
    :param file_path: filepath normal to the operating system
    :param file_type: CSV/JSON
    :param table_name: table name
    :return: str from database_handler, 0 in case of exception
    """
    if not os.path.exists(database_path):
        os.mkdir(os.path.dirname(database_path))
    database_connection = establish_connection(database_path)
    create_table_if_not_exist(database_connection, table_name)

    insert_many_query = get_insert_many_query(table_name)

    dataframe = pandas.DataFrame()
    try:
        if "json" == file_type:
            dataframe = pandas.read_json(file_path)

        elif "csv" == file_type:
            dataframe = pandas.read_csv(file_path)
    except pandas.errors.ParserError as e:
        print("reading failed: ", e.args[0])
        return 0

    dataframe = order_headers(dataframe)
    return database_connection.insert_many(insert_many_query, dataframe.values.tolist())


def build_dataframe(table_name: str) -> pandas.DataFrame:
    """
    establishing connection to database, and requesting dataframe of the data
    then processing it and preparing it for the graph
    :param table_name: database name for the query
    :return: summed and counted dataframe
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


def build_graph(graph_dataframe: pandas.DataFrame, _figure_path: str, auto_open_flag: bool) -> str:
    """
    getting the dataframe from graph_consumer and
    extracting the columns to present them properly in browser
    :param auto_open_flag: either auto_open or not
    :param _figure_path: path to figure file in database folder
    :param graph_dataframe: dataframe['InvoiceDate', 'Count', 'Total']
    :return: All Done str
    """
    dates, counts, totals = get_columns(graph_dataframe)
    figure = get_figure(dates, counts, totals)
    write_html(figure, _figure_path, auto_open_flag)
    return "Updated html File and Opened it"


def graph_consumer_callback(channel, method, properties, body) -> None:
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
    build_graph(graph_dataframe, figure_path, True)


def write_html(figure: go.Figure, _figure_path: str, auto_open_flag: bool) -> None:
    """
    writing the data into .html file
    :param figure: the graph
    :param _figure_path: path to the .html file
    :param auto_open_flag: boolean to or not to open the web
    """
    figure.write_html(_figure_path, auto_open=auto_open_flag)


def get_figure(dates: list, counts: list, totals: list) -> go.Figure:
    """
    creating a figure holding all the data from the dataframe
    :param dates: columns of dataframe
    :param counts: columns of dataframe
    :param totals: columns of dataframe
    :return: the worked figure with 2 graphs
    """
    figure = make_subplots(rows=2, cols=1,
                           subplot_titles=("Totals per Month", "Counts per New Customers"),
                           shared_xaxes=True)
    figure.add_trace(go.Scatter(x=dates, y=totals, name="Totals"), row=1, col=1)
    figure.add_trace(go.Bar(x=dates, y=counts, name="Counts"), row=2, col=1)
    figure.update_layout(xaxis2=dict(tickmode='array', tickvals=dates, ticktext=dates))
    return figure


def get_columns(graph_dataframe: pandas.DataFrame) -> tuple:
    """
    extracting columns from the dataframe
    :param graph_dataframe: worked dataframe
    :return: tuple of columns
    """
    return graph_dataframe['InvoiceDate'].tolist(), graph_dataframe['Count'].tolist(), graph_dataframe['Total'].tolist()


def get_column_count(customer_count_dataframe: pandas.DataFrame) -> pandas.DataFrame:
    """
    grouping by the dataframe by the date and counting the rows
    :param customer_count_dataframe: dataframe without CustomerId duplicates
    :return: dataframe with 2 columns, dates and counts
    """
    return customer_count_dataframe.groupby(['InvoiceDate']).size().reset_index(name="Count")


def get_column_sum(analyze_dataframe: pandas.DataFrame) -> pandas.DataFrame:
    """
    grouping by the dataframe by the date and summing the totals
    :param analyze_dataframe: copy of the clean dataframe, to keep data integrity
    :return: dataframe with 2 columns, dates and sums
    """
    return analyze_dataframe.groupby(['InvoiceDate'])['Total'].sum()


def drop_duplicates(analyze_dataframe: pandas.DataFrame) -> pandas.DataFrame:
    """
    getting a dataframe to drop duplicates if there is, duplicate is row with the same CustomerId and InvoiceDate
    :param analyze_dataframe: copy of the clean dataframe, to keep data integrity
    :return: dataframe without duplicates
    """
    return analyze_dataframe.drop_duplicates(subset=['CustomerId', 'InvoiceDate'])


def get_invoice_date_fixed(dataframe: pandas.DataFrame) -> pandas.DataFrame:
    """
    trying to change the type of values of InvoiceDate
    :param dataframe: dataframe with 3 columns
    :return: dataframe with fixed dates
    """
    try:
        dataframe.loc[:, 'InvoiceDate'] = pandas.to_datetime(dataframe['InvoiceDate']).dt.strftime('%Y-%m')
        return dataframe
    except ValueError as e:
        print("to_datetime Error: ", e.args[0])


def get_insert_many_query(table_name: str) -> str:
    """
    insert string, starting with 'replace' instead of 'insert or replace'
    :param table_name: table name
    :return: insert string
    """
    return '''REPLACE INTO ''' + table_name + ''' VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'''


def establish_connection(_database_path: str) -> DatabaseHandler:
    """
    creating an instance of DatabaseHandler
    :param _database_path: path to database
    :return: instance of DatabaseHandler
    """
    database_connection = DatabaseHandler(db_path=_database_path)
    database_connection.connect()
    return database_connection


def create_table_if_not_exist(database_connection: DatabaseHandler, table_name: str) -> None:
    """
    create table query, with column types and unique columns to prevent duplicates
    :param database_connection: DatabaseHandler instance
    :param table_name: table name
    """
    query = '''CREATE TABLE IF NOT EXISTS ''' + table_name + ''' (InvoiceId integer,
            CustomerId integer, InvoiceDate text, BillingAddress text,
            BillingCity text, BillingState text, BillingCountry text,
            BillingPostalCode text, Total float,
            UNIQUE (InvoiceId, CustomerId) ON CONFLICT IGNORE)'''
    database_connection.create_table(query)


def order_headers(dataframe: pandas.DataFrame):
    """
    ordering the columns as the database table
    creating one order to all
    :param dataframe: dataframe with scrambled columns
    :return: ordered dataframe
    """
    # rearranging the columns to match the rest of the files
    return dataframe[['InvoiceId', 'CustomerId', 'InvoiceDate',
                      'BillingAddress', 'BillingCity', 'BillingState',
                      'BillingCountry', 'BillingPostalCode', 'Total']]
