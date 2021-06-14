import pika
import pandas
import sqlite3


class GraphConsumer:
    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()
        self.graph_dataframe = pandas.DataFrame()
        self.declare()
        self.consume()
        self.keep_consume()

    def declare(self):
        """
        declaring the queue which the database_consumer will publish to
        it's important to declare it here too because i can't know which of the files will run first
        """
        self.channel.queue_declare(queue='database_to_graph')

    def callback(self, channel, method, properties, body):
        db_name = body.decode()
        print(f"Graph Consumer received the name of the database: {db_name}")
        self.build_dataframe(db_name)

        with pandas.option_context('display.max_rows', None, 'display.max_columns', None):
            print(self.graph_dataframe)

    def consume(self):
        self.channel.basic_consume(queue='database_to_graph', on_message_callback=self.callback, auto_ack=True)

    def keep_consume(self):
        print(' [***] Waiting for messages. To exit press CTRL+C')
        self.channel.start_consuming()

    def build_dataframe(self, db_name):
        database_connection = sqlite3.connect('database/invoices.db')
        dataframe = pandas.read_sql_query(
            '''SELECT CustomerId, InvoiceDate, Total FROM ''' + db_name,
            database_connection
        )
        database_connection.close()

        dataframe.loc[:, 'InvoiceDate'] = pandas.to_datetime(dataframe['InvoiceDate']).dt.strftime('%Y-%m')

        customer_count_dataframe = dataframe.copy()
        customer_count_dataframe = customer_count_dataframe.drop_duplicates(subset=['CustomerId', 'InvoiceDate'])
        customer_count_dataframe = customer_count_dataframe.groupby(['InvoiceDate']).size().reset_index(name="Count")

        total_sum_dataframe = dataframe.copy()
        total_sum_dataframe = total_sum_dataframe.groupby(['InvoiceDate'])['Total'].sum()

        self.graph_dataframe = customer_count_dataframe.merge(total_sum_dataframe, how='inner', on='InvoiceDate')


if __name__ == '__main__':
    GraphConsumer()
