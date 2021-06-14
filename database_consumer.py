import pika
import pandas
import csv
import sqlite3


class DatabaseConsumer:
    def __init__(self):
        """
        initiating the class, creating the connection to the rabbitmq
        receiving the path to read the files
        after processing the files publishing to the second queue the ok
        """
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()
        self.declare()
        self.consume()
        self.keep_consume()

    def callback(self, channel, method, properties, body):
        """
        when the queue is receiving data the callback method is invoked
        :param channel: pika.channel.Channel: channel of communication
        :param method: pika.spec.Basic.Deliver: used to acknowledge the message
        :param properties: pika.spec.BasicProperties: user-defined properties on the message
        :param body: bytes
        """
        raw_data = body.decode()
        data = raw_data.split(" ")
        print(f"Database Consumer received {data[1]} file")
        self.process_file(data[0], data[1], data[2])
        self.publish(data[2])

    def declare(self):
        """
        declaring both queues, i can do this because the 'queue_declare' is idempotent
        and it's important to declare here too because i can't really know which (consumer/producer) runs first
        """
        self.channel.queue_declare(queue='files_to_database')
        self.channel.queue_declare(queue='database_to_graph')

    def publish(self, byte_string):
        """
        publishing the ok for the graph, using the default exchange
        messages are routed to the queue with the name specified by 'routing_key'
        :param byte_string: bytes
        :return: str
        """
        self.channel.basic_publish(exchange='', routing_key='database_to_graph', body=byte_string)
        return f"Sent {byte_string}"

    def consume(self):
        """
        consume means to listen to the queue forever until some data comes,
        then process it, then continue listening
        """
        self.channel.basic_consume(queue='files_to_database', on_message_callback=self.callback, auto_ack=True)

    def keep_consume(self):
        """
        this command gives the flag to start consuming, but never stops
        """
        print(' [*] Waiting for messages. To exit press CTRL+C')
        self.channel.start_consuming()

    def process_file(self, file_path, file_type, db_name):
        database_connection = sqlite3.connect('database/invoices.db')
        cursor = database_connection.cursor()
        file = open(file_path)
        rows = None

        # creating the table then inserting all the data inside
        # cursor.execute('''CREATE TABLE ''' + db_name +
        #                ''' (InvoiceId integer, CustomerId integer, InvoiceDate text,
        #                BillingAddress text, BillingCity text, BillingState text,
        #                BillingCountry text, BillingPostalCode text, Total float,
        #                UNIQUE (InvoiceId, CustomerId, InvoiceDate, BillingAddress, BillingCity, BillingState,
        #                        BillingCountry, BillingPostalCode, Total) ON CONFLICT IGNORE)''')

        if "JSON" in file_type:
            dataframe = pandas.read_json(file)

            # rearranging the columns to match the rest of the files
            dataframe = dataframe[['InvoiceId', 'CustomerId', 'InvoiceDate', 'BillingAddress',
                                   'BillingCity', 'BillingState', 'BillingCountry', 'BillingPostalCode', 'Total']]

            rows = dataframe.values.tolist()

        elif "CSV" in file_type:
            csv_reader = csv.reader(file, delimiter=',')

            # skipping the first row (headers)
            next(csv_reader)

            rows = csv_reader

        # inserting all the rows of data to the corresponding data table
        cursor.executemany('''INSERT INTO ''' + db_name + ''' VALUES
        (?, ?, ?, ?, ?, ?, ?, ?, ?)''', rows)

        # printing
        cursor.execute('''SELECT * FROM ''' + db_name)
        for item in cursor.fetchall():
            print(item)

        # saving the data inside the database, without this line the data won't be accessible
        database_connection.commit()
        database_connection.close()


if __name__ == '__main__':
    DatabaseConsumer()
