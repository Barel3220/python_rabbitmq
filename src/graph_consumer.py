import pika
from src.processing import graph_consumer_callback


class GraphConsumer:
    def __init__(self):
        """
        initiating the class, creating the connection to pika (rabbitmq python's module)
        receiving the ok to create or update the graph
        after getting the data from the data base, processing it and passing it to the graph
        """
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()
        self.declare()
        self.consume()
        self.keep_consume()

    def declare(self):
        """
        declaring the queue which the database_consumer will publish to
        it's important to declare it here too because i can't know which of the files will run first
        """
        self.channel.queue_declare(queue='database_to_graph')

    def consume(self):
        """
        consume means to listen to the queue forever until some data comes,
        then process it, then continue listening
        """
        self.channel.basic_consume(queue='database_to_graph',
                                   on_message_callback=graph_consumer_callback, auto_ack=True)

    def keep_consume(self):
        """
        this command gives the flag to start consuming, but never stops
        """
        print('GraphConsumer is Waiting for messages. To exit press CTRL+C')
        self.channel.start_consuming()


if __name__ == '__main__':
    GraphConsumer()
