import pika


class Producer:
    def __init__(self):
        """
        initiating the class,
        creating the first queue and creating the connection to the rabbitmq
        """
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()

    def declare(self):
        """
        declaring the queue which the producer will publish to
        """
        self.channel.queue_declare(queue='files_to_database')

    def publish(self, byte_string):
        """
        publishing the path to the files, using the default exchange
        messages are routed to the queue with the name specified by 'routing_key'
        :param byte_string: bytes
        :return: str
        """
        self.channel.basic_publish(exchange='', routing_key='files_to_database', body=byte_string)
        return "Sent"

    def close(self):
        """
        closing the connection
        :return:
        """
        self.connection.close()
