import pika
from src.processing import process_file


class DatabaseConsumer:
    def __init__(self):
        """
        initiating the class, creating the connection to pika (rabbitmq python's module)
        receiving the path to read the files
        after processing the files publishing to the second queue the ok
        """
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()
        self.declare()
        self.consume()

    def callback(self, channel, method, properties, body):
        """
        when the queue is receiving data the callback method is invoked
        :param channel: channel of communication
        :type channel: pika.channel.Channel
        :param method: used to acknowledge the message
        :type method: pika.spec.Basic.Deliver
        :param properties: user-defined properties on the message
        :type properties: pika.spec.BasicProperties
        :type body: bytes
        """
        data = body.decode().split(" ")
        print(f"DatabaseConsumer received {data[1]} file")
        process_file(data[0], data[1], data[2])
        self.publish(bytes(data[2], encoding='utf-8'))

    def declare(self):
        """
        declaring both queues, i can do this because the 'queue_declare' is idempotent
        and it's important to declare here too because i can't really know which (consumer/producer) runs first
        """
        self.channel.queue_declare(queue='files_to_database')
        self.channel.queue_declare(queue='database_to_graph')

    def publish(self, byte_string: bytes):
        """
        publishing the ok for the graph, using the default exchange
        messages are routed to the queue with the name specified by 'routing_key'
        :type byte_string: bytes
        :return: str
        """
        self.channel.basic_publish(exchange='', routing_key='database_to_graph', body=byte_string)
        return f"Sent {byte_string.decode()}"

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
        print(' DatabaseConsumer is Waiting for messages. To exit press CTRL+C')
        self.channel.start_consuming()


if __name__ == '__main__':
    database_consumer = DatabaseConsumer()
    database_consumer.keep_consume()
