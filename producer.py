import pika


class Producer:
    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()
        # self.declare()
        # self.publish()
        # self.close()

    def declare(self):
        self.channel.queue_declare(queue='hello')

    def publish(self, byte_string):
        self.channel.basic_publish(exchange='', routing_key='hello', body=byte_string)
        return "Sent"

    def close(self):
        self.connection.close()


if __name__ == '__main__':
    Producer()
