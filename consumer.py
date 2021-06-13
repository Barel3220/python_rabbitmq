import pika


class Consumer:
    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()
        self.consume()
        self.keep_consume()

    def callback(self, channel, method, properties, body):
        print(" [x] Received %r" % body)
        self.declare()
        self.publish()
        print(" [y] Sent 'Goodbye World!'")

    def declare(self):
        self.channel.queue_declare(queue='goodbye')

    def publish(self):
        self.channel.basic_publish(exchange='', routing_key='goodbye', body=b"Goodbye World!")
        return " [y] Sent 'Goodbye World!'"

    def consume(self):
        self.channel.basic_consume(queue='hello', on_message_callback=self.callback, auto_ack=True)

    def keep_consume(self):
        print(' [*] Waiting for messages. To exit press CTRL+C')
        self.channel.start_consuming()


if __name__ == '__main__':
    Consumer()
