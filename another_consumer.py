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

    def consume(self):
        self.channel.basic_consume(queue='goodbye', on_message_callback=self.callback, auto_ack=True)

    def keep_consume(self):
        print(' [***] Waiting for messages. To exit press CTRL+C')
        self.channel.start_consuming()


if __name__ == '__main__':
    Consumer()
