import unittest
from producer import Producer


class TestConsumer(unittest.TestCase):
    def setUp(self):
        self.producer = Producer()

    def tearDown(self):
        self.producer.channel.queue_purge(queue='hello')
        self.producer.channel.close()

    def test_callback(self):
        self.producer.declare()
        result = self.producer.publish(b"Hello World!")

        self.assertEqual(result, "Sent")


if __name__ == '__main__':
    unittest.main()
