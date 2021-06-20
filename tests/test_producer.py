import unittest
from producer import Producer


class TestConsumer(unittest.TestCase):
    def setUp(self):
        self.producer = Producer()

    def tearDown(self):
        self.producer.channel.queue_purge(queue='files_to_database')
        self.producer.channel.close()

    def test_publish(self):
        self.producer.declare()
        result = self.producer.publish(b"Testing!")
        self.assertEqual(result, "Sent Testing!")


if __name__ == '__main__':
    unittest.main()
