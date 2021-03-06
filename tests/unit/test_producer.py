import unittest
from src.producer import Producer


class TestProducer(unittest.TestCase):
    def setUp(self):
        """
        creating an object of producer
        """
        self.producer = Producer()

    def tearDown(self):
        """
        clearing and 'forgetting' the queue
        closing the channel
        """
        self.producer.channel.queue_purge(queue='files_to_database')
        self.producer.channel.close()

    def test_publish(self):
        """
        1. checking publishing bytes string and getting the correct string in return
        """
        self.producer.declare()
        result = self.producer.publish(b"Testing!")
        self.assertEqual(result, "Sent Testing!")


if __name__ == '__main__':
    unittest.main()
