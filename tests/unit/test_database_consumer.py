import unittest
from src.database_consumer import DatabaseConsumer


class TestDatabaseConsumer(unittest.TestCase):
    def setUp(self):
        """
        creating an object of producer
        """
        self.database_consumer = DatabaseConsumer()

    def tearDown(self):
        """
        clearing and 'forgetting' the queue
        closing the channel
        """
        self.database_consumer.channel.queue_purge(queue='database_to_graph')
        self.database_consumer.channel.close()

    def test_publish(self):
        """
        1. checking publishing bytes string and getting the correct string in return
        """
        self.database_consumer.declare()
        result = self.database_consumer.publish(b"Testing!")
        self.assertEqual(result, "Sent Testing!")


if __name__ == '__main__':
    unittest.main()
