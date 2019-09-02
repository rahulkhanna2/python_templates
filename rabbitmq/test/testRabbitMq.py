"""

RabbitMq test file. Requires a local RabbitMq server

"""

import unittest
import json
from RabbitMq import RabbitMq
from time import sleep
import threading

filename = "/".join(__file__.split("/")[:-1]) + "/../app_config.json"
with open(filename) as content_file:
    configData = json.load(content_file)

class TestRabbitMq(unittest.TestCase):
    basic_options = {
        'AMQP': configData['AMQP']
    }
    ssl_options = {
    'AMQP':'amqps://localhost:5673',
    'RABBIT_CA':'someca',
    'RABBIT_CERT':'somecert',
    'RABBIT_KEY':'somekey'
  }

    TEST_QUEUE = 'testQueue1'
    TEST_QUEUE2 = 'testQueue2'
    test_data = None
    RabbitMq = None

    @classmethod
    def setUp(cls):
        cls.RabbitMq = RabbitMq(options=TestRabbitMq.basic_options, logger=DummyLogger)

    def test_RabbitMq_publish(self):
        TestRabbitMq.RabbitMq = RabbitMq(TestRabbitMq.basic_options, TestRabbitMq.DUMMYLOGGER)
        self.assertTrue(TestRabbitMq.RabbitMq.connection.is_open)
        TEST_MSG = 'hello_world'
        TestRabbitMq.RabbitMq.channel.queue_declare(queue=TestRabbitMq.TEST_QUEUE, exclusive=True, auto_delete=True)
        TestRabbitMq.RabbitMq.send_message(TestRabbitMq.TEST_QUEUE, TEST_MSG)
        sleep(0.5)  # Need to wait until message is in the queue to get
        res = TestRabbitMq.RabbitMq.channel.basic_get(queue=TestRabbitMq.TEST_QUEUE)
        self.assertEquals(json.loads(res[-1]), {u'res': u'hello_world'})

    def test_RabbitMq_consume(self):

        TEST_MSG = 'hello_world'
        TestRabbitMq.RabbitMq = RabbitMq(TestRabbitMq.basic_options, TestRabbitMq.DUMMYLOGGER)
        TestRabbitMq.test_data = None
        counter = 0

        def consume_callback(_channel, _method_frame, _header_frame, body):
            if body is not None:
                TestRabbitMq.test_data = json.loads(body)

        def consume_message(cls):
            cls.RabbitMq.consume(cls.TEST_QUEUE2, consume_callback)

        consume_thread = threading.Thread(target=consume_message, args=[TestRabbitMq])
        consume_thread.setDaemon(True)
        consume_thread.start()
        sleep(1.0)  # Need to wait until consumer is set up to send message
        TestRabbitMq.RabbitMq.send_message(TestRabbitMq.TEST_QUEUE2, TEST_MSG)

        while TestRabbitMq.test_data is None and counter < 50:
            counter += 1
            sleep(0.1)
        self.assertEqual(TestRabbitMq.test_data, {u'res': u'hello_world'})

    @classmethod
    def tearDownClass(cls):
        if cls.RabbitMq is not None:
            pass
            cls.RabbitMq.channel.queue_delete(queue=TestRabbitMq.TEST_QUEUE)
            cls.RabbitMq.channel.queue_delete(queue=TestRabbitMq.TEST_QUEUE2)
