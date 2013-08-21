# Copyright (c) 2012 - Rackspace Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to
# deal in the Software without restriction, including without limitation the
# rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
# sell copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
# IN THE SOFTWARE.

import json

import kombu
import mox

from stacktach import db
from stacktach import views
import worker.worker as worker
from tests.unit import StacktachBaseTestCase


class ConsumerTestCase(StacktachBaseTestCase):
    def setUp(self):
        self.mox = mox.Mox()

    def tearDown(self):
        self.mox.UnsetStubs()

    def _test_topics(self):
        return [
            dict(queue="queue1", routing_key="monitor.info"),
            dict(queue="queue2", routing_key="monitor.error")
        ]

    def test_get_consumers(self):
        created_queues = []
        created_callbacks = []
        created_consumers = []
        def Consumer(queues=None, callbacks=None):
            created_queues.extend(queues)
            created_callbacks.extend(callbacks)
            consumer = self.mox.CreateMockAnything()
            created_consumers.append(consumer)
            return consumer
        self.mox.StubOutWithMock(worker.Consumer, '_create_exchange')
        self.mox.StubOutWithMock(worker.Consumer, '_create_queue')
        consumer = worker.Consumer('test', None, None, True, {}, "nova",
                                   self._test_topics())
        exchange = self.mox.CreateMockAnything()
        consumer._create_exchange('nova', 'topic').AndReturn(exchange)
        info_queue = self.mox.CreateMockAnything()
        error_queue = self.mox.CreateMockAnything()
        consumer._create_queue('queue1', exchange, 'monitor.info')\
                .AndReturn(info_queue)
        consumer._create_queue('queue2', exchange, 'monitor.error')\
                .AndReturn(error_queue)
        self.mox.ReplayAll()
        consumers = consumer.get_consumers(Consumer, None)
        self.assertEqual(len(consumers), 1)
        self.assertEqual(consumers[0], created_consumers[0])
        self.assertEqual(len(created_queues), 2)
        self.assertTrue(info_queue in created_queues)
        self.assertTrue(error_queue in created_queues)
        self.assertEqual(len(created_callbacks), 1)
        self.assertTrue(consumer.on_nova in created_callbacks)
        self.mox.VerifyAll()

    def test_create_exchange(self):
        args = {'key': 'value'}
        consumer = worker.Consumer('test', None, None, True, args, 'nova',
                                   self._test_topics())

        self.mox.StubOutClassWithMocks(kombu.entity, 'Exchange')
        exchange = kombu.entity.Exchange('nova', type='topic', exclusive=False,
                                         durable=True, auto_delete=False)
        self.mox.ReplayAll()
        actual_exchange = consumer._create_exchange('nova', 'topic')
        self.assertEqual(actual_exchange, exchange)
        self.mox.VerifyAll()

    def test_create_queue(self):
        self.mox.StubOutClassWithMocks(kombu, 'Queue')
        exchange = self.mox.CreateMockAnything()
        queue = kombu.Queue('name', exchange, auto_delete=False, durable=True,
                            exclusive=False, routing_key='routing.key',
                            queue_arguments={})
        consumer = worker.Consumer('test', None, None, True, {}, 'nova',
                                   self._test_topics())
        self.mox.ReplayAll()
        actual_queue = consumer._create_queue('name', exchange, 'routing.key',
                                              exclusive=False,
                                              auto_delete=False)
        self.assertEqual(actual_queue, queue)
        self.mox.VerifyAll()


    def test_create_queue_with_queue_args(self):
        self.mox.StubOutClassWithMocks(kombu, 'Queue')
        exchange = self.mox.CreateMockAnything()
        queue_args = {'key': 'value'}
        queue = kombu.Queue('name', exchange, auto_delete=False, durable=True,
                            exclusive=False, routing_key='routing.key',
                            queue_arguments=queue_args)
        consumer = worker.Consumer('test', None, None, True, queue_args,
                                   'nova', self._test_topics())
        self.mox.ReplayAll()
        actual_queue = consumer._create_queue('name', exchange, 'routing.key',
                                              exclusive=False,
                                              auto_delete=False)
        self.assertEqual(actual_queue, queue)
        self.mox.VerifyAll()

    def test_process(self):
        deployment = self.mox.CreateMockAnything()
        raw = self.mox.CreateMockAnything()
        raw.get_name().AndReturn('RawData')
        message = self.mox.CreateMockAnything()

        exchange = 'nova'
        consumer = worker.Consumer('test', None, deployment, True, {},
                                   exchange, self._test_topics())
        routing_key = 'monitor.info'
        message.delivery_info = {'routing_key': routing_key}
        body_dict = {u'key': u'value'}
        message.body = json.dumps(body_dict)

        mock_notification = self.mox.CreateMockAnything()
        mock_post_process_method = self.mox.CreateMockAnything()
        mock_post_process_method(raw, mock_notification)
        old_handler = worker.POST_PROCESS_METHODS
        worker.POST_PROCESS_METHODS["RawData"] = mock_post_process_method

        self.mox.StubOutWithMock(views, 'process_raw_data',
                                 use_mock_anything=True)
        args = (routing_key, body_dict)
        views.process_raw_data(deployment, args, json.dumps(args), exchange) \
            .AndReturn((raw, mock_notification))
        message.ack()

        self.mox.StubOutWithMock(consumer, '_check_memory',
                                 use_mock_anything=True)
        consumer._check_memory()
        self.mox.ReplayAll()
        consumer._process(message)
        self.assertEqual(consumer.processed, 1)
        self.mox.VerifyAll()
        worker.POST_PROCESS_METHODS["RawData"] = old_handler

    def test_run(self):
        config = {
            'name': 'east_coast.prod.global',
            'durable_queue': False,
            'rabbit_host': '10.0.0.1',
            'rabbit_port': 5672,
            'rabbit_userid': 'rabbit',
            'rabbit_password': 'rabbit',
            'rabbit_virtual_host': '/',
            "services": ["nova"],
            "topics": {"nova": self._test_topics()}
        }
        self.mox.StubOutWithMock(db, 'get_or_create_deployment')
        deployment = self.mox.CreateMockAnything()
        db.get_or_create_deployment(config['name'])\
          .AndReturn((deployment, True))
        self.mox.StubOutWithMock(kombu.connection, 'BrokerConnection')
        params = dict(hostname=config['rabbit_host'],
                      port=config['rabbit_port'],
                      userid=config['rabbit_userid'],
                      password=config['rabbit_password'],
                      transport="librabbitmq",
                      virtual_host=config['rabbit_virtual_host'])
        self.mox.StubOutWithMock(worker, "continue_running")
        worker.continue_running().AndReturn(True)
        conn = self.mox.CreateMockAnything()
        kombu.connection.BrokerConnection(**params).AndReturn(conn)
        conn.__enter__().AndReturn(conn)
        conn.__exit__(None, None, None).AndReturn(None)
        self.mox.StubOutClassWithMocks(worker, 'Consumer')
        exchange = 'nova'
        consumer = worker.Consumer(config['name'], conn, deployment,
                                   config['durable_queue'], {}, exchange,
                                   self._test_topics())
        consumer.run()
        worker.continue_running().AndReturn(False)
        self.mox.ReplayAll()
        worker.run(config, exchange)
        self.mox.VerifyAll()

    def test_run_queue_args(self):
        config = {
            'name': 'east_coast.prod.global',
            'durable_queue': False,
            'rabbit_host': '10.0.0.1',
            'rabbit_port': 5672,
            'rabbit_userid': 'rabbit',
            'rabbit_password': 'rabbit',
            'rabbit_virtual_host': '/',
            'queue_arguments': {'x-ha-policy': 'all'},
            'queue_name_prefix': "test_name_",
            "services": ["nova"],
            "topics": {"nova": self._test_topics()}
        }
        self.mox.StubOutWithMock(db, 'get_or_create_deployment')
        deployment = self.mox.CreateMockAnything()
        db.get_or_create_deployment(config['name'])\
          .AndReturn((deployment, True))
        self.mox.StubOutWithMock(kombu.connection, 'BrokerConnection')
        params = dict(hostname=config['rabbit_host'],
                      port=config['rabbit_port'],
                      userid=config['rabbit_userid'],
                      password=config['rabbit_password'],
                      transport="librabbitmq",
                      virtual_host=config['rabbit_virtual_host'])
        self.mox.StubOutWithMock(worker, "continue_running")
        worker.continue_running().AndReturn(True)
        conn = self.mox.CreateMockAnything()
        kombu.connection.BrokerConnection(**params).AndReturn(conn)
        conn.__enter__().AndReturn(conn)
        conn.__exit__(None, None, None).AndReturn(None)
        self.mox.StubOutClassWithMocks(worker, 'Consumer')
        exchange = 'nova'
        consumer = worker.Consumer(config['name'], conn, deployment,
                                   config['durable_queue'],
                                   config['queue_arguments'], exchange,
                                   self._test_topics())
        consumer.run()
        worker.continue_running().AndReturn(False)
        self.mox.ReplayAll()
        worker.run(config, exchange)
        self.mox.VerifyAll()
