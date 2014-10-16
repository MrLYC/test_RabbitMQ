#!/usr/bin/env python
# encoding: utf-8

from unittest import TestCase
from functools import partial
from threading import Thread

import pika


class TestPSMode(TestCase):
    def setUp(self):
        self.connparms = pika.ConnectionParameters(host="localhost")
        self.pub_conn = pika.BlockingConnection(self.connparms)
        self.sub_conn = pika.BlockingConnection(self.connparms)
        self.exchange_name = "test_psmode"
        exchange_type = "direct"

        self.pub_channel = self.pub_conn.channel()
        self.sub_channel = self.sub_conn.channel()

        self.pub_channel.exchange_declare(
            exchange=self.exchange_name,
            exchange_type=exchange_type)
        self.sub_channel.exchange_declare(
            exchange=self.exchange_name,
            exchange_type=exchange_type)

    def tearDown(self):
        self.pub_channel.close()
        self.sub_channel.close()

        self.pub_conn.close()
        self.sub_conn.close()

    def test_direct(self):
        result = self.sub_channel.queue_declare(exclusive=True)
        queue1 = result.method.queue
        self.sub_channel.queue_bind(
            routing_key=queue1,
            exchange=self.exchange_name,
            queue=queue1)

        result = self.sub_channel.queue_declare(exclusive=True)
        queue2 = result.method.queue
        self.sub_channel.queue_bind(
            routing_key=queue2,
            exchange=self.exchange_name,
            queue=queue2)

        called = {}

        def callback(ch, method, properties, body, name):
            if body == "":
                ch.stop_consuming()
            else:
                ch.basic_ack(delivery_tag=method.delivery_tag)
                assert body == name

            if name not in called:
                called[name] = 0
            called[name] += 1

        self.sub_channel.basic_consume(
            partial(callback, name=queue1),
            queue=queue1)

        self.sub_channel.basic_consume(
            partial(callback, name=queue2),
            queue=queue2)

        sub_thread = Thread(target=self.sub_channel.start_consuming)
        sub_thread.start()

        self.pub_channel.basic_publish(
            exchange=self.exchange_name,
            routing_key=queue1,
            body=queue1)

        self.pub_channel.basic_publish(
            exchange=self.exchange_name,
            routing_key=queue2,
            body=queue2)

        self.pub_channel.basic_publish(
            exchange=self.exchange_name,
            routing_key=queue1,
            body=queue1)

        self.pub_channel.basic_publish(
            exchange=self.exchange_name,
            routing_key=queue1,
            body="")

        sub_thread.join()

        assert called[queue1] == 3
        assert called[queue2] == 1
