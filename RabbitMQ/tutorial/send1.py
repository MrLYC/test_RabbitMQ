#!/usr/bin/env python
# encoding: utf-8

import pika
import sys

if len(sys.argv) == 1:
    sys.exit(-1)

body = sys.argv[1]

connection = pika.BlockingConnection(pika.ConnectionParameters(
    host="localhost"))
channel = connection.channel()

channel.queue_declare(queue="hello")

channel.basic_publish(
    exchange="",
    routing_key="hello",
    body=body)

print "[x] Sent %r" % body

connection.close()
