#!/usr/bin/env python
# encoding: utf-8

from __future__ import unicode_literals

from argparse import ArgumentParser

import pika


def parse_arguments(argv):
    parser = ArgumentParser(
        description="Listening and print message from RabbitMQ queues")
    parser.add_argument("exchange", default="", help="the exchange to bind")
    parser.add_argument(
        "-et", "--exchange_type", action="store", default="direct",
        help="exchange type")
    parser.add_argument(
        "-k", "--routing_key", action="store", default="", nargs="+",
        help="the routing key to bind.")
    parser.add_argument(
        "-s", "--server", action="store", default="localhost",
        help="RabbitMQ host which to connect")
    parser.add_argument(
        "-p", "--port", action="store", default=5672, type=int,
        help="the port of RabbitMQ host")

    return parser.parse_args(argv)


def message_handler(ch, method, properties, body):
    ch.basic_ack(delivery_tag=method.delivery_tag)
    print "%s:%s # %s" % (method.exchange, method.routing_key, body)


def main(argv):
    args = parse_arguments(argv)

    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=args.server, port=args.port))

    channel = connection.channel()

    channel.exchange_declare(
        exchange=args.exchange, exchange_type=args.exchange_type)

    for routing_key in args.routing_key:
        result = channel.queue_declare(exclusive=True)
        queue_name = result.method.queue
        channel.queue_bind(
            exchange=args.exchange,
            queue=queue_name,
            routing_key=routing_key)
        channel.basic_consume(
            message_handler,
            queue=queue_name)

        print "binding[%s]: exchange:%s, queue:%s" % (
            routing_key, args.exchange, queue_name)

    try:
        print "Listening.."
        channel.start_consuming()
    except KeyboardInterrupt:
        pass

    channel.close()
    connection.close()

if __name__ == '__main__':
    import sys
    main(sys.argv[1:])
