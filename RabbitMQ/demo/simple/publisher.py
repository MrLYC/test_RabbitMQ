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
        "-k", "--routing_key", action="store", default="",
        help="the routing key to publish.")
    parser.add_argument(
        "-m", "--messages", action="store", nargs="+", default=[],
        help="messages to publish")
    parser.add_argument(
        "-et", "--exchange_type", action="store", default="direct",
        help="exchange type")
    parser.add_argument(
        "-s", "--server", action="store", default="localhost",
        help="RabbitMQ host which to connect")
    parser.add_argument(
        "-u", "--user", action="store", default="guest",
        help="user to connect")
    parser.add_argument(
        "-w", "--password", action="store", default="guest",
        help="password to connect")
    parser.add_argument(
        "-p", "--port", action="store", default=5672, type=int,
        help="the port of RabbitMQ host")

    return parser.parse_args(argv)


def main(argv):
    args = parse_arguments(argv)
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=args.server, port=args.port,
        credentials=pika.credentials.PlainCredentials(
            args.user, args.password)))

    channel = connection.channel()

    channel.exchange_declare(
        exchange=args.exchange, exchange_type=args.exchange_type)

    for message in args.messages:
        channel.basic_publish(
            exchange=args.exchange,
            routing_key=args.routing_key,
            body=message)

    channel.close()
    connection.close()

if __name__ == '__main__':
    import sys
    main(sys.argv[1:])
