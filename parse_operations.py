#!/usr/bin/env python3
import argparse
import sys
from utils import configure_logging
from log_file import LogFile
from consumer import Consumer
from producer import Producer

_MEASUREMENT_PREFIX = "operations_"

__author__ = 'victorhooi'

parser = argparse.ArgumentParser(description='Parse a mongod.log logfile for query timing information,'\
                                             ' and load it into an InfluxDB instance')
parser.add_argument('-b', '--batch-size', default=5000, type=int,
                    help='Batch size to process before writing to InfluxDB.')
parser.add_argument('-d', '--database', default="insight",
                    help='Name of InfluxDB database to write to. Defaults to \'insight\'.')
parser.add_argument('-n', '--hostname', required=True, help='Host(Name) of the server')
parser.add_argument('-p', '--project', required=True, help='Project name to tag this with')
# Override or set for 2.4
# parser.add_argument('-t', '--timezone', required=True, help='Hostname of the source system -e.g. "UTC", "US/Eastern", or "US/Pacific"')
parser.add_argument('-i', '--influxdb-host', default='localhost',
                    help='InfluxDB instance to connect to. Defaults to localhost.')
parser.add_argument('-s', '--ssl', action='store_true', default=False, help='Enable SSl mode for InfluxDB.')
parser.add_argument('-w', '--workers', default=8, type=int, help="# of workers to use.")
parser.add_argument('-f', '--fork', action='store_true', default=False, help='use threads or forrk.')
parser.add_argument('-l', '--level', default="INFO", help="The log level, defaults to INFO.")
parser.add_argument('--single-threaded', action='store_true', default=False,
                    help='over all others, just run single threaded.')
parser.add_argument('input_file')
args = parser.parse_args()


def main():
    logger = configure_logging('parse_operations', args)
    io = LogFile(args.input_file)

    producer = Producer(io, logger)
    if args.single_threaded:
        Consumer(producer.adapter(), logger, args, 0).process()
    else:
        workers = []

        for i in range(args.workers):
            worker = Consumer(producer, logger, args, i)
            workers.append(worker)
            worker.start()

        producer.start()
        producer.flush(args.workers)
        for worker in workers:
            worker.join()


if __name__ == "__main__":
    sys.exit(main())