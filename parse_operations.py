#!/usr/bin/env python3
from influxdb import InfluxDBClient
import json
import argparse
import sys
from dateutil.parser import parse
from utils import grouper, configure_logging, write_points
from log_file import LogFile
from threading import Thread
from multiprocessing import Process, Queue, Pool
from queue import Empty
from consumer import Consumer
from producer import Producer
import time

_MEASUREMENT_PREFIX = "operations_"

__author__ = 'victorhooi'

def create_point(timestamp, measurement_name, values, tags):
    return {
        "measurement": measurement_name,
        "tags": tags,
        "time": timestamp,
        "fields": values
    }


# def create_point(timestamp, metric_name, value, tags):
#     return {
#         "measurement": _MEASUREMENT_PREFIX + metric_name,
#         "tags": tags,
#         "time": timestamp,
#         "fields": {
#             "value": value
#         }
#     }


parser = argparse.ArgumentParser(description='Parse a mongod.log logfile for query timing information, and load it into an InfluxDB instance')
parser.add_argument('-b', '--batch-size', default=5000, type=int, help="Batch size to process before writing to InfluxDB.")
parser.add_argument('-d', '--database', default="insight", help="Name of InfluxDB database to write to. Defaults to 'insight'.")
parser.add_argument('-n', '--hostname', required=True, help='Host(Name) of the server')
parser.add_argument('-p', '--project', required=True, help='Project name to tag this with')
# Override or set for 2.4
# parser.add_argument('-t', '--timezone', required=True, help='Hostname of the source system -e.g. "UTC", "US/Eastern", or "US/Pacific"')
parser.add_argument('-i', '--influxdb-host', default='localhost', help='InfluxDB instance to connect to. Defaults to localhost.')
parser.add_argument('-s', '--ssl', action='store_true', default=False, help='Enable SSl mode for InfluxDB.')
parser.add_argument('-w', '--workers', default=8, type=int, help="# of workers to use.")
parser.add_argument('-f', '--fork', action='store_true', default=False, help='use threads or forrk.')
parser.add_argument('-l', '--level', default="INFO", help="The log level, defaults to INFO.")
parser.add_argument('input_file')
args = parser.parse_args()


def main():
    logger = configure_logging('parse_operations', args)
    producer = Producer(LogFile(args.input_file))

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

