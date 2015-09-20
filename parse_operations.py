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
parser.add_argument('input_file')
args = parser.parse_args()


def producer(q, f):
    line_count = 0
    for line in f:
        line_count += 1
        if line:
            line = line
            q.put([line, line_count])
    q.put('')

class Drainer(object):
    def __init__(self, q):
        self.q = q
        self.closed = False

    def __iter__(self):
        if self.closed:
            raise StopIteration
        while True:
            yield self.q.get()


def consumer(q, logger, client):
    logger.info("consumer starting")
    json_points = []
    skip = 0
    try:
        for tupple in q:
            line, line_count = tupple
            # zip_longest will backfill any missing values with None, so we need to handle this,
            # otherwise we'll miss the last batch
            if line is None or line == '':
                break
            line = line.decode('UTF-8').rstrip("\n")
            if line and line.endswith("ms"):
                values = {}
                tags = {
                    'project': args.project,
                    'hostname': args.hostname,
                    }
                try:
                    tags['operation'] = line.split("] ", 1)[1].split()[0]
                except IndexError as e:
                    logger.error("Unable to parse line - {} - {}".format(e, line))
                    break
                if tags['operation'] in ['command', 'query', 'getmore', 'insert', 'update', 'remove', 'aggregate', 'mapreduce']:
                    # print(line.strip())
                    thread = line.split("[", 1)[1].split("]")[0]
                    # Alternately - print(split_line[3])
                    if tags['operation'] == 'command':
                        tags['command'] = line.split("command: ")[1].split()[0]
                    if "conn" in thread:
                        tags['connection_id'] = thread
                    split_line = line.split()
                    values['duration_in_milliseconds'] = int(split_line[-1].rstrip('ms'))
                    # TODO 2.4.x timestamps have spaces
                    timestamp = parse(split_line[0])
                    if split_line[1].startswith("["):
                        # 2.4 Logline:
                        tags['namespace'] = split_line[3]
                        for stat in reversed(split_line):
                            if "ms" in stat:
                                pass
                            elif ":" in stat:
                                key, value = stat.split(":", 1)
                                values[key] = int(value)
                            elif stat == "locks(micros)":
                                pass
                            else:
                                break
                    else:
                        # 3.x logline:
                        tags['namespace'] = split_line[5]
                        # TODO - Parse locks
                        pre_locks, locks = line.split("locks:{", 1)
                        # We work backwards from the end, until we run out of key:value pairs
                        # TODO - Can we assume these are always integers?
                        for stat in reversed(pre_locks.split()):
                            if ":" in stat:
                                key, value = stat.split(":", 1)
                                values[key] = int(value)
                            else:
                                break
                                # TODO - Parse the full query plan for IXSCAN
                        if 'planSummary: ' in line:
                            tags['plan_summary'] = (line.split('planSummary: ', 1)[1].split()[0])
                    json_points.append(create_point(timestamp, "operations", values, tags))
            else:
                skip += 1

            if len(json_points) == args.batch_size:
                # TODO - We shouldn't need to wrap this in try/except - should be handled by retry decorator
                try:
                    write_points(logger, client, json_points, line_count)
                except Exception as e:
                    logger.error("Retries exceeded. Giving up on this point.")
                json_points=[]
    except:
        # exc_type, exc_value, exc_traceback = sys.exc_info()
        # print("*** print_exception:")
        # traceback.print_exception(exc_type, exc_value, exc_traceback,file=sys.stdout)
        pass
    if json_points:
        # TODO - We shouldn't need to wrap this in try/except - should be handled by retry decorator
        try:
            write_points(logger, client, json_points, line_count)
        except Exception as e:
            logger.error("Retries exceeded. Giving up on this point.")

    logger.info("consumer complete")


def flush(q, t):
    for i in range(t):
        q.put('')

def main():
    logger = configure_logging('parse_operations', args)
    f = LogFile(args.input_file)
    q = Queue()
    d = Drainer(q)
    if not args.fork:
        reader = Thread(target=producer, args=(q, f,))
        reader.start()
        workers = []
        for i in range(args.workers):
            client = InfluxDBClient(host=args.influxdb_host, ssl=args.ssl, verify_ssl=False, port=8086, database=args.database)
            workers.append(Thread(target=consumer, args=[d, logger, client], name="Thread-{:02x}".format(i)))

        for worker in workers:
            worker.start()

        reader.join()
        flush(q, args.workers)
        for worker in workers:
            worker.join()

    else:
        client = InfluxDBClient(host=args.influxdb_host, ssl=args.ssl, verify_ssl=False, port=8086, database=args.database)
        jobs = []
        for i in range(args.workers):
            p = Process(target=consumer, args=[d, logger, client], name="Process-{:02x}".format(i))
            jobs.append(p)
            p.start()

        producer(q, f)
        flush(q, args.workers)

if __name__ == "__main__":
    sys.exit(main())

