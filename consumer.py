from utils import write_points
from influxdb import InfluxDBClient
from dateutil.parser import parse
from threading import Thread
from multiprocessing import Process
import time

__author__ = 'jimoleary'


class Consumer(object):
    def __init__(self, q, logger, args, i):
        self.q = q
        self.logger = logger
        self.args = args
        self.id = i
        self._worker = None
        self._client = None
        self.sent = 0

    def tags(self):
        return {
            'project': self.args.project,
            'hostname': self.args.hostname,
        }

    def create_point(self, timestamp, measurement_name, values, tags):
        return {
            "measurement": measurement_name,
            "tags": tags,
            "time": timestamp,
            "fields": values
        }

    @property
    def client(self):
        if self._client is None:
            self._client = InfluxDBClient(host=self.args.influxdb_host, ssl=self.args.ssl, verify_ssl=False, port=8086,
                                          database=self.args.database)
        return self._client

    def write_points(self, json_points, line_count):
        write_points(self.logger, self.client, json_points, line_count)
        self.sent += len(json_points)

    def start(self):
        if self._worker is None:
            if not self.args.fork:
                self._worker = Thread(target=self.process, name="Thread-{:02x}".format(self.id))
            else:
                self._worker = Process(target=self.process, name="Process-{:02x}".format(self.id))
        self._worker.start()

    def join(self):
        if self._worker is not None:
            self._worker.join()

    def process(self):
        self.logger.info("consumer starting")
        json_points = []
        skip = 0
        line_count = 0
        try:
            for tupple in self.q:
                line, line_count = tupple
                self.logger.debug("reading  {:< 6} {}.".format(line_count, line))
                # zip_longest will backfill any missing values with None, so we need to handle this,
                # otherwise we'll miss the last batch
                # the main thread uses '' to exit
                if line is None or line == '':
                    break
                line = line.decode('UTF-8').rstrip("\n")
                if line and line.endswith("ms"):
                    values = {}
                    tags = self.tags()
                    try:
                        tags['operation'] = line.split("] ", 1)[1].split()[0]
                    except IndexError as e:
                        self.logger.error("Unable to parse line - {} - {}".format(e, line))
                        break
                    if tags['operation'] in ['command', 'query', 'getmore', 'insert', 'update', 'remove', 'aggregate',
                                             'mapreduce']:
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
                        json_points.append(self.create_point(timestamp, "operations", values, tags))
                    self.logger.debug("writing  {:< 6} {}.".format(line_count, line))
                else:
                    skip += 1
                    self.logger.debug("skipping  {:< 6} {}.".format(line_count, line))

                if len(json_points) == self.args.batch_size:
                    # TODO - We shouldn't need to wrap this in try/except - should be handled by retry decorator
                    try:
                        self.write_points(json_points, line_count)
                    except Exception:
                        self.logger.error("Retries exceeded. Giving up on this point.")
                    json_points = []
        except:
            # import sys, traceback
            # m = traceback.format_exc()
            # self.logger.warn(m)
            pass
        if json_points:
            # TODO - We shouldn't need to wrap this in try/except - should be handled by retry decorator
            try:
                self.write_points(json_points, line_count)
            except Exception:
                self.logger.error("Retries exceeded. Giving up on this point.")

        time.sleep(1)
        self.logger.info("consumer complete {}".format(self.sent))
        return self.sent