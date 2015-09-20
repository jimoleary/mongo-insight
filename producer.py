from influxdb import InfluxDBClient
from dateutil.parser import parse
from multiprocessing import Process, Queue, Pool

__author__ = 'jimoleary'


class Producer(object):
    def __init__(self, io, logger, q=None):
        if q is None:
            self.q = Queue()
        else:
            self.q = q
        self.file = io
        self.logger = logger
        self.line_count = 0

    def __iter__(self):
        while True:
            v = self.q.get()
            if v == '' or v is None:
                raise StopIteration
            yield v

    def start(self):
        a = self.adapter()
        for tupple in a:
            if tupple:
                self.q.put(tupple)

    def adapter(self):
        def decorate(line):
            self.line_count += 1
            return line, self.line_count
        return map(decorate, self.file)

    def flush(self, t):
        for i in range(t):
            self.q.put('')