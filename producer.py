#!/usr/bin/env python3
from utils import write_points
from influxdb import InfluxDBClient
from dateutil.parser import parse
from threading import Thread
from multiprocessing import Process, Queue, Pool

__author__ = 'jimoleary'

class Producer(object):
    def __init__(self, io, q = None):
        if q is None:
            self.q = Queue()
        else:
            self.q = q
        self.file = io
        self.closed = False

    def __iter__(self):
        if self.closed:
            raise StopIteration
        while True:
            yield self.q.get()

    def start(self):
        line_count = 0
        for line in self.file:
            line_count += 1
            if line:
                line = line
                self.q.put([line, line_count])
        self.q.put('')

    def flush(self, t):
        for i in range(t):
            self.q.put('')