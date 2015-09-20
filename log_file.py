__author__ = 'jimoleary'
import io

class LogFile:
    def __init__(self, name):
        """
        create a log file wrapper
        :param name: the name of the log file
        """
        self.name = name
        self.file = io.BufferedReader(io.FileIO(name))

    def __iter__(self):
        return self

    def readline(self):
        return self.file.readline()

    def tell(self):
        return self.file.tell()

    def next(self):
        return self.__next__()

    def __next__(self):
        line = self.readline()
        if not line:
            raise StopIteration
        return line

    def __getitem__(self, i):
        try:
            return self.next()
        except StopIteration:
            raise IndexError("end of input reached")