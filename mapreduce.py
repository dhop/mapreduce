import multiprocessing
import copy_reg
import types
import itertools

from collections import defaultdict


# This library makes use of multiprocessing, which requires all distributed work
# to be pickled to be sent off to different processes. Natively, pickling can't
# be done on instance methods of a class, as is necessary for this MapReduce
# library, so instead we define a custom pickle method to get around this.
# The following code snippet follows from this stackoverflow article:
# https://stackoverflow.com/questions/25156768/cant-pickle-type-instancemethod-using-pythons-multiprocessing-pool-apply-a

def _pickle_method(m):
    if m.im_self is None:
        return getattr, (m.im_class, m.im_func.func_name)
    else:
        return getattr, (m.im_self, m.im_func.func_name)

copy_reg.pickle(types.MethodType, _pickle_method)

# MapReduceCore is the base class for both MapReduce and MapReduceConcurrent
# it simply contains the placeholders for the mapper/reducer, as well as the
# init method that assigns an input path for the mapreduce job, as well as
# how many concurrent mappers and reducers to run, and iterables to hold
# intermediate results globally
class MapReduceCore:
    def __init__(self, input_path, mappers=8, reducers=8):
        self.input_path = input_path
        self.mappers = mappers
        self.reducers = reducers
        self.mapped = []
        self.reduced = []
        self.grouped = defaultdict(list)
        return

    def mapper(self, line):
        """
        method mapper(line: String) -> Iterator(key: KeyType, value: MapValueType)

        This is a placeholder for the mapper method, to be
        provided by the class inheriting MapReduce

        The mapper should be written to consume a line of
        text, and yield a key-value pair
        """

        yield

    def reducer(self, key, values):
        """
        method reducer(key: KeyType, values: List[MapValueType]) -> (key: KeyType, value: ReducerValueType)

        This is a placeholder for the reducer method, to be
        provided by the class inheriting MapReduce

        The reducer should be written to consume a key and
        an interable (List) of values associated with the
        given key, and return the key with an aggregated value
        """

        return


class MapReduceConcurrent(MapReduceCore):

    def concurrent_mapper(self, line):
        result = []
        result.extend(self.mapper(line))
        return result

    def concurrent_reducer(self, record):
        return self.reducer(*record)

    def run(self):
        """
        The run method will be called on the inherited class once
        the mapper and reducer have been properly set up. It will
        execute the MapReduce job in a multi-threaded context,
        thereby making use of more than one core as specified by
        the requirements. It will write a TSV file with the results.

        Whenever appropriate, it will also provide information about
        the status of the MapReduce job (progress bars, etc)
        """

        data = []

        with open(self.input_path) as ip:
            line = ip.readline()
            while line:
                data.append(line)
                line = ip.readline()

        # map_size = len(data) / self.mappers
        # mapper_lock = threading.Lock()

        pool_size = multiprocessing.cpu_count() * 2
        mapper_pool = multiprocessing.Pool(processes=self.mappers)

        mapper_chunk_size = len(data) / self.mappers

        mapped = mapper_pool.map(self.concurrent_mapper,
                                 data,
                                 chunksize=mapper_chunk_size)
        mapper_pool.close()
        mapper_pool.join()

        # Main thread group-by key
        # Since the mapping is 1-to-Many, each worker process returns a
        # list of their results, and the overall output is a list of lists.
        # Therefore, the usual grouping is a nested loop.
        # TODO: is this faster than just flattening mapped?
        for result in mapped:
            for key, value in result:
                self.grouped[key].append(value)

        grouped_items = self.grouped.items()

        reducer_pool = multiprocessing.Pool(processes=self.reducers)
        reducer_chunk_size = len(grouped_items) / self.reducers

        self.reduced = reducer_pool.map(self.concurrent_reducer,
                                        grouped_items,
                                        chunksize=reducer_chunk_size)
        reducer_pool.close()
        reducer_pool.join()

        for key, value in self.reduced:
            print "{}\t{}".format(key,value)


        print pool_size



class MapReduce(MapReduceCore):

    def run(self):
        """
        The run method will be called on the inherited class once
        the mapper and reducer have been properly set up. It will
        execute the MapReduce job in a single thread as specified
        by the requirements, and write a TSV file with results.

        Wherever appropriate, it will also provide information about the
        status of the MapReduce job (progress bars, etc)
        """


        # line-by-line approach -- could also read entire file into memory first?
        # TODO: test if reading into memory first is any faster (it's not)
        with open(self.input_path) as ip:
            line = ip.readline()
            while line:
                self.mapped.extend(self.mapper(line))
                line = ip.readline()

        # in the multi-core approach, there will be a combiner before this,
        # so remember to extend instead of append before sending off to reducers
        for key, value in self.mapped:
            self.grouped[key].append(value)

        # creates a generator for the reduced values, to be printed to stdout
        self.reduced = (self.reducer(key, entries) for key, entries in self.grouped.items())

        for key, value in self.reduced:
            print "{}\t{}".format(key,value)


