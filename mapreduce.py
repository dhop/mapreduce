import multiprocessing
import copy_reg
import types
import itertools

from subprocess import check_output
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


class MapReduceCore:
    """
    MapReduceCore is the base class for both MapReduce and MapReduceConcurrent
    it simply contains the placeholders for the setup/cleanup/mapper/reducer,
    the init method that assigns an input path for the mapreduce job, as well as
    how many concurrent processes to run in the Pool.
    """
    def __init__(self, input_path, processes=4):
        self.input_path = input_path
        self.processes = processes
        return

    def setup(self):
        """
        This is a placeholder for the setup method, to be called before all
        mappers and reducers take place. Traditionally, in mapreduce there would
        be a separate setup() for both the mapper and the reducer, but for the
        sake of simplicity I have only included one.
        """
        pass

    def cleanup(self):
        """
        This is a placeholder for the cleanup method, to be called at the end of
        execution of all mappers and reducers. As stated above for the setup()
        method, I have only included one of these for the sake of simplicity
        """
        pass

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
    """
    MapReduceConcurrent is the multi-process implementation of our MapReduce engine
    """

    def load_and_map(self, line):
        result = []
        result.extend(self.mapper(line))
        return result

    def reduce(self, record):
        return self.reducer(*record)

    def run(self):
        """
        The run method will be called on the inherited class once
        the mapper and reducer have been properly set up. It will
        execute the MapReduce job in a multi-threaded context,
        thereby making use of more than one core as specified by
        the requirements. It will write the results to stdout.
        """
        self.setup()

        data = []
        mapped = []
        grouped = defaultdict(list)
        lines = int(check_output(['wc', '-l', self.input_path]).split()[0])

        # to save on overhead, this pool will be used for both
        # concurrent map and reduce tasks
        pool = multiprocessing.Pool(processes=self.processes)

        # the chunksize for load_and_map is determined by the total number of
        # lines in the input file divided by the number of processes available
        # in the pool. this ensures that each process receieves equal work.
        mapper_chunksize = lines / self.processes

        # files can be treated as iterables of lines, so to extract each line
        # the built-in Pool.map is used to evenly distribute work across
        # the available processes. this is a blocking operation, and all results
        # will be in the mapped array upon completion.
        with open(self.input_path) as input_file:
            mapped = pool.map(self.load_and_map,
                                     input_file,
                                     chunksize=mapper_chunksize)

        # main process group-by
        # since the mapping is 1-to-many, each worker process returns a
        # list of their results, and the overall output is a list of lists.
        # therefore, we must add to the map in a nested loop when grouping.
        for result in mapped:
            for key, value in result:
                grouped[key].append(value)

        # extract items in the map, to be sent to the reducers
        grouped_items = grouped.items()

        # the chunksize for the reducer is simply the number of unique items
        # divided by the number of processes available in the Pool. this is
        # done to ensure equal work among the processes.
        reducer_chunksize = len(grouped_items) / self.processes

        # once more, we use the built-in Pool.map to execute the distributed
        # computation evenly over the process Pool.
        reduced = pool.map(self.reduce,
                           grouped_items,
                           chunksize=reducer_chunksize)

        # we are done with submitting tasks to the pool, so it must be closed
        # and joined with the main process
        pool.close()
        pool.join()

        # output all reduced values to stdout
        for key, value in reduced:
            print "{}\t{}".format(key,value)

        self.cleanup()


class MapReduce(MapReduceCore):
    """
    MapReduce is the single-process implementation of our MapReduce engine
    """

    def run(self):
        """
        The run method will be called on the inherited class once
        the mapper and reducer have been properly set up. It will
        execute the MapReduce job in a single thread as specified
        by the requirements, and write the results to stdout.
        """
        self.setup()
        mapped = []
        reduced = []
        grouped = defaultdict(list)

        # iterate through each line in the file, and send each line off
        # to the mapper as they are encountered
        with open(self.input_path) as ip:
            line = ip.readline()
            while line:
                mapped.extend(self.mapper(line))
                line = ip.readline()

        # group all mapped values by their key by incrementally constructing
        # a map. since grouped is a defaultdict, it will handle the case where
        # the key does not yet exist
        for key, value in mapped:
            grouped[key].append(value)

        # creates a generator for the reduced values, to be printed to stdout
        reduced = (self.reducer(key, entries) for key, entries in grouped.items())

        # output all reduced values to stdout
        for key, value in reduced:
            print "{}\t{}".format(key,value)

        self.cleanup()

