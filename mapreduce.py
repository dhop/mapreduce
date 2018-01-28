from collections import defaultdict

class MapReduce:

    def __init__(self, input_path):
        self.input_path = input_path
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

    def run(self):
        """
        The run method will be called on the inherited class once
        the mapper and reducer have been properly set up. It will
        execute the MapReduce program in a single thread as specified
        by the requirements, and write a TSV file with results.

        Wherever appropriate, it will also provide information about the
        status of the MapReduce operation (progress bars, etc)
        """
        mapped = []
        grouped = defaultdict(list)

        # line-by-line approach -- could also read entire file into memory first?
        # TODO: test if reading into memory first is any faster
        with open(self.input_path) as ip:
            line = ip.readline()
            while line:
                mapped.extend(self.mapper(line))
                line = ip.readline()

        # in the multi-core approach, there will be a combiner before this,
        # so remember to extend instead of append before sending off to reducers
        for key, value in mapped:
            grouped[key].append(value)

        # creates a generator for the reduced values, to be printed to stdout
        reduced = (self.reducer(key, entries) for key, entries in grouped.items())

        for key, value in reduced:
            print "{}\t{}".format(key,value)


