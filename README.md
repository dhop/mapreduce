# mapreduce -- a Python MapReduce engine

`mapreduce` is a simple Python module for executing programs in the MapReduce paradigm. It offers two distinct classes that one could build a MapReduce program with:

### MapReduce

The simplest implementation of the MapReduce pattern. This class executes MapReduce programs in a single process, simply by processing all maps and reduces sequentially. To use this class you must inherit it and implement your own `mapper` and `reducer`. Here is an example:

```python
from mapreduce import MapReduce

class WordCounter(MapReduce):
    WORD = re.compile(r"[\w']+")

    def mapper(self, line):
        for word in self.WORD.findall(line):
            yield (word, 1)

    def reducer(self, key, entries):
        return key, sum(entries)
```

Additionally, you may define `setup` and `cleanup` methods in the same class. `setup` is executed before the mappers run, and `cleanup` is executed right after the last reducer. See the `movie_ratings.py` example for a use case of this.

To run your MapReduce job, you need only instantiate the class with the input file path, and use the `run` method. Here is an example:

```python
if __name__ == "__main__"
    wc = WordCounter("shakespeare.txt")
    wc.run()
```


### MapReduceConcurrent

This class, in contrast to its simpler counterpart, makes use of python's built-in `multiprocessing` library to execute mappers and reducers in a distributed environment. When instantiating this class, you may also provide a `processes` parameter. This determines how many processes will be spawned in the worker pool that carries out the map and reduce tasks. Here is an example with our WordCounter class from above:

```python
class WordCounter(MapReduceConcurrent):

    ...

if __name__ == "__main__"
    wc = WordCounter("shakespeare.txt", processess=8)
    wc.run()
```

MapReduceConcurrent will start the MapReduce job by creating a pool of processes. It reuses this pool throughout the job to submit concurrent map and reduce tasks to different processes, each with an equally divide group of work to do. It uses the built-in `Pool.map` to divide the work, with a `chunksize` that is calculated from the amount of records in the Iterable divided by the amount of processes in the pool.


