import re

from mapreduce import MapReduce, MapReduceConcurrent

class WordCounter(MapReduceConcurrent):
    WORD = re.compile(r"[\w']+")

    def mapper(self, line):
        for word in self.WORD.findall(line):
            yield (word, 1)

    def reducer(self, key, entries):
        return key, sum(entries)


if __name__ == "__main__":
    wc = WordCounter("shakespeare.txt", processes=8)
    wc.run()