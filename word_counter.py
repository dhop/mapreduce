import re

from mapreduce import MapReduce

class WordCounter(MapReduce):
  WORD = re.compile(r"[\w']+")

  def mapper(self, line):
    for word in self.WORD.findall(line):
      yield (word, 1)

  def reducer(self, key, entries):
    return key, sum(entries)


if __name__ == "__main__":

    wc = WordCounter("if-kipling.txt")
    wc.run()