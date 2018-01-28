# MapReduce Engine

Your assignment is to write a map-reduce engine - essentially an engine that can execute programs structured in the map-reduce paradigm.

There are two components to this:

- A MapReduce program: a class or module that contains a map function/method and a reduce function/method. The form and signature of the mapper and reducer are exemplified bellow in pseudo-code:

```
class MapReduceProgram:
  # takes one line of input at a time (input are text files - line separate is the traditional \n)
  # returns (or emits) an iterator of key-value pairs
  method mapper(line: String) -> Iterator(key: KeyType, value: MapValueType)

  # takes a key and a list of values associated with that key as provided by the mapper function
  # returns the key and an arbitrary value (whose type can be different from the MapValueType)
  method reducer(key: KeyType, values: List[MapValueType]) -> (key: KeyType, value: ReducerValueType)
```

- The MapReduce Engine: the underlying class or module that orchestrates and executes programs implementing the MapReduce API described above. That's the bit of code you will provide.

Here's a famous word-count example in python:

word_counter.py

```python
import re

from your_library import MapReduce

class WordCounter(MapReduce):
  WORD = re.compile(r"[\w']+")

  def mapper(self, line):
    for word in WORD_RE.findall(line):
      yield (word, 1)

  def reducer(self, key, entries):
    return key, sum(entries)

```

An example input file:

if-kipling.txt

```
if you can dream and not make dreams your master
if you can think and not make thoughts your aim
if you can meet with Triumph and Disaster
and treat those two impostors just the same
if you can bear to hear the truth you’ve spoken
twisted by knaves to make a trap for fools
or watch the things you gave your life to broken
and stoop and build ’em up with worn-out tools
```

An example of executing our MapReduce program:

```bash
python word_counter.py if-kippling.txt
```

And the expected output:

```tsv
you 5
and 6
can 4
if 4
make 3
the 3
to 3
your 3
not 2
with 2
Disaster 1
Triumph 1
a 1
aim 1
bear 1
broken 1
build 1
by 1
dreams 1
dream 1
fools 1
for 1
gave 1
hear 1
impostors 1
just 1
knaves 1
life 1
master 1
meet 1
or 1
same 1
spoken 1
stoop 1
things 1
think 1
those 1
thoughts 1
tools 1
trap 1
treat 1
truth 1
twisted 1
two 1
up 1
watch 1
worn-out 1
you’ve 1
’em 1
```


## The assignment:

- Write an **engine** to execute MapReduce programs. It can be writen in either Python, Java or Scala. It doesn't necessarily have to be any similar to the examples provided, the important is the API and the output.
- Write a version of the word-count MapReduce **program** exemplified above, runnable by your engine.
- Write a new MapReduce **program** that given two datasets (movies and ratings), returns the AVG rating of each movie (in the format: Movie Name, Rating).
- Write a second **engine** (with same API and requirements) that makes use of more than one core. You should be able to run the same program in either engine without changes.


movies.txt
```JavaScript
{id: 1, name: 'The Matrix'}
{id: 2, name: 'Bicycle Thief'}
{id: 3, name: 'The Royal Tenenbaums'}
{id: 4, name: 'Ghost in the Shell'}
```

ratings.txt

```JavaScript
{user_id: 100, movie_id: 1, rating: 10}
{user_id: 101, movie_id: 1, rating: 9.2}
{user_id: 102, movie_id: 1, rating: 6}
{user_id: 103, movie_id: 1, rating: 8.5}
{user_id: 100, movie_id: 2, rating: 7.5}
{user_id: 101, movie_id: 2, rating: 10}
{user_id: 102, movie_id: 2, rating: 10}
{user_id: 103, movie_id: 2, rating: 9.5}
{user_id: 100, movie_id: 3, rating: 6}
{user_id: 101, movie_id: 3, rating: 8}
{user_id: 102, movie_id: 3, rating: 9}
{user_id: 103, movie_id: 3, rating: 9}
{user_id: 100, movie_id: 4, rating: 9}
{user_id: 101, movie_id: 4, rating: 7}
{user_id: 102, movie_id: 4, rating: 8}
{user_id: 103, movie_id: 4, rating: 8}
```