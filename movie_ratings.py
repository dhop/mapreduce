import json

from mapreduce import MapReduce, MapReduceConcurrent

class MovieRatings(MapReduceConcurrent):
    MOVIES = {}

    def setup(self):
        with open("movies.txt") as movies:
            line = movies.readline()
            while line:
                movie = json.loads(line)
                self.MOVIES[movie["id"]] = movie["name"]
                line = movies.readline()

    def mapper(self, line):
        entry = json.loads(line)
        yield (entry["movie_id"], entry["rating"])

    def reducer(self, key, entries):
        return self.MOVIES[key], float(sum(entries)) / max(len(entries), 1)


if __name__ == "__main__":

    mr = MovieRatings("ratings.txt")
    mr.run()