#!/usr/bin/env python3
from mrjob.job import MRJob
import re

class MRCountWords(MRJob):

    # The mapper creates a key for every word and a value of 1
    def mapper(self, _, line):
        for word in line.split():
            # regular expression to remove all non-word characters
            word = re.sub("[^a-zA-Z]+", "", word)
            # make it lowercase and yield the word as key and 1 as value
            yield word.lower(), 1

    # The reducer sums over all the values of the keys
    def reducer(self, word, occurences):
        yield word, sum(occurences)

if __name__ == '__main__':
    MRCountWords.run()
