#!/usr/bin/env python3
from mrjob.job import MRJob
from mrjob.step import MRStep
import re

# we store the regular expression in an object
WORD_RE = re.compile(r"[\w']+")

class MRMostUsedWord(MRJob):

    # here we explicitly define the steps to take. We have two steps, the first
    # has a mapper, combiner and reducer, the second only a reducer.
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   combiner=self.combiner_count_words,
                   reducer=self.reducer_count_words),
            MRStep(reducer=self.reducer_find_max_word)
        ]

    def mapper_get_words(self, _, line):
        # yield each word in the line
        for word in WORD_RE.findall(line):
            yield (word.lower(), 1)

    def combiner_count_words(self, word, counts):
        # optimization: sum the words we've seen so far
        yield (word, sum(counts))

    def reducer_count_words(self, word, counts):
        # send all (num_occurrences, word) pairs to the same reducer.
        # num_occurrences is so we can easily use Python's max() function.
        num_occurences = sum(counts)
        # we are not yielding a key, so that all outputs are treated together
        yield None, (num_occurences, word)

    # discard the key; it is just None
    def reducer_find_max_word(self, _, word_count_pairs):
        # each item of word_count_pairs is (count, word),
        # so yielding one results in key=counts, value=word
        yield max(word_count_pairs)


if __name__ == '__main__':
    MRMostUsedWord.run()
