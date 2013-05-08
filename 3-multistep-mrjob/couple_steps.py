"""The classic MapReduce job: count the frequency of words.
"""
from mrjob.job import MRJob
import re

WORD_RE = re.compile(r"[\w']+")

class MRWordFreqCount(MRJob):
  def count_words(self, _, line):
    for word in WORD_RE.findall(line):
      yield (word.lower(), 1)

  def combiner(self, word, counts):
    yield (word, sum(counts))

  def reducer(self, word, counts):
    yield (word, sum(counts))

  def add_one_to_counts(self, word, counts):
    yield (word, counts+1)

  def steps(self):
     return [self.mr(mapper=self.count_words,
                     combiner=self.combiner, 
                     reducer=self.reducer),
                     self.mr(mapper=self.add_one_to_counts)]

if __name__ == '__main__':
  MRWordFreqCount.run()
