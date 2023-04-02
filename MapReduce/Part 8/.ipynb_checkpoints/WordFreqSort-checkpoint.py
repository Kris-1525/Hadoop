from mrjob.job import MRJob
from mrjob.step import MRStep
import re

class WordFreqSort(MRJob):
    def steps(self):
        return [MRStep(mapper=self.mapper_word_count,reducer=self.reducer_word_count),MRStep(mapper=self.mapper_word_sort,reducer=self.reducer_word_sort)]
        
    def mapper_word_count(self, key, line):
        regex = re.compile(r"[\w']+")
        words = regex.findall(line)
        for word in words:
            yield word.lower(), 1
            
    def reducer_word_count(self, word, counts):
        yield word, sum(counts)
        
    def mapper_word_sort(self, word, count):
        yield '%04d'%int(count), word
    
    def reducer_word_sort(self, count, words):
        for word in words:
            yield word, count
        
if __name__ == '__main__':
    WordFreqSort.run()