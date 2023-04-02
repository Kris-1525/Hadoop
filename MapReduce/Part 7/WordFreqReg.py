from mrjob.job import MRJob
import re

class WordFreqReg(MRJob):
    def mapper(self, key, line):
        regex = re.compile(r"[\w']+")
        words = regex.findall(line)
        for word in words:
            yield word.lower(), 1
            
    def reducer(self, word, counts):
        yield word, sum(counts)
        
if __name__ == '__main__':
    WordFreqReg.run()