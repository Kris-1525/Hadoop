from mrjob.job import MRJob

class WordFrequency(MRJob):
    def mapper(self, key, line):
        for word in line.split(' '):
            yield word.lower(), 1
            
    def reducer(self, word, counts):
        yield word, sum(counts)
        
if __name__ == '__main__':
    WordFrequency.run()