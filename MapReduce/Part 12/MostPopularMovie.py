# Type command: python MostPopularMovie.py u.data u.item
# basic example of mapper_raw

from mrjob.job import MRJob
from mrjob.step import MRStep

class MostPopularMovie(MRJob):
    
    def steps(self):
        return [MRStep(mapper_raw=self.mapper_raw_data)]
        
    def mapper_raw_data(self, input_path, input_uri):
        if 'u.data' in input_path:
            with open(input_path, encoding='ascii', errors='ignore') as file:
                for line in file:
                    data = line.split('\t')
                    yield data[0], data[1]
                    
if __name__ == '__main__':
    MostPopularMovie.run()