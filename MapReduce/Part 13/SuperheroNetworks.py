# python SuperheroNetworks.py Marvel+Graph.txt --names=Marvel+Names.txt
from mrjob.job import MRJob
from mrjob.step import MRStep

class SuperheroNetworks(MRJob):
    
    def configure_args(self):
        super(SuperheroNetworks, self).configure_args()
        self.add_file_arg('--names')
        
    def steps(self):
        return [MRStep(mapper=self.mapper_count, reducer_init=self.reducer_count_init, reducer=self.reducer_count), 
                MRStep(mapper=self.mapper_max, reducer=self.reducer_max)]
    
    def mapper_count(self, key, line):
        ids = line.split()
        yield int(ids[0]), len(ids[1:])
        
    def reducer_count_init(self):
        self.herosNames = {}
        
        with open('Marvel+Names.txt', encoding = 'ascii', errors = 'ignore') as file:
            for line in file:
                data = line.split('"')
                self.herosNames[int(data[0])] = data[1]
    
    def reducer_count(self, sid, fids):
        yield None, (sum(fids), self.herosNames[sid])
        
    def mapper_max(self, Non, friends_name):
        yield Non, friends_name
        
    def reducer_max(self, Non, friends_names):
        yield max(friends_names)

if __name__ == '__main__':
    SuperheroNetworks.run()