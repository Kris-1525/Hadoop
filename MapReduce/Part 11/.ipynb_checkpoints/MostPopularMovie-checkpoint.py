from mrjob.job import MRJob
from mrjob.step import MRStep

class MostPopularMovie(MRJob):
    def configure_args(self):
        super(MostPopularMovie,self).configure_args()
        self.add_file_arg('--items')
        
    def steps(self):
        return [MRStep(mapper=self.mapper_movie,reducer_init=self.reducer_movie_a,reducer=self.reducer_movie_b),
                MRStep(mapper=self.mapper_max,reducer=self.reducer_max)]
    
    def mapper_movie(self, key, line):
        UserID, MovieID, Rating, Timestamp = line.split('\t')
        yield MovieID, 1
        
    def reducer_movie_a(self):
        self.movieNames = {}
        
        with open('u.item', encoding='ascii', errors='ignore') as item_file:
            for line in item_file:
                line_list = line.split('|')
                self.movieNames[line_list[0]] = line_list[1]
        
    def reducer_movie_b(self, MovieID, Users):
        yield None, (sum(Users), self.movieNames[MovieID])
        
    def mapper_max(self, Non, UMPair):
        yield Non, UMPair
        
    def reducer_max(self, Non, UMPairs):
        yield max(UMPairs)

if __name__ == '__main__':
    MostPopularMovie.run()