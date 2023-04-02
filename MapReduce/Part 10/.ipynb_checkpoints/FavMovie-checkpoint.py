from mrjob.job import MRJob
from mrjob.step import MRStep

class FavMovie(MRJob):
    def steps(self):
        return [MRStep(mapper=self.mapper_movie,reducer=self.reducer_movie), MRStep(mapper=self.mapper_sort,reducer=self.reducer_sort)]
    
    def mapper_movie(self, key, line):
        User, Movie, Rating, Timestamp = line.split('\t')
        yield Movie, User
    
    def reducer_movie(self, Movie, Users):
        yield int(Movie), len(list(Users))
        
    def mapper_sort(self, Movie, Nusers):
        yield "%04d"%int(Nusers), "%04d"%int(Movie)
        
    def reducer_sort(self, Nusers, Movies):
        for Movie in Movies:
            yield Movie, Nusers
        
if __name__ ==  '__main__':
    FavMovie.run()