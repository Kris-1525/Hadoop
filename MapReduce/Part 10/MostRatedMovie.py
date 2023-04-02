from mrjob.job import MRJob
from mrjob.step import MRStep

class MostRatedMovie(MRJob):
    def steps(self):
        return [MRStep(mapper=self.mapper_movie,reducer=self.reducer_movie),MRStep(reducer=self.reducer_sort)]
        
    def mapper_movie(self, key, line):
        UserID, MovieID, Rating, Timestamp = line.split('\t')
        yield MovieID, 1 
        
    def reducer_movie(self, MovieID, Users):
        yield None, (sum(Users), MovieID)
        
    def reducer_sort(self, Non, MovieNusers):
        yield max(MovieNusers)

if __name__ == '__main__':
    MostRatedMovie.run()