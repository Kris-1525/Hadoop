from mrjob.job import MRJob

class MRMoviesWatched(MRJob):
    def mapper(self, key, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield "%04d"%int(userID), movieID
    
    def reducer(self, userID, movieIDs):
        yield userID, len(list(movieIDs))
        
if __name__ == '__main__':
    MRMoviesWatched.run()