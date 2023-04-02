from mrjob.job import MRJob
import statistics

class AverageFriends(MRJob):
    def mapper(self, key, line):
        (UserID, Name, Age, Friends) = line.split(',')
        yield Age, int(Friends)
    
    def reducer(self, Age, FriendsByAge):
        yield Age, int(round(statistics.mean(list(FriendsByAge))))
    
if __name__ == '__main__':
    AverageFriends.run()