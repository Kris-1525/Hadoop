# python RecommendMovieEMR.py u.data --items=u.item -r emr
from math import sqrt
from mrjob.job import MRJob
from mrjob.step import MRStep
from itertools import combinations

class RecommendMovieEMR(MRJob):
    def steps(self):
        return [MRStep(mapper=self.mapper_user_movies, reducer=self.reducer_user_movies),
                MRStep(mapper=self.mapper_movie_ratings, reducer=self.reducer_movie_ratings),
                MRStep(mapper=self.mapper_movie_recommend, mapper_init=self.movie_name_extract, reducer=self.reducer_movie_recommend)]

    def configure_args(self):
        super(RecommendMovieEMR, self).configure_args()
        self.add_file_arg('--items', help = 'Path to u.item file')

    def movie_name_extract(self):
        self.movieName = {}

        with open('u.item', encoding = 'ascii', errors = 'ignore') as file:
            for line in file:
                line = line.split('|')
                self.movieName[int(line[0])] = line[1]

    def mapper_user_movies(self, key, line):
        UserID, MovieID, Rating, Timestamp = line.split('\t')
        yield UserID, (MovieID, float(Rating))

    def reducer_user_movies(self, UserID, MovieRatings):
        yield UserID, list(MovieRatings)

    def mapper_movie_ratings(self, UserID, MovieRatings):
        for MovieRatings1, MovieRatings2 in combinations(MovieRatings, 2):
            MovieID1 = MovieRatings1[0]
            Ratings1 = MovieRatings1[1]
            MovieID2 = MovieRatings2[0]
            Ratings2 = MovieRatings2[1]
            yield (MovieID1, MovieID2), (Ratings1, Ratings2)
            yield (MovieID2, MovieID1), (Ratings2, Ratings1)

    def cosine_similarity_ratings(self, RatingTuples):
        sum_xx = 0
        sum_yy = 0
        sum_xy = 0
        numPairs = 0
        for RatingTuple in RatingTuples:
            sum_xx = sum_xx + RatingTuple[0]*RatingTuple[0]
            sum_yy = sum_yy + RatingTuple[1] * RatingTuple[1]
            sum_xy = sum_xy + RatingTuple[0] * RatingTuple[1]
            numPairs = numPairs + 1

        denominator = sqrt(sum_xx)+sqrt(sum_yy)
        score = float(sum_xy)/float(denominator)
        return [score, numPairs]

    def reducer_movie_ratings(self, MovieTupleID, RatingTuples):
        RatingTuples = list(RatingTuples)
        score, numPairs = self.cosine_similarity_ratings(RatingTuples)
        if (score > 0.95) & (numPairs > 10):
            yield MovieTupleID, (score, numPairs)

    def mapper_movie_recommend(self, MovieTupleID, ScoreNumPairs):
        MovieTupleID = list(MovieTupleID)
        ScoreNumPairs = list(ScoreNumPairs)
        yield (self.movieName[int(MovieTupleID[0])], ScoreNumPairs[0]), \
            (self.movieName[int(MovieTupleID[1])], ScoreNumPairs[1])

    def reducer_movie_recommend(self, MovieScoreID, MovieNumPairsID):
        MovieScoreID = list(MovieScoreID)
        for MovieNumPairID in MovieNumPairsID:
            yield str(MovieScoreID[0]), (str(MovieNumPairID[0]), str(MovieScoreID[1]), str(MovieNumPairID[1]))

if __name__ == '__main__':
    RecommendMovieEMR.run()