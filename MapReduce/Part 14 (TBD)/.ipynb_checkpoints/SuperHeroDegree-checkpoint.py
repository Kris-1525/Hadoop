from mrjob.job import MRJob
from mrjob.step import MRStep

class SuperHeroDegree(MRJob):
    def steps(self):
        return [MRStep(mapper=self., reducer=self.), MRStep(mapper=self., reducer=self.)]