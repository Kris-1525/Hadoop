from mrjob.job import MRJob

class MinTemp(MRJob):
    def mapper(self, key, line):
        (LocID, Date, TempClass, Temp, Dum1, Dum2, Dum3, Dum4) = line.split(',')
        if TempClass == 'TMIN':
            yield LocID, float(Temp)/10
    
    def reducer(self,LocID,Temps):
        yield LocID, min(list(Temps))
        
if __name__ == '__main__':
    MinTemp.run()