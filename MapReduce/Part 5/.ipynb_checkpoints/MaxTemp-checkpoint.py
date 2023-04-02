from mrjob.job import MRJob

class MaxTemp(MRJob):
    def mapper(self, key, line):
        (LocID, Date, TempClass, Temp, Dum1, Dum2, Dum3, Dum4) = line.split(',')
        if TempClass == 'TMAX':
            yield LocID, float(Temp)/10
    
    def reducer(self,LocID,Temps):
        yield LocID, max(list(Temps))
        
if __name__ == '__main__':
    MaxTemp.run()