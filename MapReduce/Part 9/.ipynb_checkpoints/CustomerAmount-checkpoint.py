from mrjob.job import MRJob
from mrjob.step import MRStep

class CustomerAmount(MRJob):
    def steps(self):
        return [MRStep(mapper=self.mapper_total_amount,reducer=self.reducer_total_amount),MRStep(mapper=self.mapper_amount_sort,reducer=self.reducer_amount_sort)]
    
    def mapper_total_amount(self, key, line):
        (Customer, Item, Amount) = line.split(',')
        yield int(Customer), float(Amount)
        
    def reducer_total_amount(self, Customer, Amounts):
        yield Customer, sum(Amounts)
        
    def mapper_amount_sort(self, Customer, Amount):
        yield round(float(Amount),2), int(Customer)
        
    def reducer_amount_sort(self, Amount, Customers):
        for Customer in Customers:
            yield Customer, Amount

if __name__ == '__main__':
    CustomerAmount.run()