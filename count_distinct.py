
from mrjob.job import MRJob
from mrjob.step import MRStep
import sys
import re
from random import randint

class MRDistinctFinder(MRJob):

  def steps(self):
    return [
        MRStep(mapper=self.mapper,
              reducer=self.reducer),
        MRStep(reducer=self.second_reducer)
    ]
  def mapper(self,_,line):
    lines = line.split(",")
    for num in lines:
      yield (num, 1)
      #(machine number going to , number given to that machine)

  def reducer(self,key,values):
    yield None, 1
  
  def second_reducer(self,key,values):
    yield "Number of distinct values", sum(values)
  

  

if __name__ == '__main__':
    MRDistinctFinder.run()