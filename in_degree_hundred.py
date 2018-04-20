from mrjob.job import MRJob
from mrjob.step import MRStep
import sys
import re
from random import randint

class MRHundFinder(MRJob):

  def steps(self):
    return [
        MRStep(mapper=self.mapper,
              reducer=self.reducer),
        MRStep(reducer=self.second_reducer),
        MRStep(reducer=self.third_reducer)
        #combiner = self.combiner,
    ]
  def mapper(self,_,line):
    lines = line.split()
    key = lines[0]
    value = lines[1]
    yield (value, 1)
      #(machine number going to , number given to that machine)

  
  def reducer(self,key,values):
    yield key, sum(values)

  def second_reducer(self,key,values):
    for val in values:
      if(val > 100):
        yield None, 1
  def third_reducer(self,key,values):
    yield "number of nodes with degree > 100", sum(values)


if __name__ == '__main__':
    MRHundFinder.run()