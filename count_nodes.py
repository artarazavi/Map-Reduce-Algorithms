
from mrjob.job import MRJob
from mrjob.step import MRStep
import sys
import re
from random import randint

class MRCountFinder(MRJob):

  def steps(self):
    return [
        MRStep(mapper=self.mapper,
              combiner = self.combiner,
              reducer=self.reducer),
        MRStep(reducer=self.second_pass_reducer)
    ]
  def mapper(self,_,line):
    lines = line.split()
    key = lines[0]
    value = lines[1]
    yield (key, 1)
    yield (value, 1)
      #(machine number going to , number given to that machine)

  def combiner(self,key,values):
    yield key, 1
  
  def reducer(self,key,values):
    yield None, 1

  def second_pass_reducer(self,key,values):
    yield "Number of nodes", sum(values)
  

  

if __name__ == '__main__':
    MRCountFinder.run()