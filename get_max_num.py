from mrjob.job import MRJob
from mrjob.step import MRStep
import sys
import re
from random import randint

class MRMaxFinder(MRJob):

  def steps(self):
    return [
        MRStep(mapper=self.mapper,
               reducer=self.reducer),
        MRStep(reducer=self.second_reducer)
    ]
  def mapper(self,_,line):

      yield (randint(0, 9) ,line)
      #(machine number going to , number given to that machine)
        
  def reducer(self,key,values):
    yield None, max(values)

  def second_reducer(self,key,values):
    yield "Maximum value", max(values)


if __name__ == '__main__':
    MRMaxFinder.run()

