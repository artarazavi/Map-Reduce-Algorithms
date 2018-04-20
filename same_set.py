
from mrjob.job import MRJob
from mrjob.step import MRStep
import sys
import re
from random import randint

class MRSetFinder(MRJob):

  def steps(self):
    return [
        MRStep(mapper=self.mapper,
              reducer=self.reducer),
        MRStep(reducer=self.second_reducer)
    ]
  def mapper(self,_,line):
    lines = line.split(",")
    for num in lines:
      yield (randint(0, 9), int(num) )
      #(machine number going to , number given to that machine)

  def reducer(self,key,values):
    arr = []
    for val in values:
      if val not in arr:
        arr.append(val)
    yield None, arr
  
  def second_reducer(self,key,values):
    arr = []
    for val in values:
      for v in val:
        if v not in arr:
          arr.append(v)
    yield "same set of input", arr
  

  

if __name__ == '__main__':
    MRSetFinder.run()