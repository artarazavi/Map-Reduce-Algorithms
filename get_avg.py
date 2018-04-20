
from mrjob.job import MRJob
from mrjob.step import MRStep
import sys
import re
from random import randint

class MRAvgFinder(MRJob):

  def steps(self):
    return [
        MRStep(mapper=self.mapper,
              reducer=self.reducer),
        MRStep(reducer=self.second_reducer)
    ]
  def mapper(self,_,line):
    lines = line.split(",")
    for num in lines:
      yield (randint(0, 9), (int(num),1) )
      #(machine number going to , number given to that machine)

  def reducer(self,key,values):
    avg = 0.0
    count = 0
    for num, c in values:
      #reconstruct average
      avg = ( (avg * count) + (num * c) )
      count += c
      avg = avg/count
    yield None, (avg,count)

  def second_reducer(self,key,values):
    avg = 0.0
    count = 0
    for num, c in values:
      avg = ( (avg * count) + (num * c) ) 
      count += c
      avg = avg / count
    yield "average", avg


if __name__ == '__main__':
    MRAvgFinder.run()