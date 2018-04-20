from mrjob.job import MRJob
from mrjob.step import MRStep
import sys
import re
from random import randint
import numpy
import statistics

class MRAvgMedFinder(MRJob):

  def steps(self):
    return [
        MRStep(mapper=self.mapper,
              reducer=self.reducer),
        MRStep(reducer=self.second_reducer)
        #combiner = self.combiner,
    ]
  def mapper(self,_,line):
    lines = line.split()
    key = lines[0]
    value = lines[1]
    yield ("out" ,(key, 1))
    yield ("in" ,(value, 1))
      #(machine number going to , number given to that machine)

  
  def reducer(self,key,values):
    val_sum = 0
    for k, c in values:
      val_sum += c
    yield key, val_sum

  def second_reducer(self,key,values):
      counter = 265214
      avg = 0.0
      arr = []
      c = 0
      for val in values:
        c+=1
        avg += val
        arr.append(val)
      avg = avg / counter
      n = counter - c
      listofzeros = [0] * n
      arr.extend(listofzeros)
      median = statistics.median(arr) 
      yield ("average " +key+ "_degree"), avg
      yield ("median " +key+"_degree") , median

if __name__ == '__main__':
    MRAvgMedFinder.run()