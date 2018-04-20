from mrjob.job import MRJob
from mrjob.step import MRStep
import sys
import re
from random import randint
import numpy
import statistics

class MRAvgHopFinder(MRJob):

  def steps(self):
    return [
        MRStep(mapper=self.mapper,
              reducer=self.reducer),
        MRStep(reducer=self.second_reducer),
        MRStep(reducer=self.third_reducer),
        MRStep(reducer=self.fourth_reducer)
        #combiner = self.combiner,
    ]
  def mapper(self,_,line):
    lines = line.split()
    #sources
    s = lines[0]
    #destinations
    d = lines[1]
    if(s==d):
      #to deal with self loops 
      yield ("self", (s,d))
      yield (s, (s,d))
    else:
      yield (s, (s,d))
      yield (d, (s,d))
      #(machine number going to , number given to that machine)

  #aggregate all edges
  def reducer(self,key,values):
    arr = []
    for d in values:
      arr.append(d)
    yield key,arr

  def second_reducer(self,key,values):
    store_dest = []
    remove = []
    #count all self loops as second hop
    if(key == "self"):
      for v in values:
        for m in v:
          yield m[0], [m[1]]
    else:
      for v in values:
        for m in v:
          #nodes pointing to key
          if(m[0] != key):
            remove.append(m[0])
          #nodes reachable from key aka 2nd hop dest
          else:
            store_dest.append(m[1])
      if(len(remove) > 0):
        for r in remove:
          if(len(store_dest)>0):
            yield r, store_dest

  def third_reducer(self,key,values):
    dest = []
    for v in values:
      if(len(v)>0):
        dest.extend(v)
    #use none to aggregate and length of all nodes reachable
    #by second hop from key
    #using set because you dont want to count repeating edges
    #aka cant reach same node twice
    yield None, len(set(dest))
    #yield key, dest

  def fourth_reducer(self,key,values):
    counter = 265214
    avg = 0.0
    c=0
    arr = []
    for v in values:
      c+=1
      avg += float(v)
      arr.append(v)

    n = counter - c
    listofzeros = [0] * n
    arr.extend(listofzeros)
    median = statistics.median(arr) 
    avg = float(avg/counter)
    yield "average number nodes reachable in 2 hops", avg
    yield "median number of nodes reachable in 2 hops", median


if __name__ == '__main__':
    MRAvgHopFinder.run()