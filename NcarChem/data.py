#!/usr/bin/env python
# encoding: utf-8

## Get variables from the sql database
##
## Author: Ryan Orendorff <ryano@ucar.edu>
## Date: 21/07/11 10:31:47
##

## Syntax notes for coders who are not author
## - A double pound (##) is a comment, a single is commented code


## --------------------------------------------------------------------------
## Imports and Globals
## --------------------------------------------------------------------------
from collections import OrderedDict

## --------------------------------------------------------------------------
## Functions
## --------------------------------------------------------------------------

def createOrderedList(variables):
  var_list = []
  for var in variables:
    var_list.append((var, NCARVar(var)))

  return var_list


## --------------------------------------------------------------------------
## Classes
## --------------------------------------------------------------------------

class NCARVarSet(OrderedDict):
  def __init__(self, *variables):
    self._str = ""
    self._len = 0


    if 'datetime' not in variables:
      variables = ('datetime',) + variables
    self._str = str(variables)

    super(NCARVarSet,self).__init__(createOrderedList(variables))


  def __str__(self):
    return self._str

  def addData(self, data):
    if len(data) != 0:
      self._len += len(data)
      pos = 1
      for var in OrderedDict.__iter__(self):
        OrderedDict.__getitem__(self,var).addData([(column[0], column[pos]) for column in data])
        pos += 1

  def csv(self):
    output = "year,month,day,hour,minute,second" ## Always start with date.
    for key in self.keys():
      if key == 'datetime':
        continue
      output += ",%s" % key
    output += '\n'

    for counter in range(self._len):
      line = OrderedDict.__getitem__(self,'datetime')[counter].strftime("%Y,%m,%d,%H,%M,%S")
      for var in OrderedDict.__iter__(self):
        if var == "datetime":
          continue
        line += ',' + str(OrderedDict.__getitem__(self,var)[counter])
      line += '\n'
      output += line

    return output.rstrip('\n')


class NCARVar(OrderedDict):

  def __init__(self, name=None):
    self._name = name.lower()
    self._order = {}
    self._start_time = None
    self._end_time = None
    self._len = 0
    super(NCARVar, self).__init__()

  def __str__(self):
    return '%s (%s): %s' % (self._name, self._len, OrderedData.__str__(self))

  def __getitem__(self, index):
    try:
      return OrderedDict.__getitem__(self,self._order[index])
    except Exception, e:
      print "Out of bounds: %s[%d]" %(self._name, index)
      raise e

  def __iter__(self):
    for date in OrderedDict.__iter__(self):
      yield OrderedDict.__getitem__(self, date)

  def getName(self):
    return self._name

  def addData(self, data = [], process_algo = None):
    if len(data) == 0:
      return

    self.__mergeData(data)
    if OrderedDict.__len__(self) == 0:
      self._start_time = data[0][0]
      self._end_time = data[-1][0]


  ## Does not make any assumptions about the data inside the containter.
  def __mergeData(self, data):
    for row in data:
      OrderedDict.__setitem__(self, row[0], row[1])
      self._order[self._len] = row[0]
      self._len += 1
    self._end_time = data[-1][0]


  def clearData(self):
    OrderedDict.clear(self)
    self._order = {}
    self._len = 0
