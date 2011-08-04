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
    var_list.append((var, NVar(var)))

  return var_list


## --------------------------------------------------------------------------
## Classes
## --------------------------------------------------------------------------


class NVarSet(OrderedDict):

  def __init__(self, *variables):
    self._str = ""
    self._rows = 0


    if 'datetime' not in variables:
      variables = ('datetime',) + variables
    self._str = str(variables)

    super(NVarSet, self).__init__(createOrderedList(variables))

  def addData(self, data):
    if len(data) != 0:
      self._rows += len(data)
      pos = 1
      for var in OrderedDict.__iter__(self):
        OrderedDict.__getitem__(self, var).addData([(column[0], column[pos])\
                                                   for column in data])
        pos += 1

  def csv(self):
    output = "YEAR,MONTH,DAY,HOUR,MINUTE,SECOND" ## Always start with date.
    for key in self.keys():
      if key == 'datetime':
        continue
      output += ",%s" % key.upper()
    output += '\n'

    for counter in range(self._rows):
      line = OrderedDict.__getitem__(self, 'datetime')[counter].strftime("%Y,%m,%d,%H,%M,%S")
      for var in OrderedDict.__iter__(self):
        if var == "datetime":
          continue
        line += ',' + str(OrderedDict.__getitem__(self, var)[counter])
      line += '\n'
      output += line

    return output.rstrip('\n')


  def clearData(self):
    for var in OrderedDict.__iter__(self):
      OrderedDict.__getitem__(self, var).clearData()

class NVar(OrderedDict):

  def __init__(self, name=None):
    self._name = name.lower()
    self._order = {}
    super(NVar, self).__init__()

  def __getitem__(self, index):
    if isinstance(index, int):
      if index < 0:
        return OrderedDict.__getitem__(self, self._order[(OrderedDict.__len__(self)-1) + index])
      else:
        return OrderedDict.__getitem__(self, self._order[index])
    else:
      return OrderedDict.__getitem__(self, index)
  def getDate(self, index):
    if index < 0:
      return self._order[(OrderedDict.__len__(self) - 1) + index]
    else:
      return self._order[index]


  def getName(self):
    return self._name

  def addData(self, data=[]):
    if len(data) == 0:
      return

    self.__mergeData(data)

  ## Does not make any assumptions about the data inside the containter.
  def __mergeData(self, data):
    for row in data:
      self._order[OrderedDict.__len__(self)] = row[0]
      OrderedDict.__setitem__(self, row[0], row[1])

  def clearData(self):
    OrderedDict.clear(self)
    self._order = {}
