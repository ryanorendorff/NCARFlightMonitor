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
from datafile import NRTFile

## --------------------------------------------------------------------------
## Functions
## --------------------------------------------------------------------------


def createOrderedList(variables):
  var_list = []
  for var in variables:
    var_list.append((var, NVar(var)))

  return var_list

def createOrderedListFromFile(file_name):
  nfile = NRTFile(file_name)
  olist = NVarSet(nfile.variables)
  olist.addData(nfile.data)
  return olist


## --------------------------------------------------------------------------
## Classes
## --------------------------------------------------------------------------


class NVarSet(OrderedDict):

  def __init__(self, var_start, *variables):
    self._str = ""
    self._rows = 0
    self._date = []

    if isinstance(var_start, list) and variables == ():
      variables = tuple(var_start)
    elif isinstance(var_start, tuple) and variables == ():
      variables = var_start
    else:
      variables = (var_start,) + variables

    self._str = str(variables)
    super(NVarSet, self).__init__(createOrderedList(variables))

  def __str__(self):
    return "NVarSet%s" % self._str

  def addData(self, data):
    if len(data) != 0:
      self._rows += len(data)
      pos = 1
      self._date += [column[0] for column in data]
      for var in OrderedDict.__iter__(self):
        OrderedDict.__getitem__(self, var).addData([(column[0], column[pos])\
                                                   for column in data])
        pos += 1

  def getDataAsList(self):
    labels = tuple(['DATETIME'] + self.keys())
    data = []
    for counter in range(self._rows):
      line = (self._date[counter],)
      for var in OrderedDict.__iter__(self):
        line += (OrderedDict.__getitem__(self, var)[counter],)
      data += (line,)
    return labels, data

  def clearData(self):
    for var in OrderedDict.__iter__(self):
      OrderedDict.__getitem__(self, var).clearData()
      self._date = []


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


## --------------------------------------------------------------------------
## Start command line interface (main)
## --------------------------------------------------------------------------

if __name__ == "__main__":
  import sys

  olist = createOrderedListFromFile(sys.argv[1])
  print olist
  print olist.getDataAsList()
