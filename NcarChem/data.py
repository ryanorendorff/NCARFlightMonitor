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

def createOrderedDict(variables):
  var_list = []
  for var in variables:
    var_list.append((var, NCARVar(var)))

  return OrderedDict(var_list)


## --------------------------------------------------------------------------
## Classes
## --------------------------------------------------------------------------

class NCARVarSet():
  def __init__(self, *variables):
    self._vars = None
    self._str = str(variables)
    self._len = 0

    if 'datetime' not in variables:
      variables = ('datetime',) + variables

    self._vars = createOrderedDict(variables)


  def __str__(self):
    return self._str

  def __iter__(self):
    for var in self._vars:
      yield self._vars[var]

  def keys(self):
    return self._vars.keys()

  def addData(self, data):
    if len(data) != 0:
      self._len += len(data)
      pos = 1
      for var in self._vars:
        self._vars[var].addData([(column[0], column[pos]) for column in data])
        pos += 1

  def csv(self):
    output = "year,month,day,hour,minute,second" ## Always start with date.
    for key in self.keys():
      if key == 'datetime':
        continue
      output += ",%s" % key
    output += '\n'

    for counter in range(self._len):
      line = self._vars['datetime'][counter][1].strftime("%Y,%m,%d,%H,%M,%S")
      for var in self._vars:
        if var == "datetime":
          continue
        line += ',' + str(self._vars[var][counter][1])
      line += '\n'
      output += line

    return output.rstrip('\n')


class NCARVar:

  def __init__(self, name=None):
    self._name = name.lower()
    self._data = [] ## Will contain tuple pair (datetime, value)
    self._start_time = None
    self._end_time = None
    self.length = 0

  def __str__(self):
    return self._name + ": (" + str(self.length) + ") "+ str(self._data)


  def __getitem__(self, index):
    try:
      return self._data[index]
    except Exception, e:
      print "Out of bounds: %s[%d]" %(self._name, index)
      raise e

  def getName(self):
    return self._name

  def addData(self, data = [], process_algo = None):
    if len(data) == 0:
      return

    if len(self._data) == 0:
      self._data = data
      self._start_time = data[0][0]
      self._end_time = data[-1][0]
    else:
      self.__mergeData(data)

    self.length = len(self._data)

  ## TODO: figure out how to check if data is continuous.
  ## Assumes data is continuous, assume correct input.
  def __mergeData(self, data):
    if len(data) == 0:
      return


    self._data += data
    self._end_time = data[-1][0]


  def clearData(self):
    self._data = []



  def getLastPoints(number_entries=None, from_time = None, to_time=None):
    if number_entries != None and from_time == None:
      return self._data[-number_entries]





## --------------------------------------------------------------------------
## Start command line interface (main)
## --------------------------------------------------------------------------
