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


## --------------------------------------------------------------------------
## Functions
## --------------------------------------------------------------------------

def createVarList(names):
  variables = []
  for i in names:
    variables.append(NCARVar(i))

  return variables

## --------------------------------------------------------------------------
## Classes
## --------------------------------------------------------------------------

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
