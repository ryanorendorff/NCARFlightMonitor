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

#### Intrapackage imports
from datafile import NRTFile

## New data type, not supported below python 2.7
from collections import OrderedDict

import datetime

## --------------------------------------------------------------------------
## Functions
## --------------------------------------------------------------------------

def createOrderedList(variables):
  """
  Creates a list where col[0] is the variable name and
  col[1] is an NVar object.
  """
  var_list = []
  for var in variables:
    var_list.append((var.lower(), NVar(var)))

  return var_list

def createOrderedListFromFile(file_name):
  nfile = NRTFile(file_name)
  olist = NVarSet(nfile.labels[1:])
  olist.addData(nfile.data)
  return olist


## --------------------------------------------------------------------------
## Classes
## --------------------------------------------------------------------------


class NVarSet(OrderedDict):
  """
  Holds multiple NVars, and assumes that they are all the same size (this is
  true from live aircraft data). Can export data as a list using .data
  """

  def __init__(self, var_start, *variables):
    """
    Init function can take either a python list of variables or every variable
    listed as a parameter.
    """
    self._str = ""
    self._rows = 0
    self._date = []

    ## If input is just NVarSet('var1','var2'), convert into list
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
    """
    Adds data to the set. Must match the variable order of the set and the
    number of variables in the set.
    """
    if len(data) != 0:
      self._rows += len(data)
      pos = 1
      self._date += [column[0] for column in data]
      for var in OrderedDict.__iter__(self):
        OrderedDict.__getitem__(self, var).addData([(column[0], column[pos])\
                                                   for column in data])
        pos += 1

  @property
  def labels(self):
    """ Return the names associated with the columns in .data """
    return tuple(['DATETIME'] + self.keys())

  @property
  def data(self):
    """ Return a 2D list matrix of data points, datetime variable is first. """
    data = []
    for counter in range(self._rows):
      line = (self._date[counter],)
      for var in OrderedDict.__iter__(self):
        line += (OrderedDict.__getitem__(self, var)[counter],)
      data += (line,)
    return data

  def clearData(self):
    """
    Removes all data from the set, keeps variable list.
    """
    for var in OrderedDict.__iter__(self):
      OrderedDict.__getitem__(self, var).clearData()
      self._date = []


class NVar(OrderedDict):
  """
  The basic class for holding chronological list data. It is accessed like a
  dictionary where the datetime is the key. It will also ordered, so using an
  integer as the key will give the Nth value in the list. Only accepts one
  data value per datetime.
  """

  def __init__(self, name=None):
    self.name = name.lower()
    self._order = {}
    self._date_order = {}
    super(NVar, self).__init__()

  def __getitem__(self, item):
    if isinstance(item, slice):
      data = []

      if isinstance(item.start, datetime.datetime):
        start = self._date_order[item.start]
      else:
        if item.start is None:
          start = 0
        else:
          if item.start < 0:
            start = OrderedDict.__len__(self) + item.start
          else:
            start = item.start
      if isinstance(item.stop, datetime.datetime):
        stop = self._date_order[item.stop]
      else:
        if item.stop is None:
          stop = OrderedDict.__len__(self)
        else:
          if item.stop < 0:
            stop = OrderedDict.__len__(self) + item.stop
          else:
            stop = item.stop

      for point in range(start, stop):
        data += [self.__getitem__(point)]

      return data
    if isinstance(item, int):
      if item < 0:
        return OrderedDict.__getitem__(self,
                 self._order[(OrderedDict.__len__(self)) + item])
      else:
        return OrderedDict.__getitem__(self, self._order[item])
    else:
      return OrderedDict.__getitem__(self, item)


  def __add__(self, y):
    data = []

    name = None
    x_name = self.name
    y_name = None

    for k, v in self.iteritems():
      data += [(k, v)]

    if isinstance(y, NVar):
      y_name = y.name
      for k, v in y.iteritems():
        data += [(k, v)]
    else:
      data += y

    data.sort(key=lambda x: x[0])

    if x_name == y_name and (x_name is not None and y_name is not None):
      name = x_name
    elif x_name is not None and y_name is None:
      name = x_name
    elif x_name is None and y_name is not None:
      name = y_name
    else:
      raise ValueError('NVar: can only add NVars of the same name.')

    if name is not None:
      var = NVar(name)
      var.addData(data)
      return var

  def getDate(self, index):
    """ Returns the date associated with an integer index.  """
    if index < 0:
      return self._order[(OrderedDict.__len__(self)) + index]
    else:
      return self._order[index]

  def addData(self, data=[]):
    """
    Adds data to the variable. It does not check the data input, this must
    be done prior to adding the data.
    """
    if len(data) == 0:
      return

    self.__mergeData(data)

  def __mergeData(self, data):
    try:
      if not isinstance(data[0][0], datetime.datetime):
        raise ValueError
      try:
        data[0][1]
      except Exception:
        raise ValueError

    except ValueError, e:
      raise ValueError('NVar: Data must be formatted as [(datetime, value), ...]')

    for row in data:
      self._order[OrderedDict.__len__(self)] = row[0]
      self._date_order[row[0]] = OrderedDict.__len__(self)
      OrderedDict.__setitem__(self, row[0], row[1])

  def clearData(self):
    """
    Removes all data from variable, keeps variable name.
    """
    OrderedDict.clear(self)
    self._order = {}


## --------------------------------------------------------------------------
## Start command line interface (main)
## --------------------------------------------------------------------------

if __name__ == "__main__":
  """
  Testing function.
  """
  import sys

  olist = createOrderedListFromFile(sys.argv[1])
  print "Printing NVarSet:\n%s" % olist
  print "Printing NvarSet[-1:]:\n%s" % olist.data[-1:]
  if olist.data[:] == olist.data:
    print "NVarSet[:] the same as olist.data"
  tasx = olist['tasx']
  print tasx[:]
  print tasx[datetime.datetime(2011,8,11,13,14,6)]
  print tasx[-1]
  print tasx[datetime.datetime(2011, 8, 11, 13, 14, 6):]

  ggalt = NVar('ggalt')
  ggalt += [(datetime.datetime(2011,8,11,13,14,16), 0.453), (datetime.datetime(2011,8,11,13,14,11), 0.5)]
  ggalt_2 = NVar('ggalt')
  ggalt_2 += [(datetime.datetime(2011,8,11,13,14,6),0.2)]
  ggalt_3 = ggalt + ggalt_2
  print ggalt_3[:]
