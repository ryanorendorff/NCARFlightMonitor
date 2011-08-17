#!/usr/bin/env python
# encoding: utf-8

## Test of live data reading.
##
## Author: Ryan Orendorff <ryano@ucar.edu>
## Date: 21/07/11 10:31:47
##

## Syntax notes for coders who are not author
## - A double pound (##) is a comment, a single is commented code


## --------------------------------------------------------------------------
## Imports and Globals
## --------------------------------------------------------------------------

## Time based imports
import datetime

## --------------------------------------------------------------------------
## Classes
## --------------------------------------------------------------------------
class NAlgorithm(object):
  """
  A container class for processing data. Users are meant to place a processing
  algorithm in either an instance of the class or the class itself (as a
  process() function). This function does not currenetly accept parameters.
  """
  def __init__(self):
    self.error = False  ## Used for error checking, caling, etc
    self.last_date = None
    self.variables = None
    self.updated = False
    self.new_data = None
    self._flight_start_time = None


    self.setup = lambda : None
    self.process = lambda : None

  @property
  def flight_start_time(self):
    return self._flight_start_time

  ## algo._flight_start_time is a shallow copy!
  @flight_start_time.setter
  def flight_start_time(self, value):
    self._flight_start_time = value

  def run(self):
    new_date = self._time.getTimeFromPos(-1)
    if new_date > self.last_date:
      self.updated = True

      new_data = self.variables.sliceWithTime(self.last_date, None)[1:]
      for point in new_data:
        tm = point[0]
        update = point[1:]
        try:
          self.process(tm, update)
        except Exception, e:
          raise e

      self.last_date = new_date
    else:
      self.updated = False

  def reset(self):
    try:
      self.setup()
      self._time = self.variables.getNVar(self.variables.keys()[0])
      self.last_date = self._time.getTimeFromPos(-1)
    except Exception, e:
      print "Could not rerun setup command."
      print e
