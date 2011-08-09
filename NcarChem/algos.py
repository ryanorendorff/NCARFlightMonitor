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
import datetime


## --------------------------------------------------------------------------
## Functions
## --------------------------------------------------------------------------



## --------------------------------------------------------------------------
## Classes
## --------------------------------------------------------------------------


class NAlgorithm(object):

  def __init__(self):
    self.error = False
    self.last_date = datetime.datetime(1970, 1, 1, 0, 0, 0)
    self.variables = []
    self.updated = False

    self.setup = lambda : None
    self.process = lambda : None

  def run(self):
    new_date = self.variables[0].getDate(-1)
    if new_date > self.last_date:
      self.last_date = new_date
      self.updated = True
    else:
      self.updated = False

    if self.updated == True:
      self.process()



## --------------------------------------------------------------------------
## Start command line interface (main)
## --------------------------------------------------------------------------
