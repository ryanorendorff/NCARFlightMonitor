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
from NcarChem.watch import watcher
import datetime

## --------------------------------------------------------------------------
## Functions
## --------------------------------------------------------------------------

## --------------------------------------------------------------------------
## Classes
## --------------------------------------------------------------------------

## --------------------------------------------------------------------------
## Start command line interface (main)
## --------------------------------------------------------------------------

if __name__ == "__main__":
  import sys

  ## Functions used to determine if fo3_acd is out of range
  def setup_fo3(self, *extra, **kwds):
    self.cal = False
    self.fo3_acd = self.variables[0]
    self.psfdc = self.variables[1]

  def process_fo3(self):
    if 0 <= self.fo3_acd[-1] <= 0.09 and self.psfdc[-1]*0.75006 < 745 \
        and self.cal == False:
      print "[%s] fO3 cal occuring." % (str(self.fo3_acd.getDate(-1)) + "Z")
      self.cal = True
    elif self.fo3_acd[-1] > 0.09 and self.cal == True:
      self.cal = False

  ## Functions used to determine if coraw_al is out of range
  def setup_co(self, *extra, **kwds):
    self.cal = False
    self.coraw_al = self.variables[0]

  def process_co(self):
    if self.coraw_al[-1] <= 8000 and self.cal == False:
      self.pnt( "[%s] CO cal occuring." %\
                (str(self.coraw_al.getDate(-1)) + "Z"))
      self.cal = True
    elif self.coraw_al[-1] > 8000 and self.cal == True:
      self.cal = False


  ## Start watching the server
  watch_server = watcher(database="C130",
                         host="127.0.0.1",
                         user="postgres",
                         simulate_start_time=
                           datetime.datetime(2011, 8, 9, 14, 0, 0),
                         simulate_file=sys.argv[1],
                         #email="ryano@ucar.edu",
                         variables=('ggalt', 'tasx', 'coraw_al'))
                         #variables=('psfdc', 'fo3_acd', 'co2_pic', 'ch4_pic'))


  ## Check that variables are in bounds
  #watch_server.attachBoundsCheck('co2_pic', 350, 500)
  #watch_server.attachBoundsCheck('ch4_pic', 1.7, 1.9)
  watch_server.attachAlgo(variables=('coraw_al',), start_fn=setup_co, process_fn=process_co)

  ## This is an infinite loop.
  watch_server.startWatching()
