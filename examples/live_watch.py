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
import functions

## System
import os
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

  ## Functions used to determine if coraw_al is out of range

  ## Start watching the server
  watch_server = watcher(database="hippo5_rf02",
                         host="192.168.1.49",
                         user="postgres",
                         simulate_start_time=
                           datetime.datetime(2011, 8, 11, 14, 30, 0),
                         variables=('ggalt', 'tasx',
                                    'coraw_al', 'co_qlive',
                                    'co2_qlive', 'ch4_qlive',
                                    'atx', 'o3mr_csd', 'psxc', 'theta', 'wic')
                         )

  #watch_server = watcher(database="GV",
                         ##header=True,
                         ##email_fn=sendMail,
                         #simulate_start_time=
                           #datetime.datetime(2011, 8, 19, 17, 30, 0),
                         #variables=('ggalt', 'tasx',
                                    #'coraw_al', 'co_qlive',
                                    #'co2_qlive', 'ch4_qlive',
                                    #'atx', 'o3mr_csd', 'psxc', 'theta', 'wic')
                         #)
  watch_server.attachAlgo(variables=('coraw_al',), start_fn=functions.setup_co, process_fn=functions.process_co)
  watch_server.attachAlgo(variables=('coraw_al','co_qlive'), start_fn=functions.setup_follow, process_fn=functions.process_follow)

  watch_server.runOnce()
