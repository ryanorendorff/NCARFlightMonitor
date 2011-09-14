#!/usr/bin/env python
# encoding: utf-8

## Record variables during flights of a particular airplane, and place
## these in an ascii file in the system's temp directory after a flight.
## Additionally informational messages about the flight are created for bad
## data flags by default.
##
## An additional algorithm (located in 'examples/functions.py') is run against
## the data as it arrives. Please see the functions file for more information.
##
## Author: Ryan Orendorff <ryano@ucar.edu>
## Date: 30/08/11 12:38:26
##

## --------------------------------------------------------------------------
## Imports and Globals
## --------------------------------------------------------------------------
from NCARFlightMonitor.watch import NWatcher
import datetime
import functions

## System
import os

## --------------------------------------------------------------------------
## Start command line interface (main)
## --------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    watch_server = NWatcher(database="GV")

    watch_server.attachAlgo(variables=('coraw_al',),
                            start_fn=functions.setup_co,
                            process_fn=functions.process_co,
                            description="CO raw cal checker")

    watch_server.attachAlgo(variables=('coraw_al',),
                            start_fn=functions.setup_lost_satcom,
                            process_fn=functions.process_lost_satcom,
                            run_mode="every update",
                            description="Satcom loss indicator")

    watch_server.startWatching()
