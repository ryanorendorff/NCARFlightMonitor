#!/usr/bin/env python
# encoding: utf-8
from NCARFlightMonitor.watch import NWatcher
import datetime

## Function Definitions
def setup_co(self):
    ## All variables starting with `self` are persistant across process
    ## function runs and can be called in the process function. This can
    ## be useful for running averages and the like.
    self.cal = False

def process_co(self, tm, data):
    coraw_al = data[0]

    if coraw_al <= 8000 and self.cal == False:
        self.log.print_msg("CO cal occuring.", tm)
        self.cal = True
    ## Reset so that the log message does not appear thousands of times.
    elif coraw_al > 8000 and self.cal == True:
        self.cal = False

## Main
watch_server = NWatcher(database="hippo5_rf05",
                        host="192.168.1.49",
                        user="postgres",
                        simulate_start_time=
                          datetime.datetime(2011, 8, 19, 18, 0, 0)
                        )

watch_server.attachAlgo(variables=('coraw_al',),
                        start_fn=setup_co,
                        process_fn=process_co,
                        description="CO raw cal checker")

## Spend less time waiting for flight, should only be used in simulation mode.
watch_server._speedWait(100)

## Run for only one flight, then quit.
watch_server.runNumFlights(1)
