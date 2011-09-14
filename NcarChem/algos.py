#!/usr/bin/env python
# encoding: utf-8

## The shell that holds the algorithms that are attached to a watcher object.
## These objects are not designed to be used for purposes other than the real
## time analysis through the watcher class.
##
## Author: Ryan Orendorff <ryano@ucar.edu>
## Date: 30/08/11 13:06:03

## TODO: Allow for the process function to accept and return data, for use in
## a generic NAlgorithm class.

## TODO: consider renaming to NRTAlgorithm and creating a more generic
## NAlgorithm class that can be used outside of a NcarChem.watcher
## instantiated object.


## --------------------------------------------------------------------------
## Imports and Globals
## --------------------------------------------------------------------------
import datetime

## --------------------------------------------------------------------------
## Classes
## --------------------------------------------------------------------------


class NAlgorithm(object):
    """
    A container class for processing data to be used with the
    NcarChem.watcher.attachAlgorithm() function. The variables, setup and
    process objects need to be added to the algorithm through dot notation
    after instantiation of the class.
    """
    def __init__(self, run_mode="new data", desc="No Description"):
        self.last_date = None
        self.variables = None
        self.updated = False
        self.new_data = None
        self._flight_start_time = None
        self.desc = desc

        self.setup = lambda: None
        self.process = lambda: None
        self._run_mode = run_mode

    @property
    def flight_start_time(self):
        return self._flight_start_time

    ## algo._flight_start_time is a shallow copy!
    @flight_start_time.setter
    def flight_start_time(self, value):
        self._flight_start_time = value

    def run(self):
        try:
            new_date = self._time.getTimeFromPos(-1)
        except KeyError:
            return

        if self.last_date is None:
            self.last_date = new_date

        if new_date > self.last_date:
            self.updated = True
            self._process_update()
            self.last_date = new_date
        else:
            self.updated = False
            if self._run_mode == "every update":
                self.process(new_date, None)

    def _process_update(self):
        new_data = self.variables.sliceWithTime(self.last_date, None)[1:]

        for point in new_data:
            tm = point[0]
            update = point[1:]
            try:
                self.process(tm, update)
            except Exception, e:
                raise e

    def reset(self):
        try:
            self.setup()
        except Exception, e:
            print "Could not rerun setup command."
            print e

        self._time = self.variables.getNVar(self.variables.keys()[0])
        try:
            self.last_date = self._time.getTimeFromPos(-1)
        except KeyError, e:
            self.last_date = None
