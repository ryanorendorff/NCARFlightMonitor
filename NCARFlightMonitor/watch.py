#!/usr/bin/env python
# encoding: utf-8

## Copyright 2011 Ryan Orendorff, NCAR under GPLv3
## See README.mkd for more details.

## Contains the NWatcher class that combines all of the other parts of the
## package together into a coherent and easy to use flight tracker. This class
## can be used in a stand alone manner in its own application or attached to
## another process like an IRC chat bot.
##
## The class is designed to wait for a flight and monitor the desired
## variables (or all the server variables if none are specified) when a flight
## is detected. The data is then logged in an NVarSet, run through data
## integrity checking algorithms, and then output to a file in the temporary
## directly. A emailing function can be attached to the class to email this
## file to desired recipients.
##
## The NWatcher class supports the ability for arbitrary real time data
## processing algorithms to be attached to the class, where each time point
## is passed to the algorithm one at a time and in order. Default algorithms
## check for bad data flags and if a variable is inside bounds (although the
## bounds are set by the user of the class). All attached algorithms are
## tested at every point during a flight; if an algorithm fails it is removed
## from the list of processing algorithms and does not run again until the
## next flight.
##
## Additionally the class can be set to run its own continuous monitoring loop
## with startWatching(), run through a few flights with a continuous loop with
## runNumFlights(), or it can be set to use its monitoring capabilities in a
## non-blocking manner, which allows the class to be run inside other loops
## such as event loops for IRC bots. The non-blocking mode will catch all data
## run in-between its last call; the non-blocking call does not know about the
## data acquisition rate and just attempts its best to get the most recent
## data.
##
## Author: Ryan Orendorff <ryano@ucar.edu>
## Date: 04/09/11 03:10:31

## Syntax notes for coders who are not author
## - A double pound (##) is a comment, a single is commented code


## --------------------------------------------------------------------------
## Imports and Globals
## --------------------------------------------------------------------------

## Server Imports
from database import NDatabaseLiveUpdater, NDatabase
## ASCII file imports
from datafile import NRTFile
## Internal Python Ordered Dictionary data structures
from data import NVarSet, NVar
## Mutable algorithm containers
from algos import NAlgorithm

## All dates are handled in datetime.datetime format
import datetime

## Allows methods to be injected into instantiated objects.
import types

## General
import os
import tempfile
import time


## --------------------------------------------------------------------------
## Functions
## --------------------------------------------------------------------------

def output_file_str(flight_info):
    """
    Create output file string, which contains the flight and project number.
    """
    project = flight_info['ProjectNumber']  # Ex: ICE-T
    flight = flight_info['FlightNumber']  # Ex: rf12

    file_path = "".join([tempfile.gettempdir(), os.sep,
                         "-".join([project, flight,
                                   datetime.datetime.utcnow()
                                     .strftime("%Y_%m_%d-%H_%M_%S")]),
                         os.extsep, "asc"])
    return file_path


## --------------------------------------------------------------------------
## Classes
## --------------------------------------------------------------------------
class Logger(object):
    def __init__(self, print_msg_fn=None):
        self.messages = []
        if print_msg_fn is None:
            self.print_msg_fn = self.print_default
        else:
            self.print_msg_fn = print_msg_fn

    def reset(self):
        self.messages = []

    def print_msg(self, msg, tm):
        self.messages += [self.print_msg_fn(msg, tm)]

    def print_default(self, msg, tm):
        """
        A print method that allows a msg to be easily redirected. This can be
        overwritten externally, see examples/bot.py.
        """
        if tm is not None:
            formatted_msg = "[%sZ] %s" % (tm, msg)
        else:
            formatted_msg = msg

        print formatted_msg
        return formatted_msg


class NWatcher(object):
    """
    A class designed to watch a server to see when an aircraft is in flight.
    When in flight, real time data is collected and analysed to ensure
    proper instrumentation operation. Arbitrary variables and processing
    functions can be added to this class though dynamic method attachments. For
    an example, see the main section of this source,
    """
    def __init__(self, database=None,
                       host="eol-rt-data.guest.ucar.edu",
                       user="ads",
                       simulate_start_time=None,
                       simulate_file=None,
                       header=False,
                       email_fn=None,
                       print_msg_fn=None,
                       output_file_path=None,
                       variables=None,
                       *extra,
                       **kwds):
        """
        Give the watcher class the database information and email to send the
        resulting files.
        """
        ## Private Vars
        self._database = database
        self._host = host
        self._user = user
        self._simulate_start_time = simulate_start_time
        self._simulate_file = simulate_file

        self._header = header
        self._email = email_fn if email_fn is not None else None
        self._output_file_path = output_file_path

        self._algos = []
        self.__input_algos = []

        self.__print_msg_fn = print_msg_fn

        self._variables = None

        self._flying_now = False
        self._flight_start_time = None
        self._flight_end_time = None
        self._num_flight = 0
        self._waiting = False
        self.__wait = 1

        if self._simulate_file is not None:
            self._server = NDatabase(database=self._database,
                                     host=self._host,
                                     user=self._user,
                                     simulate_start_time=(
                                       self._simulate_start_time),
                                     simulate_fast=True,
                                     simulate_file=self._simulate_file)
        elif self._simulate_start_time is not None:
            self._server = NDatabase(database=self._database,
                                     host=self._host,
                                     user=self._user,
                                     simulate_start_time=(
                                       self._simulate_start_time),
                                     simulate_fast=True)
        else:
            self._server = NDatabase(database=self._database,
                                     host=self._host,
                                     user=self._user)

        self._updater = None  # Interfaces with server to get regular updates.

        if variables is None:
            self.__input_variables = self._server.variable_list
            #self._variables = NVarSet(self._server.variable_list)
        else:
            self.__input_variables = variables

        self._badDataCheck(self.__input_variables)

    def startWatching(self):
        """ Runs run() all the time, operates in a 'daemon' mode """
        while(True):
            self.run()

    def runNumFlights(self, number_flights):
        """ Run the program for a certain number of flights. """
        while self._num_flight < number_flights:
            self.run()

    def runForDuration(self, duration, fake_flight=False):
        """ Run the program for a certain duration """
        if fake_flight:
            self._server._fake_flying = True
        start_time = self._server.getTime()
        while duration > (self._server.getTime() - start_time):
            self.run()

        self._flightEnding()

    def runTillTime(self, run_time, fake_flight=False):
        """ Run the program till a certain time is reached """
        if fake_flight:
          self._server._fake_flying = True

        while run_time > self._server.getTime():
            self.run()

        if self._flying_now:
          self._flightEnding()

    def _speedWait(self, multiple):
        self.__wait *= multiple

    def run(self):
        """
        Looks for if a flight is in progress, and if so grabs new data from it
        since run() was last called. This data is then processed though real
        time algorithms added by attachAlgo().

        This is a NON BLOCKING function, it does not loop. Hence it can be
        used inside event loops in other packages (such as the twisted IRC bot
        package).
        """
        if not self._server.flying():
            if self._flying_now == False:  # No flight in progress.
                self._server.reconnect()  # Done to ensure good connection.
                if self._waiting is False:
                    print ("[%sZ] Waiting for flight."
                           % self._server.getTimeStr())
                    self._waiting = True
                self._server.sleep(3 * self.__wait)

            ## Just switched from flying to not flying.
            else:
                self.log.print_msg("Flight ending.", self._server.getTimeStr())
                self._server.sleep(2 * 60)  # Get more data after landing
                self._updater.update()  # Get last bit of data.
                self._flightEnding()

        ## Flight is in progress
        else:
            if self._flying_now == False:  # Just started flying
                self._flightStarting()
                self.log.print_msg("In Flight.", self._server.getTimeStr())

            # Can return none, sleeps for at least DataRate
            # seconds (three seconds by default).
            self._updater.update()

            # Run algorithms attached by user.
            for algo in self._algos:
                try:
                    algo.run()
                except Exception, e:
                    print ("%s: Could not run algorithm; used variables %s."
                           % (self.__class__.__name__, algo.variables))
                    print "Algorithm Description: %s" % algo.desc
                    self._algos.remove(algo)
                    print e

    def _flightStarting(self):
        self._flight_start_time = self._server.getTime()
        self._flight_end_time = None
        self._flying_now = True
        self._waiting = False
        self.log = Logger(self.__print_msg_fn)
        ##    Get preflight data
        self._variables = self._resetVariables(self.__input_variables)
        self._variables.addData(self._server.getData(start_time="-60 MINUTE",
                                                     variables=(
                                                       self._variables.keys()))
                                                     )
        self._updater = NDatabaseLiveUpdater(server=self._server,
                                             variables=self._variables)
        self.resetAlgos()

    def _flightEnding(self):
        ## Output file string creation
        if self._output_file_path is None:
            out_file_name = output_file_str(self._server
                                            .getFlightInformation())
        else:
            out_file_name = self._output_file_path
        print ("[%sZ] Outputting file to %s" %
                     (self._server.getTimeStr(), out_file_name))

        ## Actually try to write the file
        try:
            out_file = NRTFile()
            labels = self._variables.labels
            data = self._variables.sliceWithTime(None, None)
            if self._header == False:
                out_file.write(file_name=out_file_name,
                               labels=labels,
                               data=data)
            else:
                out_file.write(file_name=out_file_name,
                               header=self._server.getDatabaseStructure(),
                               labels=labels,
                               data=data)

        except Exception, e:
            print "%s: Could not create data file" % self.__class__.__name__
            print e

        ## Now try to mail the file
        try:
            mail_time = self._server.getTimeStr()

            ## TODO: Change email subject to project name and flight number
            if self._email is not None:
                if self.log.messages != []:
                    body_msg = "\n".join(self.log.messages)
                else:
                    body_msg = "Data attached"
                self._email(self._server.getFlightInformation(),
                            [out_file_name], body_msg)

                print "[%s] Sent mail." % self._server.getTimeStr()
        except Exception, e:
            print "%s: Could not send mail" % self.__class__.__name__
            print e

        self._flying_now = False
        self._flight_end_time = self._server.getTime()
        self._num_flight += 1
        self.log = None
        self._variables = None
        self._updater = None

    def attachAlgo(self, variables=None,
                         start_fn=None, process_fn=None,
                         run_mode=None,
                         description=None,
                         *extra, **kwds):
        """
        Store an NAlgorithm object to later call its process function in
        NAlgorithm.run(). Can use a setup function to programmatically create
        a persistent local scope.
        """
        if description is None:
            description = "No Description"

        if run_mode is None:
            run_mode = "new data"

        ## Types module required to add to instance of class,
        ## see http://en.wikibooks.org/wiki/Python_Programming/
        ##                        Classes#To_an_instance_of_a_class
        algo = NAlgorithm(run_mode=run_mode, desc=description)
        algo.setup = types.MethodType(start_fn, algo, NAlgorithm)
        algo.process = types.MethodType(process_fn, algo, NAlgorithm)

        self.__input_algos += [(algo, variables)]

    def removeAlgos(self):
        """ Remove all attached algorithms. """
        for algo in self._algos:
            self._algos.remove(algo)

    def resetAlgos(self):
        """ Return to setup state. """
        for algo_var in self.__input_algos:
            algo = algo_var[0]
            variables = algo_var[1]

            bad_variables = self._checkIfVariablesExists(variables)
            if len(bad_variables) != 0:
                print ("Could not run algorithm that has "
                        "the following bad variable names: %s"
                        % bad_variables)
                print "Algorithm description: %s" % algo.desc
                continue

            var_list = []
            for var in variables:
                var_list.append(self._variables.getNVar(var))

            algo.variables = NVarSet(var_list)

            algo.log = self.log
            algo.flight_start_time = self._flight_start_time
            algo.reset()
            self._algos.append(algo)

    def attachBoundsCheck(self, variable_name=None,
                          lower_bound=-32767,
                          upper_bound=32767):

        """
        Checks to see if a variable is within bounds. If not it calls
        log.print() to print a message to the user.
        """
        def boundsCheckSetup(self, *args, **kwds):
            """
            Setup function to give instantiated object persistent variables.
            """
            self.lower_bound = lower_bound
            self.upper_bound = upper_bound
            self.error = False
            self.name = variable_name

        def boundsCheck(self, tm, data):
            """
            Determine if out of bounds, and only print a message once if so.
            """
            val = data[0]

            ## If out of range and was not so before
            if not(self.lower_bound <= val <= self.upper_bound) \
                 and self.error == False:
                self.log.print_msg("%s out of bounds." % self.name, tm)
                self.error = True
            ## If in range after being out of range
            elif self.lower_bound <= val <= self.upper_bound \
                     and self.error == True:
                self.log.print_msg("%s back in bounds." % self.name, tm)
                self.error = False

        ## Attach method to object of NAlgorithm
        self.attachAlgo(variables=[variable_name],
                        start_fn=boundsCheckSetup,
                        process_fn=boundsCheck,
                        description=("Bounds check for %s"
                                     % variable_name))

    def _badDataCheck(self, variables=None):
        for var in variables:
            self.__badDataForVariable(var)

    def __badDataForVariable(self, variable_name=None):
        bad_data_flags = self._server.getBadDataValues()

        def setup_bad(self, *args, **kwds):
            self._bad_table = bad_data_flags
            self.var = variable_name
            self.out_of_bounds = bad_data_flags[self.var.upper()]
            self.error = False
            self.name = variable_name

        def process_bad(self, tm, data):
            if data[0] == self.out_of_bounds and self.error == False:
                self.log.print_msg('%s MISSING DATA' % self.name, tm)
                self.error = True
            elif data[0] != self.out_of_bounds and self.error == True:
                self.log.print_msg('%s no longer has missing data'
                                   % self.name, tm)
                self.error = False

        self.attachAlgo(variables=[variable_name],
                        start_fn=setup_bad,
                        process_fn=process_bad,
                        description=("Bad data check for %s"
                                     % variable_name))

    def _resetVariables(self, variables):
        ## Remove dud variables
        variables = [var for var in variables
                     if (var in self._server.variable_list)]
        if len(variables) != len(self.__input_variables):
            print ("The following variables do not exist: %s"
                   % [var for var in self.__input_variables
                      if var not in variables])
        return NVarSet(variables)

    def _checkIfVariablesExists(self, variables):
        return [var for var in variables
                if not (
                        (var in self.__input_variables) and
                        (var in self._server.variable_list)
                        )]
