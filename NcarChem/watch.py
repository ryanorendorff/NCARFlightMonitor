#!/usr/bin/env python
# encoding: utf-8

## File Description
##
## Author: Ryan Orendorff <ryano@ucar.edu>
## Date: 21/07/11 10:31:47
##

## Syntax notes for coders who are not author
## - A double pound (##) is a comment, a single is commented code


## --------------------------------------------------------------------------
## Imports and Globals
## --------------------------------------------------------------------------

#### Inter-package imports
## Server Imports
from database import NDatabaseLiveUpdater, NDatabaseManager, NDatabase
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
  project = flight_info['ProjectNumber']  ## Ex: ICE-T
  flight = flight_info['FlightNumber']  ## Ex: rf12

  file_path = tempfile.gettempdir() + os.sep + "-".join( [project, flight, datetime.datetime.utcnow().strftime("%Y_%m_%d-%H_%M_%S")]) + os.extsep + "asc"
  return file_path


## --------------------------------------------------------------------------
## Classes
## --------------------------------------------------------------------------
class logger(object):
  def __init__(self, print_msg_fn=None):
    self.messages = []
    if print_msg_fn is None:
      self.print_msg_fn = self.print_default
    else:
      self.print_msg_fn = print_msg_fn

  def reset(self):
    self.messages = []

  def print_msg(self, msg, tm):
    self.messages += [self.print_msg_fn(msg,tm)]


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

class watcher(object):
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
                     email_fn = None,
                     print_msg_fn = None,
                     output_file_path=None,
                     variables=None,
                     *extra,
                     **kwds):
    """
    Give the watcher class the database information and email to send the
    resulting files.
    """
    ## Private Vars
    self._database=database
    self._host=host
    self._user=user
    self._simulate_start_time=simulate_start_time
    self._simulate_file=simulate_file

    self._header = header
    self._email = email_fn if email_fn is not None else None
    self._output_file_path = output_file_path

    self._algos = []

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
                               simulate_start_time=self._simulate_start_time,
                               simulate_fast=True,
                               simulate_file=self._simulate_file)
    elif self._simulate_start_time is not None:
      self._server = NDatabase(database=self._database,
                               host=self._host,
                               user=self._user,
                               simulate_start_time=self._simulate_start_time,
                               simulate_fast=True)

    else:
      self._server = NDatabase(database=self._database,
                               host=self._host,
                               user=self._user)

    self._updater = None  ## Interfaces with server to get regular updates.

    if print_msg_fn is None:
      self.log = logger(None)
    else:
      self.log = logger(print_msg_fn)

    if variables is None:
      self.__input_variables = self._server.variable_list
      self._variables = NVarSet(self._server.variable_list)
    else:
      self.__input_variables = variables
      ## Remove dud variables
      variables = [var for var in variables if (var in self._server.variable_list)]
      if len(variables) != self.__input_variables:
        print "The following variables do not exist: %s" % [var for var in self.__input_variables if var not in variables]
      self._variables = NVarSet(variables)

    self._badDataCheck(self._variables.keys())



  def startWatching(self):
    """ Runs run() all the time, operates in a 'daemon' mode """
    while(True):
      self.run()

  def runNumFlights(self, number_flights):
    while self._num_flight < number_flights:
      self.run()

  def _speed_wait(self, multiple):
    self.__wait *= multiple


  def run(self):
    """
    Looks for if a flight is in progress, and if so grabs new data from it
    since run() was last called. This data is then processed though real time
    algorithms added by attachAlgo().

    This is a NON BLOCKING function, it does not loop. Hence it can be used
    inside event loops in other packages (such as the twisted IRC bot package).
    """
    if not self._server.flying():
      if self._flying_now == False:  ## No flight in progress.
        self._server.reconnect()  ## Done to ensure good connection.
        if self._waiting is False:
          print "[%sZ] Waiting for flight." % self._server.getTimeStr()
          self._waiting = True
        self._server.sleep(3*self.__wait)

      ## Just switched from flying to not flying.
      else:
        self.log.print_msg("Flight ending.", self._server.getTimeStr())
        self._server.sleep(2 * 60) ## Get more data after landing
        self._updater.update() ## Get last bit of data.

        if self._output_file_path is None:
          out_file_name = output_file_str(self._server.getFlightInformation())
        else:
          out_file_name = self._output_file_path
        print ("[%sZ] Outputting file to %s" %
               (self._server.getTimeStr(), out_file_name))
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

        try:
          mail_time = self._server.getTimeStr()

          ## TODO: Change email subject to project name and flight number
          if self._email is not None:
            if self.log.messages != []:
              body_msg = "\n".join(self.log.messages)
            else:
              body_msg = "Data attached"
            self._email(self._server.getFlightInformation(), [out_file_name], body_msg)

            print "[%s] Sent mail." % self._server.getTimeStr()
        except Exception, e:
          print "%s: Could not send mail" % self.__class__.__name__
          print e

        self._flying_now = False
        self._flight_end_time = self._server.getTime()
        self._num_flight += 1
        self._variables.clearData()
        self.log.reset()

    ## Flight is in progress
    else:
      if self._flying_now == False:  ## Just started flying
        self.log.print_msg("In Flight.", self._server.getTimeStr())
        self._flight_start_time = self._server.getTime()
        self._flight_end_time = None
        self._flying_now = True
        self._waiting = False
        ##  Get preflight data
        self._variables.addData(self._server.getData(start_time="-60 MINUTE",
                                         variables=self._variables.keys()))
        self.resetAlgos()
        self._updater = NDatabaseLiveUpdater(server=self._server,
                                             variables=self._variables)

      self._updater.update()  ## Can return none, sleeps for at least DataRate
                              ## seconds (three seconds by default).

      for algo in self._algos:  ## Run algorithms attached by user.
        try:
          algo.run()
        except Exception, e:
          print "%s: Could not run algorithm; used variables %s." % (self.__class__.__name__, algo.variables)
          self._algos.remove(algo)
          print e


  def attachBoundsCheck(self, variable_name=None,
                              lower_bound=-32767,
                              upper_bound=32767):

    """
    Checks to see if a variable is within bounds. If not it calls log.print() to
    print a message to the user.
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
        self.error=False

    ## Attach method to object of NAlgorithm
    self.attachAlgo(variables=[variable_name],
                    start_fn=boundsCheckSetup,
                    process_fn=boundsCheck)

  def _badDataCheck(self, variables=None):
    bad_data_flags = self._server.getBadDataValues()

    for var in variables:
      bad_flag = bad_data_flags[var.upper()]
      self.__badDataForVariable(var, bad_flag)

  def __badDataForVariable(self, variable_name=None, bad_flag=-32767):
    def setup_bad(self, *args, **kwds):
      self.out_of_bounds = bad_flag
      self.error = False
      self.name = variable_name

    def process_bad(self, tm, data):
      if data[0] == bad_flag and self.error == False:
        self.log.print_msg('%s MISSING DATA' % self.name, tm)
        self.error = True
      elif data[0] != bad_flag and self.error == True:
        self.log.print_msg('%s no longer has missing data' % self.name, tm)
        self.error = False

    self.attachAlgo(variables=[variable_name],
                    start_fn=setup_bad,
                    process_fn=process_bad)

  def attachAlgo(self, variables=None,
                       start_fn=None, process_fn=None,
                       *extra, **kwds):
    """
    Store an NAlgorithm object to later call its process function in
    NAlgorithm.run(). Can use a setup function to programmatically create a
    persistent local scope.
    """
    algo = NAlgorithm()
    ## Types module required to add to instance of class,
    ## see http://en.wikibooks.org/wiki/Python_Programming/
    ##            Classes#To_an_instance_of_a_class
    algo.setup = types.MethodType(start_fn, algo, NAlgorithm)
    algo.process = types.MethodType(process_fn, algo, NAlgorithm)

    ## Force load NVars into instantiated scope, by object (are updated when
    ## updater.update is called
    algo.log = self.log  ## Allows message redirection.

    var_list = []
    for var in variables:
      var_list.append(self._variables.getNVar(var))

    algo.variables = NVarSet(var_list)

    self._algos += [algo]  ## Must be in [] to add to list

  def removeAlgos(self):
    """ Remove all attached algorithms. """
    for algo in self._algos: self._algos.remove(algo)

  def resetAlgos(self):
    """ Return to setup state. """
    for algo in self._algos:
      algo.flight_start_time = self._flight_start_time
      algo.reset()
