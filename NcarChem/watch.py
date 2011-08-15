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
                     simulate_start_time=False,
                     simulate_file=None,
                     header=False,
                     email_fn = None,
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

    if self._simulate_file is not None:
      self._server = NDatabase(database=self._database,
                               host=self._host,
                               user=self._user,
                               simulate_start_time=self._simulate_start_time,
                               simulate_fast=True,
                               simulate_file=self._simulate_file)
    else:
      self._server = NDatabase(database=self._database,
                               host=self._host,
                               user=self._user)

    self._updater = None  ## Interfaces with server to get regular updates.

    if variables is None:
      self._variables = NVarSet(self._server.variable_list)
    else:
      self._variables = NVarSet(variables)

  ## TODO: Determine if a queue is better
  def pnt(self, msg):
    """
    A print method that allows a msg to be easily redirected. This can be
    overwritten externally, see examples/bot.py.
    """
    print msg

  def startWatching(self):
    """ Runs run() all the time, operates in a 'daemon' mode """
    while(True):
      self.run()

  def runOnce(self):
    """ Run for only one flight.  """
    while self._num_flight == 0:
      self.run()


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
          print "[%s] Waiting for flight." % self._server.getTimeStr()
          self._waiting = True
        self._server.sleep(5 * 60)  ## Look for another flight in 5 minutes.

      ## Just switched from flying to not flying.
      else:
        self.pnt("[%s] Flight ending." % self._server.getTimeStr())
        self._server.sleep(2 * 60) ## Get more data after landing
        self._updater.update() ## Get last bit of data.

        if self._output_file_path is None:
          out_file_name = output_file_str(self._server.getFlightInformation())
        else:
          out_file_name = self._output_file_path
        print ("[%s] Outputting file to %s" %
               (self._server.getTimeStr(), out_file_name))
        try:
          out_file = NRTFile()
          labels = self._variables.labels
          data = self._variables.data
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
            self._email(self._server.getFlightInformation(), [out_file_name])

            print "[%s] Sent mail." % self._server.getTimeStr()
        except Exception, e:
          print "%s: Could not send mail" % self.__class__.__name__
          print e

        self._flying_now = False
        self._flight_end_time = self._server.getTime()
        self._num_flight += 1
        self.resetAlgos()

    ## Flight is in progress
    else:
      if self._flying_now == False:  ## Just started flying
        self.pnt("[%s] In Flight." % self._server.getTimeStr())
        self._flight_start_time = self._server.getTime()
        self._flight_end_time = None
        self._flying_now = True
        self._waiting = False
        self._variables.clearData()
        ##  Get preflight data
        self._variables.addData(self._server.getData(start_time="-60 MINUTE",
                                         variables=self._variables.keys()))
        self._updater = NDatabaseLiveUpdater(server=self._server,
                                             variables=self._variables)

      self._updater.update()  ## Can return none, sleeps for at least DataRate
                              ## seconds (three seconds by default).

      for algo in self._algos:  ## Run algorithms attached by user.
        try:
          algo.run()
        except Exception, e:
          print "%s: Could not run algorithm." % self.__class__.__name__
          self._algos.remove(algo)
          print e


  def attachBoundsCheck(self, variable_name=None,
                              lower_bound=-32767,
                              upper_bound=32767):

    """
    Checks to see if a variable is within bounds. If not it calls pnt() to
    print a message to the user.
    """
    def boundsCheckSetup(self, *extra, **kwds):
      """
      Setup function to give instantiated object persistent variables.
      """
      self.lower_bound = lower_bound
      self.upper_bound = upper_bound
      self.variable = self.variables[0]

    def boundsCheck(self):
      """
      Determine if out of bounds, and only print a message once if so.
      """
      val = self.variable[-1]

      ## If out of range and was not so before
      if not(self.lower_bound <= val <= self.upper_bound) \
         and self.error == False:
        self.pnt("[%s] %s out of bounds." % \
                  (str(self.variable.getDate(-1)) + "Z",
                   self.variable.getName()))
        self.error = True
      ## If in range after being out of range
      elif self.lower_bound <= val <= self.upper_bound \
           and self.error == True:
        self.pnt("[%s] %s back in bounds." % \
                   (str(self.variable.getDate(-1)) + "Z",
                    self.variable.getName()))
        self.error=False

    ## Attach method to object of NAlgorithm
    self.attachAlgo(variables=[variable_name],
                    start_fn=boundsCheckSetup,
                    process_fn=boundsCheck)

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
    algo.variables = []
    algo.pnt = self.pnt  ## Allows message redirection.
    for var in variables:
      algo.variables += [self._variables[var]]
    algo.setup(extra, kwds)  ## Can pass setup arbitrary inputs.

    self._algos += [algo]  ## Must be in [] to add to list

  def removeAlgos(self):
    """ Remove all attached algorithms. """
    for algo in self._algos: self._algos.remove(algo)

  def resetAlgos(self):
    """ Return to setup state. """
    for algo in self._algos: algo.reset()

## --------------------------------------------------------------------------
## Start command line interface (main)
## --------------------------------------------------------------------------

if __name__ == "__main__":
  """
  Example of how to use the watcher class.
  """
  ## Get command line arguments
  import sys

  def setup(self, *extra, **kwds):
    """
    Setup for fo3_acd cal checking.
    """
    self.cal = False   ## Used to prevent message print spam.
    self.fo3_acd = self.variables[0]
    self.psfdc = self.variables[1]

  def process(self):
    """
    What gets run every time new data is acquired.
    """
    ## Only cals when psfdc (pressure) is below 745 torr.
    if 0 <= self.fo3_acd[-1] <= 0.09 \
        and self.psfdc[-1]*0.75006 < 745 \
        and self.cal == False:
      print "[%s] fO3 cal occuring." % (str(self.fo3_acd.getDate(-1)) + "Z")
      self.cal = True
    elif self.fo3_acd[-1] > 0.09 and self.cal == True:
      self.cal = False

  ## Attach to a local database, will be filled with an example file.
  watch_server = watcher(database="C130",
                         host="127.0.0.1",
                         user="postgres",
                         simulate_start_time=
                           datetime.datetime(2011, 7, 28, 14, 0, 0),
                         simulate_file=sys.argv[1],
                         variables=('psfdc', 'fo3_acd', 'co2_pic', 'ch4_pic'))

  watch_server.attachBoundsCheck('co2_pic', 350, 500)
  watch_server.attachBoundsCheck('ch4_pic', 1.7, 1.9)
  watch_server.attachAlgo(variables=('fo3_acd', 'psfdc'),
                          start_fn=setup, process_fn=process)
  ## Will go indefinitely, end with CTRL-C
  watch_server.startWatching()
