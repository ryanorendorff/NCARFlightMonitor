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

## Email libraries
import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email.MIMEText import MIMEText
from email.Utils import COMMASPACE, formatdate
from email import Encoders

## General
import os
import time


## --------------------------------------------------------------------------
## Functions
## --------------------------------------------------------------------------

def time_str():
  """
  Retyrns the current UTC time, no microseconds, as a string. A "Z" is attached
  to the end to signify Zulu Time.
  """
  return str(datetime.datetime.utcnow().replace(microsecond=0)) + "Z"


def output_file_str(server):
  """
  Create output file string, which contains the flight and project number.
  """
  info = server.getFlightInformation()  ## Updated when flight detected.
  project = info['ProjectNumber']  ## Ex: ICE-T
  flight = info['FlightNumber']  ## Ex: rf12
  return '/tmp/%s-%s-%s.asc' % (project, flight, datetime.datetime.utcnow().\
                                strftime("%Y_%m_%d-%H_%M_%S"))


def sendMail(to, subject, text, files=[], server="localhost"):
  """
  Mail function copied from Stack Overflow. Uses gmail account for SMTP server.
  """
  assert type(to) == list
  assert type(files) == list
  fro = "ryano@ucar.edu"

  msg = MIMEMultipart()
  msg['From'] = fro
  msg['To'] = COMMASPACE.join(to)
  msg['Date'] = formatdate(localtime=True)
  msg['Subject'] = subject

  msg.attach(MIMEText(text))

  for file in files:
      part = MIMEBase('application', "octet-stream")
      part.set_payload(open(file, "rb").read())
      Encoders.encode_base64(part)
      part.add_header('Content-Disposition', 'attachment; filename="%s"'
                     % os.path.basename(file))
      msg.attach(part)

  pw = open(".pass", 'r').read()
  server = smtplib.SMTP('smtp.gmail.com', 587)
  server.starttls()
  server.login("linux755@gmail.com", pw)
  server.sendmail("ryano@ucar.edu", "ryano@ucar.edu", msg.as_string())
  server.quit()


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
  def __init__(self, database=None,  ## Ex: GV
                     host="eol-rt-data.guest.ucar.edu",
                     user="ads",  ## ads is the default used at eol.
                     simulate_start_time=False,  ## Change clock
                     simulate_file=None,  ## Load a  file sql database.
                     email="",  ## Used to email results after flight.
                     variables=None,  ## List of variables to watch.
                     *extra,  ## to prevent bitching
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
    self._email = email
    self._variables = NVarSet(variables)

    self._algos = []
    self._flying_now = False

    self._server = NDatabase(database=self._database,
                             host=self._host,
                             user=self._user,
                             simulate_start_time=self._simulate_start_time,
                             simulate_fast=True,
                             simulate_file=self._simulate_file)

    self._updater = None  ## Interfaces with server to get regular updates.

  ## TODO: Determine if a queue is better
  def pnt(self, msg):
    """
    A print method that allows a msg to be easily redirected. This can be
    overwritten externally, see examples/bot.py.
    """
    print msg

  def startWatching(self):
    """
    Runs run() all the time, operates in a 'daemon' mode
    """
    while(True):
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
        print "[%s] Waiting for flight." % time_str()
        self._server.sleep(5 * 60)  ## Look for another flight in 5 minutes.

      ## Just switched from flying to not flying.
      else:
        self.pnt("[%s] Flight ending, acquiring two minutes of data." %\
                  time_str())
        self._server.sleep(2 * 60) ## Get more data after landing
        self._updater.update() ## Get last bit of data.

        print "[%s] Outputting file to %s" %\
               (time_str(), output_file_str(self._server))
        out_file_name = output_file_str(self._server)
        try:
          out_file_name = output_file_str(self._server)
          out_file = NRTFile()
          labels, data = self._variables.getDataAsList()
          out_file.write(out_file_name,
                         self._server.getDatabaseStructure(),
                         labels, data)

          mail_time = time_str()
          sendMail([self._email],
                   "Data from flight " + mail_time, \
                   "Attached is data from flight on " + mail_time,
                   [out_file_name])

          print "[%s] Sent mail." % time_str()
        except Exception, e:
          print "Could not send mail"
          print e

        self._flying_now = False

    ## Flight is in progress
    else:
      if self._flying_now == False:  ## Just started flying
        self.pnt("\n[%s] In Flight." % time_str())
        self._flying_now = True
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
          print "Could not run algorithm."
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