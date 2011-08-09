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

from database import NDatabaseLiveUpdater, NDatabaseManager, NDatabase
from datafile import NRTFile
from data import NVarSet, NVar
from algos import NAlgorithm
import datetime

import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email.MIMEText import MIMEText
from email.Utils import COMMASPACE, formatdate
from email import Encoders

import os
import time

import types

## --------------------------------------------------------------------------
## Functions
## --------------------------------------------------------------------------

def time_str():
  return str(datetime.datetime.utcnow().replace(microsecond=0)) + "Z"


def output_file_str(server):
  info = server.getFlightInformation()
  project = info['ProjectNumber']
  flight = info['FlightNumber']
  return '/tmp/%s-%s-%s.asc' % (project, flight, datetime.datetime.utcnow().\
                                strftime("%Y_%m_%d-%H_%M_%S"))


def sendMail(to, subject, text, files=[], server="localhost"):
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

  def __init__(self, database=None,
                     host="eol-rt-data.guest.ucar.edu",
                     user="ads",
                     simulate_start_time=False,
                     simulate_file=None,
                     variables=None,
                     *extra,
                     **kwds):
    self._database=database
    self._host=host
    self._user=user
    self._simulate_start_time=simulate_start_time
    self._simulate_file=simulate_file
    self._variables = NVarSet(variables)

    self._algos = []
    self._flying_now = False

    self._server = NDatabase(database=self._database,
                             host=self._host,
                             user=self._user,
                             simulate_start_time=self._simulate_start_time,
                             simulate_fast=True,
                             simulate_file=self._simulate_file)

    self._updater = None

  def pnt(self,msg):
    print msg

  def startWatching(self):
    while(True):
      self.run()

  def run(self):
    if not self._server.flying():
      if self._flying_now == False:
        self._server.reconnect() ## Done to ensure good connection.
        print "[%s] Waiting for flight." % time_str()
        self._server.sleep(5*60)
      else:
        self.pnt( "[%s] Flight ending, acquiring two minutes of data." % time_str())
        self._server.sleep(2 * 60) ## Get more data after landing
        self._updater.update() ## Get last bit of data.

        print "[%s] Outputting file to %s" % (time_str(), output_file_str(self._server))
        out_file_name = output_file_str(self._server)
        try:
          out_file_name = output_file_str(self._server)
          out_file = NRTFile()
          labels, data = self._variables.getDataAsList()
          out_file.write(out_file_name, self._server.getDatabaseStructure(), labels, data)

          mail_time = time_str()
          sendMail(["ryano@ucar.edu"],
                   "Data from flight " + mail_time, \
                   "Attached is data from flight on " + mail_time,
                   [out_file_name])

          print "[%s] Sent mail." % time_str()
        except Exception, e:
          print "Could not send mail"
          print e

        self._flying_now = False

    else:
      if self._flying_now == False:
        self.pnt( "\n[%s] In Flight." % time_str())
        self._flying_now = True
        self._variables.clearData()
        self._variables.addData(self._server.getData(start_time="-60 MINUTE",
                                         variables=self._variables.keys()))
        self._updater = NDatabaseLiveUpdater(server=self._server, variables=self._variables)

      self._updater.update() ## Can return none, sleeps for at least DatRate seconds.

      for algo in self._algos:
        algo.run()

  def attachBoundsCheck(self, variable_name=None,
                              lower_bound=-32767,
                              upper_bound=32767):

    def boundsCheckSetup(self, *extra, **kwds):
      self.lower_bound = lower_bound
      self.upper_bound = upper_bound
      self.variable = self.variables[0]

    def boundsCheck(self):
      val = self.variable[-1]

      if not(self.lower_bound <= val <= self.upper_bound) and self.error == False:
        self.pnt( "[%s] %s out of bounds." % (str(self.variable.getDate(-1)) + "Z", self.variable.getName()))
        self.error = True
      elif self.lower_bound <= val <= self.upper_bound and self.error == True:
        self.pnt( "[%s] %s back in bounds." % (str(self.variable.getDate(-1)) + "Z", self.variable.getName()))
        self.error=False


    self.attachAlgo(variables=[variable_name], start_fn=boundsCheckSetup, process_fn=boundsCheck)


  def attachAlgo(self, variables=None, start_fn=None, process_fn=None, *extra, **kwds):
    algo = NAlgorithm()
    algo.setup = types.MethodType(start_fn, algo, NAlgorithm)
    algo.process = types.MethodType(process_fn, algo, NAlgorithm)

    algo.variables = []
    algo.pnt = self.pnt
    for var in variables:
      algo.variables += [self._variables[var]]
    algo.setup(extra, kwds)

    self._algos += [algo]


## --------------------------------------------------------------------------
## Start command line interface (main)
## --------------------------------------------------------------------------

if __name__ == "__main__":
  import sys

  def setup(self, *extra, **kwds):
    self.cal = False
    self.fo3_acd = self.variables[0]
    self.psfdc = self.variables[1]

  def process(self):
    if 0 <= self.fo3_acd[-1] <= 0.09 and self.psfdc[-1]*0.75006 < 745 and self.cal == False:
      print "[%s] fO3 cal occuring." % (str(self.fo3_acd.getDate(-1)) + "Z")
      self.cal = True
    elif self.fo3_acd[-1] > 0.09 and self.cal == True:
      self.cal = False



  watch_server = watcher(database="C130",
                         host="127.0.0.1",
                         user="postgres",
                         simulate_start_time=
                           datetime.datetime(2011, 7, 28, 14, 0, 0),
                         simulate_file=sys.argv[1],
                         variables=('psfdc', 'fo3_acd', 'co2_pic', 'ch4_pic'))

  watch_server.attachBoundsCheck('co2_pic', 350, 500)
  watch_server.attachBoundsCheck('ch4_pic', 1.7, 1.9)
  watch_server.attachAlgo(variables=('fo3_acd', 'psfdc'), start_fn=setup, process_fn=process)
  watch_server.startWatching()
