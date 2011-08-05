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
from NcarChem.database import NDatabase
from NcarChem.database import NDatabaseLiveUpdater, NDatabaseManager
from NcarChem.data import NVar
import NcarChem
import datetime

import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email.MIMEText import MIMEText
from email.Utils import COMMASPACE, formatdate
from email import Encoders
import os
import time


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

    #smtp = smtplib.SMTP(server)
    #smtp.sendmail(fro, to, msg.as_string() )
    #smtp.close()

    pw = open(".pass", 'r').read()
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login("linux755@gmail.com", pw)
    server.sendmail("ryano@ucar.edu", "ryano@ucar.edu", msg.as_string())
    server.quit()



## --------------------------------------------------------------------------
## Classes
## --------------------------------------------------------------------------

## --------------------------------------------------------------------------
## Start command line interface (main)
## --------------------------------------------------------------------------

NDatabaseManager.register('Server', NDatabase)

if __name__ == "__main__":
  import sys

  NManager = NDatabaseManager()
  NManager.start()
  server = NManager.Server(database="C130",
                           host="127.0.0.1",
                           user="postgres",
                           simulate_start_time=\
                             datetime.datetime(2011, 7, 28, 14, 0, 0),
                           simulate_fast=True,
                           simulate_file=sys.argv[1])
  #server = NManager.Server(database="C130",
                           #simulate_start_time=\
                             #datetime.datetime(2011, 7, 28, 20, 12, 0),
                           #simulate_fast=True)
  #server = NManager.Server(database="C130")


  while(not server.flying()):
    server.reconnect() ## Done to ensure good connection.
    print "[%s] Waiting for flight." % time_str()
    server.sleep(5*60)
  print "[%s] In flight." % time_str()

  variables = NcarChem.data.NVarSet('ggalt', 'tasx', 'atx','psfdc',
                                    'fo3_acd',
                                    'ch4_pic', 'co2_pic',
                                    'dkl_mc', 'idxl_mc', 'pp2fl_mc', 'pwrl_mc')

  variables.addData(server.getData(start_time="-60 MINUTE",
                                   variables=variables.keys()))

  BAD_DATA = -32767
  psfdc = variables['psfdc']
  fo3_acd = variables['fo3_acd']
  fo3_caling = False
  co2_out = False
  ch4_out = False
  fo3_error = False
  co2 = variables['co2_pic']
  ch4 = variables['ch4_pic']

  updater = NDatabaseLiveUpdater(server=server, variables=variables)
  while(server.flying()):
    updater.update() ## Can return none, sleeps for at least DatRate seconds.

    if not (350 <= co2[-1] <= 500) and co2_out == False:
      print "[%s] CO2 out of bounds." % co2.getDate(-1)
      co2_out = True
    elif 350 <= co2[-1] <= 500 and co2_out == True:
      print "[%s] CO2 back in bounds." % co2.getDate(-1)
      co2_out = False

    if not (1.7 <= ch4[-1] <= 1.9) and ch4_out == False:
      print "[%s] CH4 out of bounds." % ch4.getDate(-1)
      ch4_out = True
    elif 1.7 <= ch4[-1] <= 1.9 and ch4_out == True:
      print "[%s] CH4 back in bounds." % ch4.getDate(-1)
      ch4_out = False

    if 0 <= fo3_acd[-1] <= 0.09 and psfdc[-1]*0.75006 < 745 and fo3_caling == False:
      print "[%s] fO3 cal occuring." % fo3_acd.getDate(-1)
      fo3_caling = True
    elif fo3_acd[-1] > 0.09 and fo3_caling == True:
      fo3_caling = False

    if fo3_acd[-1] == -0.1 and fo3_error == False:
      print "[%s] fo3 error data flag." % fo3_acd.getDate(-1)
      fo3_error = True
    elif fo3_acd[-1] != -0.1 and fo3_error == True:
      fo3_error = False

  print "[%s] Flight ending, acquiring two minutes of data." % time_str()
  server.sleep(2 * 60) ## Get more data after landing
  updater.update() ## Get last bit of data.

  print "[%s] Outputting file to %s" % (time_str(), output_file_str(server))
  out_file_name = output_file_str(server)
  open(out_file_name, 'w').write(variables.csv())

  mail_time = time_str()
  #sendMail(["ryano@ucar.edu"],
           #"Data from flight " + mail_time, \
           #"Attached is data from flight on " + mail_time,
           #[out_file_name])

  print "[%s] Sent mail." % time_str()
  time.sleep(1800)
  server.stop()
