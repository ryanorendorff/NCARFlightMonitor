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




## --------------------------------------------------------------------------
## Functions
## --------------------------------------------------------------------------
def time_str():
  return str(datetime.datetime.utcnow().replace(microsecond=0))+"Z"

def output_file_str(server):
  info = server.getFlightInformation()
  project = info['ProjectNumber']
  flight =  info['FlightNumber']
  return '/tmp/%s-%s-%s.asc' % (project, flight, datetime.datetime.utcnow().strftime("%Y_%m_%d-%H_%M_%S"))

def sendMail(to, subject, text, files=[],server="localhost"):
    assert type(to)==list
    assert type(files)==list
    fro = "ryano@ucar.edu"

    msg = MIMEMultipart()
    msg['From'] = fro
    msg['To'] = COMMASPACE.join(to)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject

    msg.attach( MIMEText(text) )

    for file in files:
        part = MIMEBase('application', "octet-stream")
        part.set_payload( open(file,"rb").read() )
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
    server.sendmail("ryano@ucar.edu", "ryano@ucar.edu",  msg.as_string())
    server.quit()



## --------------------------------------------------------------------------
## Classes
## --------------------------------------------------------------------------

## --------------------------------------------------------------------------
## Start command line interface (main)
## --------------------------------------------------------------------------

NDatabaseManager.register('Server',NDatabase)

if __name__ == "__main__":

  NManager = NDatabaseManager()
  NManager.start()
  #server = NManager.Server(database="C130", simulate_start_time = datetime.datetime(2011,7,28,14,0,0), simulate_fast=True)
  server = NManager.Server(database="C130", simulate_start_time = datetime.datetime(2011,7,28,20,12,0), simulate_fast=True)
  #server = NManager.Server(database="C130")


  while(not server.flying()):
    server.reconnect() ## Done to ensure good connection.
    print "[%s] Waiting for flight." % time_str()
    server.sleep(1800)
  print "[%s] In flight." % time_str()

  variables=NcarChem.data.NVarSet('ggalt', 'tasx', 'atx', 'fo3_acd', 'ch4_pic', 'co2_pic', 'dkl_mc', 'idxl_mc', 'pp2fl_mc', 'pwrl_mc')

  variables.addData(server.getData(start_time="-60 MINUTE", variables=variables.keys()))

  updater = NDatabaseLiveUpdater(server=server, variables=variables)
  while(server.flying()):
    updater.update() ## Can return none, sleeps for at least DatRate seconds.

  print "[%s] Flight ending, acquiring two minutes of data." % time_str()
  server.sleep(2*60) ## Sleep for ten minutes after the flight, then get data.
  updater.update() ## Get last bit of data.

  print "[%s] Outputting file to %s" %(time_str(), output_file_str(server))
  #open(output_file_str(server), 'w').write(variables.csv())

  #sendMail(
          #["ryano@ucar.edu"],
          #"Data from flight " + str(datetime.datetime.today()), "Attached is data from flight on " + str(datetime.datetime.today()),
          #[output_file]
      #)

  print "[%s] Sent mail." % time_str()
