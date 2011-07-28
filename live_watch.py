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
from NcarChem.database import NCARDatabase
from NcarChem.database import NCARDatabaseLiveUpdater, NCARDatabaseManager
from NcarChem.data import NCARVar
import NcarChem
import time
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


def flying(server):
  speed = 0
  data = (server.getData(number_entries=1, variables=('tasx',)))
  if len(data) != 0:
    speed = data[0][1]
  if speed > 50:
    return True
  else:
    return False

## --------------------------------------------------------------------------
## Classes
## --------------------------------------------------------------------------

## --------------------------------------------------------------------------
## Start command line interface (main)
## --------------------------------------------------------------------------

NCARDatabaseManager.register('Server',NCARDatabase)

if __name__ == "__main__":

  NCARManager = NCARDatabaseManager()
  NCARManager.start()
  #server = NCARManager.Server(database="C130", simulate_time = datetime.datetime(2011,7,27,21,07,00))
  server = NCARManager.Server(database="C130")


  while(not flying(server)):
    server.reconnect()
    print "Waiting for flight [" + str(datetime.datetime.now()) + "]"
    time.sleep(1800)
  print "In flight [" + str(datetime.datetime.now()) + "]"

  variables=('datetime', 'ch4_pic', 'co2_pic', 'fo3_acd', 'idxl_mc', 'pp2fl_mc', 'pwrl_mc', 'dkl_mc', 'atx', 'ggalt', 'tasx' )
  NCARVar_list = NcarChem.data.createVarList(variables)

  pre_takeoff = server.getData(start_time="-60 MINUTE", variables=variables)

  if len(pre_takeoff) != 0:
    pos = 1
    for var in NCARVar_list:
      var.addData([(column[0], column[pos]) for column in pre_takeoff])
      pos += 1

  updater = NCARDatabaseLiveUpdater(server=server, variables=NCARVar_list)

  while(flying(server)):
    updater.update() ## A blocking call
    #print NCARVar_list[0]

  time.sleep(10*60) ## Sleep for ten minutes after the flight, then get data.
  updater.update()

  output_string = ""
  for var in variables:
    output_string += var + ","
  output_string = output_string.rstrip(', ')

  output_string += '\n'
  for line in range(NCARVar_list[0].length):
    line = ""
    index = 0
    for var in NCARVar_list:
      line += str(var[index][1]) + ","
    line = line.rstrip(',') + "\n"
    output_string += line

  output_file = '/tmp/data-' + str(datetime.datetime.now()) + '.txt'
  output = open(output_file, 'w')
  print >>output, output_string
  output.close()

  sendMail(
          ["ryano@ucar.edu"],
          "Data from flight " + str(datetime.datetime.today()), "Attached is data from flight on " + str(datetime.datetime.today()),
          [output_file]
      )
