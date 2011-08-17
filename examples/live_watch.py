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
from NcarChem.watch import watcher
import datetime

## Email libraries
import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email.MIMEText import MIMEText
from email.Utils import COMMASPACE, formatdate
from email import Encoders

## System
import os
## --------------------------------------------------------------------------
## Functions
## --------------------------------------------------------------------------

## sendMail in watch module is empty, it must be filled out later in order
## to send emails. This was done because there is no cross platform, cross
## mail server implementation that works to send emails except an SMTP server,
## which many users do not have running on their personal machines.
def sendMail(flight_info=None, files=None):
  """
  Mail function copied from Stack Overflow. Uses gmail account for SMTP server.
  """
  pw = open(".pass", 'r').read().split("\n")
  fro = pw[0]
  to = "ryano@ucar.edu"
  project_name = flight_info['ProjectName']
  flight_number = flight_info['FlightNumber']

  msg = MIMEMultipart()
  msg['From'] = fro
  msg['To'] = to
  msg['Date'] = formatdate(localtime=True)
  msg['Subject'] = "Data from flight %s_%s" % (project_name, flight_number)

  body = ("Data from flight %s of project %s attached." %
          (flight_number, project_name))
  msg.attach(MIMEText(body))

  ## MIME type attachments accepted by most servers
  for file in files:
      part = MIMEBase('application', "octet-stream")
      part.set_payload(open(file, "rb").read())
      Encoders.encode_base64(part)
      part.add_header('Content-Disposition', 'attachment; filename="%s"'
                     % os.path.basename(file))
      msg.attach(part)

  ## Alternative to storing password in plaintext in git repo.
  server = smtplib.SMTP('smtp.gmail.com', 587)

  ## Send mail, gmail specific
  server.starttls()
  server.login(fro, pw[1])
  server.sendmail(fro, to, msg.as_string())
  server.quit()
## --------------------------------------------------------------------------
## Classes
## --------------------------------------------------------------------------

## --------------------------------------------------------------------------
## Start command line interface (main)
## --------------------------------------------------------------------------

if __name__ == "__main__":
  import sys

  ## Functions used to determine if coraw_al is out of range
  def setup_co(self, *args, **kwds):
    self.cal = False

  def process_co(self, tm, data):
    coraw_al = data[0]

    if coraw_al <= 8000 and self.cal == False:
      self.pnt("[%s] CO cal occuring." % tm)
      self.cal = True
    elif coraw_al > 8000 and self.cal == True:
      self.cal = False


  ## Start watching the server
  watch_server = watcher(database="GV",
                         host="127.0.0.1",
                         user="postgres",
                         simulate_start_time=
                           datetime.datetime(2011, 8, 9, 14, 0, 0),
                         simulate_file=sys.argv[1],
                         variables=('ggalt', 'tasx',
                                    'coraw_al', 'co_qlive',
                                    'co2_qlive', 'ch4_qlive',
                                    'atx', 'o3mr_csd', 'psxc', 'theta', 'wic')
                         )

  watch_server.attachAlgo(variables=('coraw_al',), start_fn=setup_co, process_fn=process_co)

  watch_server.runOnce()
