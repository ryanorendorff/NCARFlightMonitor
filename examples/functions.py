#!/usr/bin/env python
# encoding: utf-8

## Sample functions to use with the NcarChem.watcher class
##
## Author: Ryan Orendorff <ryano@ucar.edu>
## Date: 30/08/11 12:56:44

## --------------------------------------------------------------------------
## Imports and Globals
## --------------------------------------------------------------------------
import datetime
import os
import time

## Email libraries
import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email.MIMEText import MIMEText
from email.Utils import COMMASPACE, formatdate
from email import Encoders

## --------------------------------------------------------------------------
## Functions
## --------------------------------------------------------------------------


## Functions for determining if the coraw_al variable is in a calibration
## mode.
def setup_co(self, *args, **kwds):
    """ Setup for the coraw_al calibration. """
    ## All variables starting with `self` are persistant across process
    ## function runs and can be called in the function process. This can be
    ## useful for running averages and the like.
    self.cal = False
    self.last_cal_time = self.flight_start_time
    self.time_interval_lower = datetime.timedelta(seconds=3300)
    self.time_interval_upper = datetime.timedelta(seconds=3900)
    self.time_late_flag = False


def process_co(self, tm, data):
    """ Is the coraw_al value below calibrating for the current data point? """
    coraw_al = data[0]

    ## Is a calibration late? Starts measuring from start time of flight, and
    ## then measures if cals happen every hour
    if ((tm - self.last_cal_time) >= self.time_interval_upper
             and self.time_late_flag == False):
        self.log.print_msg("CO cal is late.", tm)
        self.time_late_flag = True
    elif ((tm - self.last_cal_time) < self.time_interval_upper
             and self.time_late_flag == True):
        self.time_late_flag = False

    ## Are we calibrating
    if coraw_al <= 8000 and self.cal == False:
        self.log.print_msg("CO cal occuring.", tm)

        if (tm - self.last_cal_time) < self.time_interval_lower:
            self.log.print_msg("CO cal is early.", tm)

        self.last_cal_time = tm
        self.cal = True
    ## Reset so that the log message does not appear thousands of times.
    elif coraw_al > 8000 and self.cal == True:
        self.cal = False


## sendMail in watch module is empty, it must be filled out later in order
## to send emails. This was done because there is no cross platform, cross
## mail server implementation that works to send emails except an SMTP server,
## which many users do not have running on their personal machines.
def sendMail(flight_info=None, files=None, body_msg=None):
    """
    Mail function copied from Stack Overflow. Uses gmail account for SMTP
    server.
    """
    pw = open(".pass", 'r').read().split("\n")
    fro = pw[0]
    to = ["ryano@ucar.edu"]
    project_name = flight_info['ProjectName']
    flight_number = flight_info['FlightNumber']

    msg = MIMEMultipart()
    msg['From'] = fro
    msg['To'] = ", ".join(to)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = "Data from flight %s_%s" % (project_name, flight_number)

    if body_msg is not None:
        body = body_msg
    else:
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
