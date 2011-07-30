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
from twisted.words.protocols import irc
from twisted.internet import protocol, reactor, ssl, task




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


class live_watch():

  def __init__(self):
    self.server = None
    self.updater = None
    self.variables = None
    self.fo3_caling = False
    self.fo3_error = False
    self.flying_now = False

    NManager = NDatabaseManager()
    NManager.start()
    self.server = NManager.Server(database="C130",
                                  simulate_start_time=\
                                    datetime.datetime(2011, 7, 28, 14, 0, 0),
                                  simulate_fast=True)
    #server = NManager.Server(database="C130",
                             #simulate_start_time=\
                               #datetime.datetime(2011, 7, 28, 20, 12, 0),
                             #simulate_fast=True)
    #server = NManager.Server(database="C130")

    self.variables = NcarChem.data.NVarSet('ggalt', 'tasx', 'atx','psfdc',
                                      'fo3_acd',
                                      'ch4_pic', 'co2_pic',
                                      'dkl_mc', 'idxl_mc', 'pp2fl_mc', 'pwrl_mc')
    self.updater = NDatabaseLiveUpdater(server=self.server, variables=self.variables)

  def zeusMsg(self, channel, message):
    try:
      self.zeusbot.msg(channel, message)
    except Exception, e:
      print "Could not message chat server"

  def process(self):
    #print "Processing"
    if not self.server.flying():
      if self.flying_now == False:
        self.server.reconnect() ## Done to ensure good connection.
        print "[%s] Waiting for flight." % time_str()
        self.server.sleep(5*60)
      else:
        self.zeusMsg('#co','Landed.')
        print "[%s] Flight ending, acquiring two minutes of data." % time_str()
        self.server.sleep(2 * 60) ## Get more data after landing
        self.updater.update() ## Get last bit of data.

        print "[%s] Outputting file to %s" % (time_str(), output_file_str(self.server))
        out_file_name = output_file_str(self.server)
        try:
          open(out_file_name, 'w').write(self.variables.csv())
          mail_time = time_str()
          sendMail(["ryano@ucar.edu"],
                   "Data from flight " + mail_time, \
                   "Attached is data from flight on " + mail_time,
                   [out_file_name])

          print "[%s] Sent mail." % time_str()
        except Exception, e:
          print "Could not send mail"
          print e

        self.flying_now = False

    else:
      if self.flying_now == False:
        self.zeusMsg('#co', "[%s] In flight." % time_str())
        self.flying_now = True
        self.variables.clearData()
        self.variables.addData(self.server.getData(start_time="-60 MINUTE",
                                         variables=self.variables.keys()))

      BAD_DATA = -32767
      psfdc = self.variables['psfdc']
      fo3_acd = self.variables['fo3_acd']
      co2 = self.variables['co2_pic']
      ch4 = self.variables['ch4_pic']

      self.updater.update() ## Can return none, sleeps for at least DatRate seconds.

      if not (350 <= co2[-1] <= 500):
        self.zeusMsg('#co', "[%sZ] CO2 out of bounds." % co2.getDate(-1))

      if not (1.7 <= ch4[-1] <= 1.9):
        self.zeusMsg('#co', "[%sZ] CH4 out of bounds." % ch4.getDate(-1))

      if 0 <= fo3_acd[-1] <= 0.09 and psfdc[-1]*0.75006 < 745 and self.fo3_caling == False:
        self.zeusMsg('#co', "[%sZ] fO3 cal occuring." % fo3_acd.getDate(-1))
        self.fo3_caling = True
      elif fo3_acd[-1] > 0.09 and self.fo3_caling == True:
        self.fo3_caling = False

      if fo3_acd[-1] == -0.1 and self.fo3_error == False:
        self.zeusMsg('#co', "[%sZ] fo3 error data flag." % fo3_acd.getDate(-1))
        self.fo3_error = True
      elif fo3_acd[-1] != -0.1 and self.fo3_error == True:
        self.fo3_error = False


class ZeusBot(irc.IRCClient):
    password = 'ch@773rb0x'

    def _get_nickname(self):
        return self.factory.nickname
    nickname = property(_get_nickname)

    def signedOn(self):
        self.join(self.factory.channel)
        self.join('#ICET')
        self.join('#C130Q')
        print "Signed on as %s." % (self.nickname,)
        #self.msg('#co', "Zeus is home baby")
        live_watch.zeusbot = self



    def joined(self, channel):
        print "Joined %s." % (channel,)

    def privmsg(self, user, channel, msg):
        print msg

class ZeusBotFactory(protocol.ClientFactory):
    protocol = ZeusBot

    def __init__(self, channel, nickname='ZeusBot'):
        self.channel = channel
        self.nickname = nickname

    def clientConnectionLost(self, connector, reason):
        print "Lost connection (%s), reconnecting." % (reason,)
        connector.connect()

    def clientConnectionFailed(self, connector, reason):
        print "Could not connect: %s" % (reason,)

## --------------------------------------------------------------------------
## Start command line interface (main)
## --------------------------------------------------------------------------

NDatabaseManager.register('Server', NDatabase)

if __name__ == "__main__":

  watcher = live_watch()
  l = task.LoopingCall(watcher.process)
  l.start(0.001)
  reactor.connectSSL('rdcc.guest.ucar.edu',6668, ZeusBotFactory('#' + 'co'), ssl.ClientContextFactory())
  reactor.run()

