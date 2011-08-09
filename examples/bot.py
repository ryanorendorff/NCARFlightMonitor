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
from NcarChem.watch import watcher
import NcarChem
import datetime

import os, time
import threading
from twisted.words.protocols import irc
from twisted.internet import protocol, reactor, ssl, task


import sys


## --------------------------------------------------------------------------
## Functions
## --------------------------------------------------------------------------

def setup(self, *extra, **kwds):
  self.cal = False
  self.fo3_acd = self.variables[0]
  self.psfdc = self.variables[1]

def process(self):
  if 0 <= self.fo3_acd[-1] <= 0.09 and self.psfdc[-1]*0.75006 < 745 and self.cal == False:
    self.pnt( "[%s] fO3 cal occuring." % (str(self.fo3_acd.getDate(-1)) + "Z"))
    self.cal = True
  elif self.fo3_acd[-1] > 0.09 and self.cal == True:
    self.cal = False

def zeusMsg(self, message):
  try:
    self.zeusbot.msg("#co", message)
  except Exception, e:
    print "Could not message chat server"
## --------------------------------------------------------------------------
## Classes
## --------------------------------------------------------------------------


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

    def joined(self, channel):
        print "Joined %s." % (channel,)

    def privmsg(self, user, channel, msg):
      if msg == "start":
        watcher.zeusbot = self
        watcher.pnt=zeusMsg
        NcarChem.algos.zeusbot = self
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
        l = task.LoopingCall(watch_server.run)
        l.start(0.01)
      elif msg == "stop":
        pass

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

  reactor.connectSSL('rdcc.guest.ucar.edu',6668, ZeusBotFactory('#' + 'co'), ssl.ClientContextFactory())
  reactor.run()

