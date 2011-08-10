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

## Used to determine if co is caling
def setup(self, *extra, **kwds):
  self.cal = False
  self.coraw_al = self.variables[0]

def process_co(self):
  if self.coraw_al[-1] <= 8000 and self.cal == False:
    self.pnt( "[%s] CO cal occuring." %\
              (str(self.coraw_al.getDate(-1)) + "Z"))
    self.cal = True
  elif self.coraw_al[-1] > 8000 and self.cal == True:
    self.cal = False


## Used to print messages to a chatroom
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
        print "Signed on as %s." % (self.nickname,)

    def joined(self, channel):
        print "Joined %s." % (channel,)

    def privmsg(self, user, channel, msg):
      ## /msg ZeusBot start to start watching server
      if msg == "start":
        watcher.zeusbot = self  ## Attach this bot instance
        watcher.pnt=zeusMsg  ## Change print function
        NcarChem.algos.zeusbot = self
        watch_server = watcher(database="GV",
                               variables=('ggalt','tasx','coraw_al'))
        watch_server.attachAlgo(variables=('coraw_al',),
                                start_fn=setup, process_fn=process)
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

  ## Connect to IRC server and run infinite loop.
  reactor.connectSSL('rdcc.guest.ucar.edu',6668, ZeusBotFactory('#' + 'co'), ssl.ClientContextFactory())
  reactor.run()

