#!/usr/bin/env python
# encoding: utf-8

## Copyright 2011 Ryan Orendorff, NCAR under GPLv3
## See README.mkd for more details.

## A bot that outputs the wather class' informational messages to an IRC
## channel.
##
## IMPORTANT: Change the password on line 58 to IRC server password.
##
## Author: Ryan Orendorff <ryano@ucar.edu>
## Date: 30/08/11 12:47:17

## --------------------------------------------------------------------------
## Imports and Globals
## --------------------------------------------------------------------------
from NCARFlightMonitor.database import NDatabase
from NCARFlightMonitor.database import NDatabaseLiveUpdater, NDatabaseManager
from NCARFlightMonitor.data import NVar
from NCARFlightMonitor.watch import NWatcher, logger
import NCARFlightMonitor
import datetime

import os
import time
from twisted.words.protocols import irc
from twisted.internet import protocol, reactor, ssl, task

import sys
import functions

Zeus = None

## --------------------------------------------------------------------------
## Functions
## --------------------------------------------------------------------------


## Used to print messages to a chatroom
def zeusMsg(message, tm=None):
    try:
        if tm is not None:
            formatted_msg = "[%sZ] %s" % (tm, message)
            Zeus.msg("#co", formatted_msg)
        else:
            formatted_msg = message
            Zeus.msg("#co", formatted_msg)

        print formatted_msg
        return formatted_msg
    except Exception, e:
        print "Could not message chat server"

## --------------------------------------------------------------------------
## Classes
## --------------------------------------------------------------------------


class ZeusBot(irc.IRCClient):
    password = ''

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
            global Zeus
            Zeus = self

            watch_server = NWatcher(database="GV",
                                   email_fn=functions.sendMail,
                                   print_msg_fn=zeusMsg)

            watch_server.attachAlgo(variables=('coraw_al',),
                start_fn=functions.setup_co,
                process_fn=functions.process_co)

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
        connector.connect()


## --------------------------------------------------------------------------
## Start command line interface (main)
## --------------------------------------------------------------------------

if __name__ == "__main__":

    ## Connect to IRC server and run infinite loop.
    reactor.connectSSL('rdcc.guest.ucar.edu', 6668,
                       ZeusBotFactory('#' + 'co'), ssl.ClientContextFactory())
    reactor.run()
