#!/usr/bin/env python
# encoding: utf-8

## Connect to EOL Real time databases.
##
## Author: Ryan Orendorff <ryano@ucar.edu>
## Date: 21/07/11 10:31:47
##

## Syntax notes for coders who are not author
## - A double pound (##) is a comment, a single is commented code


## --------------------------------------------------------------------------
## Imports and Globals
## --------------------------------------------------------------------------
import psycopg2 ## PostGreSQL module, http://www.initd.org/psycopg/
import datetime
import time
import sys
from multiprocessing.managers import BaseManager
import datafile
import random

## --------------------------------------------------------------------------
## Functions
## --------------------------------------------------------------------------

def _loadFile(file_path, dbname, host, user, password, dbstart):
  try:
    file_str = open(file_path, 'r').read()
  except Exception, e:
    print e
  header, labels, data = datafile._parseIntoHeaderLabelsData(file_str)

  if header != None:
    SQL_CMDS = datafile._createSimSqlFromHeader(header)

  data = datafile._createDataFromString(labels, data)

  conn = psycopg2.connect(database=dbstart, \
                          user=user, \
                          host=host, \
                          password=password)
  conn.set_isolation_level(0)
  cursor = conn.cursor()
  cursor.execute("CREATE DATABASE %s;" % dbname)
  cursor.close()
  conn.close()

  conn = psycopg2.connect(database=dbname, \
                          user=user, \
                          host=host, \
                          password=password)

  cursor = conn.cursor()
  conn.set_isolation_level(0)
  for cmd in SQL_CMDS:
    cursor.execute(cmd)

  VARS = ""
  for var in data[0]:
    VARS += "%s," % var.lower()

  INSERT_CMD = "INSERT INTO raf_lrt (" + VARS.rstrip(', ') + ") VALUES (%s);"
  for row in data[1:]:
    data_piece = ""
    for item in row:
      data_piece += "'%s'," % str(item)
    cursor.execute(INSERT_CMD % data_piece.rstrip(', '))
  cursor.close()
  conn.close()

## --------------------------------------------------------------------------
## Classes
## --------------------------------------------------------------------------


class NDatabaseManager(BaseManager):
  pass


class NDatabaseLiveUpdater(object):

  def __init__(self, server=None, variables=None):
    self.server = server
    self._last_update_time = None
    self._vars = variables

    current_time = server.getData(number_entries=1)
    self._last_update_time = current_time[0][0]

  def update(self):
    data = self.server.getData(start_time=self._last_update_time,\
                               variables=self._vars.keys())

    if len(data) != 0:
      self._last_update_time = data[-1][0]
      self._vars.addData(data)

    self.server.sleep()


class NDatabase(object):

  def __init__(self, host="eol-rt-data.guest.ucar.edu",
                     user="ads",
                     password="",
                     database=None,
                     simulate_start_time=None,
                     simulate_fast=False,
                     simulate_file=None):
    self._database = database
    self._user = user
    self._password = password
    self._host = host
    self.variable_list = ()
    self._flight_info = None
    self._start_time = datetime.datetime.now()
    self._simulate_start_time = simulate_start_time
    self._current_time = simulate_start_time if simulate_start_time != None\
                    else self._start_time
    self._conn = None
    self._simulate_fast = simulate_fast if simulate_start_time != None else\
                          False
    self._sql_bad_attempts = 0
    self._simulate_start_db = None

    if database == None:
      raise ValueError('Database must be specified')

    if database == "C130" or database == "GV":
      self._database = "real-time-" + database

    if simulate_file != None:
      dbname = "test" + str(int(random.random() * 1000000))
      self._simulate_start_db = self._database
      tmp_db = self._database
      self._database = dbname
      _loadFile(simulate_file, dbname, self._host, self._user, self._password, tmp_db)

    self.reconnect()

    cursor = self._conn.cursor()
    cursor.execute("SELECT column_name FROM Information_Schema.Columns WHERE\
                    table_name='raf_lrt'")
    variable_list = cursor.fetchall()

    for var in variable_list:
      self.variable_list = var + self.variable_list

    cursor.execute("SELECT * FROM global_attributes;")
    self._flight_info = dict(cursor.fetchall())

    cursor.close()
    self._running = True

  def __del__(self):
    try:
      self._conn.close()
    except Exception, e:
      pass
    if self._simulate_start_db != None and self._running == True:
      import psycopg2 ## Load library back in before finally taking stuff down
      conn = psycopg2.connect(database=self._simulate_start_db, \
                              user=self._user, \
                              host=self._host, \
                              password=self._password)
      conn.set_isolation_level(0)
      cursor = conn.cursor()
      cursor.execute("DROP DATABASE %s;" % self._database)
      cursor.close()
      conn.close()

  def stop(self):
    self.__del__()
    self._running = False

  def reconnect(self):
    try:
      self._conn = psycopg2.connect(database=self._database, \
                                    user=self._user, \
                                    host=self._host, \
                                    password=self._password)
    except Exception, e:
      print e

  def flying(self):
    speed = 0
    data = (self.getData(number_entries=1, variables=('tasx',)))
    if len(data) != 0:
      speed = data[0][1]
    if speed > 50:
      return True
    else:
      return False

  def sleep(self, sleep_time=0):
    if sleep_time == 0:
      sleep_time = int(self._flight_info['DataRate'])

    if self._simulate_fast:
      self._current_time += datetime.timedelta(seconds=sleep_time)
    else:
      time.sleep(sleep_time)

  def getFlightInformation(self):
    return self._flight_info

  def _getSimulatedCurrentTime(self):
    if self._simulate_fast:
      return ((self._current_time - self._simulate_start_time) \
             + self._simulate_start_time).replace(microsecond=0)
    else:
      return ((datetime.datetime.now() - self._start_time) \
             + self._simulate_start_time).replace(microsecond=0)

  def getData(self, variables=None, start_time=None, end_time=None,\
                    number_entries=None):
    NOW = str(self._getSimulatedCurrentTime()) \
          if self._simulate_start_time != None \
          else "NOW()"

    if isinstance(start_time, datetime.datetime):
      start_time = str(start_time)

    sql_command = "SELECT "


    var_str = "datetime, "
    if variables != None:
      for var in variables:
        if var in self.variable_list:
          var_str += var + ", "
        else:
          print >> sys.stderr, "Could not add variable %s, does not exist"\
                                % var

    var_str = var_str.rstrip(', ')
    sql_command += var_str + " FROM raf_lrt "

    cursor = self._conn.cursor()

    time_interval = ""
    if   end_time == None and start_time != None and number_entries == None:
      ## Assume -# INTERVAL syntax, SQL style.
      if start_time[0] == "-" or start_time[0] == "+":
        if self._simulate_start_time:
          time_interval = "WHERE (datetime > timestamp '" + NOW + \
                          "' + interval '" + start_time + "') \
                          AND (datetime <= '" + NOW + "')"
        else:
          time_interval = "WHERE datetime > " + NOW + " " + \
                          start_time[0] + " interval '" + start_time[1:] + "'"
      else: ## Assume explicit date given, SQL style.
        if self._simulate_start_time != None:
          time_interval = "WHERE (datetime > '%s' \
                                  AND datetime <= '%s')" % (start_time, NOW)
        else:
          time_interval = "WHERE datetime > '" + start_time + "'"
    elif end_time == None and start_time != None and number_entries != None:
      if start_time[0] == "-" or start_time[0] == "+":
        if self._simulate_start_time:
          time_interval = "WHERE datetime > (timestamp '" + NOW + \
                          "' + interval '" + start_time + "')"
        else:
          time_interval = "WHERE datetime > " + NOW + " " + \
                          start_time[0] + " interval '" + start_time[1:] + "'"
      else: ## Assume explicit date given, SQL style.
        if self._simulate_start_time != None:
          time_interval = "WHERE (datetime > '%s' \
                                  AND datetime <= '%s')" % (start_time, NOW)
        else:
          time_interval = "WHERE datetime > '" + start_time + "'"
      time_interval += " ORDER BY datetime ASC LIMIT " + str(number_entries)
    elif end_time == None and start_time == None and number_entries != None:
      if self._simulate_start_time != None:
        time_interval = "WHERE datetime <= '%s' \
                         ORDER BY datetime DESC LIMIT %s" \
                        % (NOW, str(number_entries))
      else:
        time_interval = " ORDER BY datetime DESC LIMIT " + str(number_entries)
    else:
      print >> sys.stderr, "Invalid time scale change"
      return

    sql_command += time_interval + ";"
    data = []
    try:
      cursor.execute(sql_command)
      data = cursor.fetchall()
    except Exception, e:
      print >> sys.stderr, "SQL Command failed: " + sql_command
      self._sql_bad_attempts += 1
      if self._sql_bad_attempts % 10 == 0:
        print >> sys.stderr, "Ten SQL commands failed, \
                              attempting to reconnect to the server."
        self.reconnect()

    cursor.close()

    return data



## --------------------------------------------------------------------------
## Start command line interface (main)
## --------------------------------------------------------------------------


if __name__ == "__main__":

  server = NDatabase(database="test",
                     user="postgres",
                     host="127.0.0.1",
                     simulate_start_time=\
                       datetime.datetime(2011, 7, 28, 14, 0, 0),
                     simulate_fast=True,
                     simulate_file=sys.argv[1])

  print "Done loading"
  time.sleep(124121)
