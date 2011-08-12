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

#### Inter-package
import datafile
import data

## PSQL interface
import psycopg2 ## PostGreSQL module, http://www.initd.org/psycopg/

## Allows for multiple child processes off one parent program.
from multiprocessing.managers import BaseManager

## Time imports
import datetime
import time
import random

## Used for sys.stderr
import sys

## --------------------------------------------------------------------------
## Functions
## --------------------------------------------------------------------------

def _loadFile(file_path, dbname, host, user, password, dbstart):
  """
  Loads a .asc file with a header into a sql database for testing.
  """
  nfile = datafile.NRTFile(file_path)

  SQL_CMDS = nfile.getSql()
  labels = nfile.labels
  data = nfile.data

  ## Create a new test database
  conn = psycopg2.connect(database=dbstart, \
                          user=user, \
                          host=host, \
                          password=password)
  conn.set_isolation_level(0)
  cursor = conn.cursor()
  cursor.execute("CREATE DATABASE %s;" % dbname)
  cursor.close()
  conn.close()

  ## Join the new database
  conn = psycopg2.connect(database=dbname, \
                          user=user, \
                          host=host, \
                          password=password)

  cursor = conn.cursor()
  conn.set_isolation_level(0)
  for cmd in SQL_CMDS:
    cursor.execute(cmd)

  VARS = ""
  for var in labels:
    VARS += "%s," % var.lower()

  ## Add data into test dataabase
  INSERT_CMD = "INSERT INTO raf_lrt (" + VARS.rstrip(', ') + ") VALUES (%s);"
  for row in data:
    data_piece = ""
    for item in row:
      data_piece += "'%s'," % str(item)
    cursor.execute(INSERT_CMD % data_piece.rstrip(', '))

  ## Close all connections to the database, will be picked up later.
  cursor.close()
  conn.close()

## --------------------------------------------------------------------------
## Classes
## --------------------------------------------------------------------------


class NDatabaseManager(BaseManager):
  """
  Used for multiprocess purposes.
  """
  pass


class NDatabaseLiveUpdater(object):
  """
  Used to update the data inside an NVarSet with the newest data from the
  server. This provides easy server use without knowing server/NVar
  functions.
  """

  def __init__(self, server=None, variables=None):
    self.server = server
    self._last_update_time = None

    ## Get all the variables is they are not specified
    if variables is None:
      self._vars = data.NVarSet(server.variable_list)
    else:
      self._vars = variables  ## of type NVarSet

    current_time = server.getData(number_entries=1)
    self._last_update_time = current_time[0][0]

  def update(self):
    """
    Update attached variables with new data, and then sleep the server so it
    polls less frequently.
    """
    data = self.server.getData(start_time=self._last_update_time,\
                               variables=self._vars.keys())

    if len(data) != 0:
      self._last_update_time = data[-1][0]
      self._vars.addData(data)

    self.server.sleep()


class NDatabase(object):
  """
  An interface to the EOL PostGresQL database. It is designed to return data
  normally picked up by Aeros without knowing the SQL commands necessary.
  """

  ## All defaults used at RAF as of 10/08/11 02:48:30
  def __init__(self, host="eol-rt-data.guest.ucar.edu",
                     user="ads",
                     password="",
                     database=None,
                     simulate_start_time=None,
                     simulate_fast=False,
                     simulate_file=None):
    ## Database related
    self._database = database
    self._user = user
    self._password = password
    self._host = host
    self.variable_list = ()
    self._flight_info = None
    self._conn = None
    self._running = True
    self._sql_bad_attempts = 0

    ## Time (which can be spoofed with the simulate parameters
    self._start_time = datetime.datetime.now()
    self._current_time = (simulate_start_time if simulate_start_time is not None
                          else self._start_time)

    ## Simulate variables
    self._simulate_start_db = None
    self._simulate_start_time = simulate_start_time
    self._simulate_fast = (simulate_fast if simulate_start_time is not None else
                           False)

    ## Error Checking
    if database is None:
      raise ValueError('Database must be specified')

    ## Shorthand for C130 and GV
    if database == "C130" or database == "GV":
      self._database = "real-time-" + database

    ## If we are simulating from a file, load the file with
    ## a random database name
    if simulate_file is not None:
      dbname = "test" + str(int(random.random() * 1000000))
      self._simulate_start_db = self._database
      tmp_db = self._database
      self._database = dbname
      _loadFile(simulate_file,
                dbname, self._host, self._user, self._password, tmp_db)

    ## Create first connection to database
    self.reconnect()

    ## Grab variable list from server
    cursor = self._conn.cursor()
    cursor.execute("SELECT column_name FROM Information_Schema.Columns WHERE\
                    table_name='raf_lrt'")
    variable_list = cursor.fetchall()

    ## Variable list is a list of single entry tuples, make into tuple
    self.variable_list = tuple([ col[0] for col in variable_list])

    ## Get flight information
    cursor.execute("SELECT * FROM global_attributes;")
    self._flight_info = dict(cursor.fetchall())

    ## We are done
    cursor.close()

  def __del__(self):
    try:
      self._conn.close()
    except Exception, e:
      pass
    if self._simulate_start_db is not None and self._running is True:
      try:
        ## Load library back in before finally taking stuff down
        import psycopg2
        conn = psycopg2.connect(database=self._simulate_start_db, \
                                user=self._user, \
                                host=self._host, \
                                password=self._password)
        conn.set_isolation_level(0)
        cursor = conn.cursor()
        cursor.execute("DROP DATABASE %s;" % self._database)
        cursor.close()
        conn.close()
      except Exception, e:
        ## Did not work, but don't change error exit code
        print "Could not load simulated db"

  def stop(self):
    """
    Stop running the server. Use if in an infinite loop.
    """
    self.__del__()
    self._running = False

  def reconnect(self):
    """
    Sometimes the connection goes stale and will not recognize when the server
    has replaced its database with a fresh one. Hence this function should be
    called periodically to update when these database transitions occur.
    """
    try:
      self._conn = psycopg2.connect(database=self._database, \
                                    user=self._user, \
                                    host=self._host, \
                                    password=self._password)
    except Exception, e:
      print e

  def flying(self):
    """
    Is the airplane going faster than 50 m/s right now?
    """
    speed = 0
    data = (self.getData(number_entries=1, variables=('tasx',)))
    if len(data) != 0:
      speed = data[0][1]
    if speed > 50:
      return True
    else:
      return False

  def sleep(self, sleep_time=0):
    """
    Used to wait for new data. If in simulation mode this increments time
    forward.
    """
    ## Get the data rate from the server, usually 3 seconds
    if sleep_time == 0:
      sleep_time = int(self._flight_info['DataRate'])

    if self._simulate_fast:
      self._current_time += datetime.timedelta(seconds=sleep_time)
    else:
      time.sleep(sleep_time)

  def getTimeStr(self):
    """
    Returns the most recent datapoint time with a Z attached to signify
    Zulu time.
    """
    return str(self._getSimulatedCurrentTime()) + "Z"


  def getFlightInformation(self):
    """
    Get the flight information. This is updated when a new flight is detected.
    """
    return self._flight_info

  def _getSimulatedCurrentTime(self):
    """
    Get the most recent time from the data. Will give a simulated time if
    in simulation mode.
    """
    if self._simulate_fast:
      return ((self._current_time - self._simulate_start_time) \
             + self._simulate_start_time).replace(microsecond=0)
    else:
      return ((datetime.datetime.now() - self._start_time) \
             + self._simulate_start_time).replace(microsecond=0)

  def getData(self, variables=None,
                    start_time=None, end_time=None,
                    number_entries=None):
    """
    Get data from the server for the selected variables, where the variables
    are a tuple/list. Can be manipulated to get data from a range or just a
    certain number of entries. The times can also be intervals such as
    start_time = "-60 MINUTES".
    """
    ## Use server now function if not in simulation mode, otherwise perform
    ## the SQL query with the simulated current time as the upper bound.
    NOW = (str(self._getSimulatedCurrentTime())
          if self._simulate_start_time is not None
          else "NOW()")

    if isinstance(start_time, datetime.datetime):
      start_time = str(start_time)

    ## Start building SQL command
    sql_command = "SELECT "

    ## Variables to get. Will always get datetime.
    var_str = "datetime, "
    if variables is not None:
      for var in variables:
        if var in self.variable_list:
          var_str += var + ", "
        else:
          print >> sys.stderr, "Could not add variable %s, does not exist"\
                                % var
    var_str = var_str.rstrip(', ')

    ## All Aeros displayable data from raf_lrt
    sql_command += var_str + " FROM raf_lrt "

    ## Open SQL connection
    cursor = self._conn.cursor()

    ## TODO: Clean time interval part of server.getData
    ## Start creating time interval string.
    time_interval = ""
    if   end_time is None and start_time is not None and number_entries is None:
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
        if self._simulate_start_time is not None:
          time_interval = "WHERE (datetime > '%s' \
                                  AND datetime <= '%s')" % (start_time, NOW)
        else:
          time_interval = "WHERE datetime > '" + start_time + "'"
    elif end_time is None and start_time is not None and number_entries is not None:
      if start_time[0] == "-" or start_time[0] == "+":
        if self._simulate_start_time:
          time_interval = "WHERE datetime > (timestamp '" + NOW + \
                          "' + interval '" + start_time + "')"
        else:
          time_interval = "WHERE datetime > " + NOW + " " + \
                          start_time[0] + " interval '" + start_time[1:] + "'"
      else: ## Assume explicit date given, SQL style.
        if self._simulate_start_time is not None:
          time_interval = "WHERE (datetime > '%s' \
                                  AND datetime <= '%s')" % (start_time, NOW)
        else:
          time_interval = "WHERE datetime > '" + start_time + "'"
      time_interval += " ORDER BY datetime ASC LIMIT " + str(number_entries)
    elif end_time is None and start_time is None and number_entries is not None:
      if self._simulate_start_time is not None:
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

  def getDatabaseStructure(self):
    """
    Get the database structure, return as string. See documentation for
    the string formatting. This information goes into the header of
    outputted files.
    """
    cursor = self._conn.cursor()

    ## Get a list of Tables
    cursor.execute("SELECT table_name FROM information_schema.tables " +\
                   "WHERE table_type = 'BASE TABLE' "+\
                   "AND table_schema NOT IN " +\
                     "('pg_catalog', 'information_schema');")
    tables = cursor.fetchall()
    tables = tuple([col[0] for col in tables])

    ## Get a list of constrains on those tables, as a dict{tbl_name:constrain}
    cursor.execute("SELECT t.table_name, k.column_name " + \
                   "FROM information_schema.table_constraints " +\
                   "T INNER JOIN information_schema.key_column_usage " +\
                   "K ON T.CONSTRAINT_NAME = k.constraint_name " +\
                   "WHERE T.CONSTRAINT_TYPE = 'PRIMARY KEY' -- " +\
                   "AND T.TABLE_NAME = 'table_name' " +\
                   "ORDER BY T.TABLE_NAME, K.ORDINAL_POSITION;")
    constraints = dict(cursor.fetchall())

    ## Start building output string
    output = ""
    for table in tables:
      ## Start building string for a specific table.
      tbl_string = "%s=" % table

      ## Get the listing of columns and their data types.
      cursor.execute("SELECT " +\
                     "column_name, data_type, is_nullable, " +\
                     "character_maximum_length, udt_name " +\
                     "FROM information_schema.columns " +\
                     "WHERE TABLE_NAME = '%s' " % table +\
                     "ORDER BY ordinal_position;");
      columns = cursor.fetchall()

      ## Each column string is (COLUMNS, (col1, type, null?), etc)
      tbl_string += "('COLUMNS',"
      for col in columns:
        ## Get column information from returned string.
        col_name = col[0]

        ## Unfortunately  data_type from SQL returns ARRAY instead of integer[]
        ## or double precision[], and so these types must be reconstructed
        ## from their raw variable types.
        if col[4] == "_int4":
          col_base_type = "integer[]"
        elif col[4] == "_float8":
          col_base_type = "double precision[]"
        else:
          col_base_type = col[1]

        ## Similarly data_type does not give the length of a char array.
        ## It must be reconstructed as character(#) from the SQL variable
        ## character_maximum_length. Does not affect not char values.
        col_type = col_base_type + ('(' + str(col[3]) + ')' if col[3] is not None else "")

        ## Make some values forced to be something besides NULL
        col_null = 'NOT NULL' if col[2] == 'NO' else ''

        ## (name, compiled_type, am I null?)
        tbl_string += "('%s','%s','%s')," % (col_name, col_type, col_null)

      tbl_string = tbl_string.rstrip(', ') + ')'

      ## Add constraint if applicable
      if table in constraints:
        tbl_string += ";('CONSTRAINT', '%s')" % constraints[table]

      ## Data in table goes after the % character
      tbl_string += '%'

      ## Ignore raf_lrt, that is where the Aeros data comes from.
      if table != "raf_lrt":
        cursor.execute("SELECT * from %s;" % table);
        data = cursor.fetchall()
        data = "" if data == [] else tuple(data)

        ## Data is just the string representation of a tuple, allows for
        ## data to be imported using the eval() function
        tbl_string += str(data).replace("Uncorr'd Raw", "Uncorr''d Raw")

      ## Add the table to the output string
      output += tbl_string + '\n'

    ## Finally, we have all the tables, end query.
    cursor.close()
    return output.strip('\n')


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
