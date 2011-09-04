#!/usr/bin/env python
# encoding: utf-8

## Connect to EOL Real time databases.
##
## Author: Ryan Orendorff <ryano@ucar.edu>
## Date: 21/07/11 10:31:47
##

## --------------------------------------------------------------------------
## Imports and Globals
## --------------------------------------------------------------------------

#### Intrapackage
import datafile
import data

## PostGreSQL module, http://www.initd.org/psycopg/
import psycopg2

## Allows for multiple child processes off one parent program.
from multiprocessing.managers import BaseManager

## Time imports
import datetime
import time
import random

## Used for sys.stderr
import sys

import math

## Unload server at program exit.
import atexit

## --------------------------------------------------------------------------
## Functions
## --------------------------------------------------------------------------


def __ending__(server):
    try:
        server._conn.close()
    except:
        pass

    if server._simulate_start_db is not None and server._running is True:
        try:
            ## Load library back in before finally taking stuff down
            conn = psycopg2.connect(database=server._simulate_start_db,
                                    user=server._user,
                                    host=server._host,
                                    password=server._password)
            conn.set_isolation_level(0)
            cursor = conn.cursor()
            cursor.execute("DROP DATABASE %s;" % server._database)
            cursor.close()
            conn.close()
        except Exception, e:
            ## Did not work, but don't change error exit code
            print "%s: Could not load simulated db" % server.__class__.__name__

    server._running = False


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

    VARS = ",".join(labels).lower()

    ## Add data into test dataabase
    INSERT_CMD = "INSERT INTO raf_lrt (" + VARS.rstrip(', ') + ") VALUES (%s);"
    for row in data:
        data_piece = ""
        for item in row:
            data_piece = "".join([data_piece, "'%s'," % str(item)])

        cursor.execute(INSERT_CMD % data_piece.rstrip(', '))

    ## Close all connections to the database, will be picked up later.
    cursor.close()
    conn.close()


def _loadVariables(file_path, dbname, host, user="postgres", password=""):
    """
    Loads a .asc file with a header into a sql database for testing.
    """
    nfile = datafile.NRTFile(file_path)

    labels = nfile.labels
    data = nfile.data

    ## Join the new database
    print "Database: %s" % dbname
    print "User: %s" % user
    print "Host: %s" % host
    print "Password: %s" % password
    conn = psycopg2.connect(database=dbname, \
                                                    user=user, \
                                                    host=host, \
                                                    password=password)

    cursor = conn.cursor()

    VARS = ",".join(labels).lower()
    ## Add data into test dataabase
    INSERT_CMD = "INSERT INTO raf_lrt (" + VARS.rstrip(', ') + ") VALUES (%s);"
    for row in data:
        data_piece = ""
        for item in row:
            data_piece = "".join([data_piece, "'%s'," % str(item)])
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
            ## of type NVarSet
            self._vars = variables

        current_time = server.getData(number_entries=1)
        self._last_update_time = current_time[0][0]

    def update(self):
        """
        Update attached variables with new data, and then sleep the server so
        it polls less frequently.
        """
        data = self.server.getData(start_time=self._last_update_time,
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
        self._bad_data_values = None
        self._conn = None
        self._running = True
        self._sql_bad_attempts = 0

        ## Time (which can be spoofed with the simulate parameters
        self._start_time = datetime.datetime.utcnow()
        self._current_time = (simulate_start_time
                              if simulate_start_time is not None
                              else self._start_time)

        ## Simulate variables
        self._simulate_start_db = None
        self._simulate_start_time = simulate_start_time
        self._simulate_fast = (simulate_fast
                               if simulate_start_time is not None
                               else False)

        self._flying = False

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
        cursor.execute("SELECT column_name FROM Information_Schema.Columns "
                       "WHERE table_name='raf_lrt'")
        variable_list = cursor.fetchall()
        try:
            variable_list.remove(('datetime',))
        except ValueError:
            pass

        self._bad_data_values = self.getBadDataValues()

        ## Variable list is a list of single entry tuples, make into tuple
        self.variable_list = tuple([col[0] for col in variable_list])

        ## Get flight information
        cursor.execute("SELECT * FROM global_attributes;")
        self._flight_info = dict(cursor.fetchall())

        ## We are done
        cursor.close()
        atexit.register(__ending__, self)

    def __del__(self):
        try:
            self._conn.close()
        except Exception, e:
            pass

    def stop(self):
        """
        Stop running the server. Use if in an infinite loop.
        """
        try:
            self._conn.close()
        except Exception, e:
            pass
        self._running = False

    def reconnect(self):
        """
        Sometimes the connection goes stale and will not recognize when
        the server has replaced its database with a fresh one. Hence this
        function should be called periodically to update when these database
        transitions occur.
        """
        try:
            self._conn = psycopg2.connect(database=self._database,
                                          user=self._user,
                                          host=self._host,
                                          password=self._password)
        except Exception, e:
            print e

    def flying(self):
        r"""
        Is the airplane going faster than 50 m/s right now? If the speed
        variable TASX is not available then the speed is calculated from the
        last two GPS coordinates GGLAT and GGLON using the Vincenty formula
        (which functions correctly for all possible values).

        The Vincenty Formula:
          d =
            \arctan\left(
              \frac{
                \sqrt{
                  \left( \cos\phi_f\sin\Delta\lambda \right)^2 +
                  \left( \cos\phi_s\sin\phi_f -
                         \sin\phi_s\cos\phi_f\cos\Delta\lambda \right)^2
                }
              }{
                \sin\phi_s\sin\phi_f +
                \cos\phi_s\cos\phi_f\cos\Delta\lambda
              }
            \right)

          where
          \phi_s and \lambda_s are latitude, longitude of first point
          \phi_f and \lambda_f are latitude, longitude of second point
        """
        speed = 0
        data = (self.getData(number_entries=1, variables=('tasx',)))
        if len(data) != 0:
            speed = data[0][1]
        else:
            return self._flying

        if speed == self._bad_data_values['TASX']:
            speed = self._gps_speed()

        if speed == self._bad_data_values['TASX']:
            return self._flying

        if speed > 50:
            if self._flying == False:
                cursor = self._conn.cursor()
                try:
                    cursor.execute("SELECT * FROM global_attributes;")
                    self._flight_info = dict(cursor.fetchall())
                except Exception:
                    print "Could not update flight information variable."
                self._flying = True
            return True
        else:
            self._flying = False
            return False

    def _gps_speed(self):
        data = (self.getData(number_entries=2, variables=('gglat', 'gglon')))

        if len(data) == 0:
            return self._bad_data_values['TASX']

        lat1 = math.radians(data[0][1])
        lat2 = math.radians(data[1][1])
        lon1 = math.radians(data[0][2])
        lon2 = math.radians(data[1][2])

        cos = math.cos
        sin = math.sin
        atan2 = math.atan2
        sqrt = math.sqrt

        ## Radius of the Earth
        R = 6371
        dLon = lon2 - lon1
        tm = (data[0][0] - data[1][0]).total_seconds()

        ## Vincenty Formula
        d = (atan2(sqrt((cos(lat2) * sin(dLon)) ** 2
             + (cos(lat1) * sin(lat2)
             - sin(lat1) * cos(lat2) * cos(dLon)) ** 2),
             sin(lat1) * sin(lat2) + cos(lat1) * cos(lat2) * cos(dLon)) * R)

        return d * 1000 / tm

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
        """ Returns the most recent datapoint time as a string """
        return str(self._getSimulatedCurrentTime())

    def getTime(self):
        """ Returns latest datapoint time as datetime object """
        return self._getSimulatedCurrentTime()

    def getFlightInformation(self):
        """
        Get the flight information. This is updated when a new flight is
        detected.
        """
        return self._flight_info

    def _getSimulatedCurrentTime(self):
        """
        Get the most recent time from the data. Will give a simulated time if
        in simulation mode.
        """
        if self._simulate_fast:
            return (((self._current_time - self._simulate_start_time)
                         + self._simulate_start_time).replace(microsecond=0))
        else:
            return (((datetime.datetime.utcnow() - self._start_time)
                         + self._current_time).replace(microsecond=0))

    def getData(self, variables=None,
                                        start_time=None, end_time=None,
                                        number_entries=None):
        """
        Get data from the server for the selected variables, where the
        variables are a tuple/list. Can be manipulated to get data from a
        range or just a certain number of entries. The times can also be
        intervals such as start_time = "-60 MINUTES".
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
        var_str = "datetime"
        if variables is not None:
            for var in variables:
                if var in self.variable_list or var is "datetime":
                    var_str = ", ".join([var_str, var])
                else:
                    print >> sys.stderr, (
                    "%s: Could not add variable %s, does not exist"
                    % (self.__class__.__name__, var))

        ## All Aeros displayable data from raf_lrt
        sql_command = "".join([sql_command, var_str, " FROM raf_lrt "])

        ## Open SQL connection
        cursor = self._conn.cursor()

        ## TODO: Clean time interval part of server.getData
        ## Start creating time interval string.
        time_interval = ""
        if (end_time is None and
            start_time is not None and
            number_entries is None):
            ## Assume -# INTERVAL syntax, SQL style.
            if start_time[0] == "-" or start_time[0] == "+":
                if self._simulate_start_time:
                    time_interval = ("WHERE (datetime > timestamp '%s' "
                                     "+ interval '%s') AND "
                                     "(datetime <= '%s')"
                                     % (NOW, start_time, NOW))
                else:
                    time_interval = ("WHERE datetime > %s %s interval '%s'"
                                     % (NOW, start_time[0], start_time[1:]))
            else:  # Assume explicit date given, SQL style.
                if self._simulate_start_time is not None:
                    time_interval = ("WHERE (datetime > '%s' "
                                     "AND datetime <= '%s')"
                                     % (start_time, NOW))
                else:
                    time_interval = "WHERE datetime > '%s'" % start_time
        elif (end_time is None and
              start_time is not None and
              number_entries is not None):
            if start_time[0] == "-" or start_time[0] == "+":
                if self._simulate_start_time:
                    time_interval = ("WHERE datetime > (timestamp '%s' "
                                     "+ interval '%s')"
                                     % (NOW, start_time))
                else:
                    time_interval = ("WHERE datetime > %s %s interval '%s'"
                                     % (NOW, start_time[0], start_time[1:]))
            else:  # Assume explicit date given, SQL style.
                if self._simulate_start_time is not None:
                    time_interval = ("WHERE (datetime > '%s' "
                                     "AND datetime <= '%s')"
                                     % (start_time, NOW))
                else:
                    time_interval = "WHERE datetime > '%s'" % start_time

            time_interval = " ".join([time_interval,
                                      ("ORDER BY datetime ASC LIMIT %s"
                                       % number_entries)]
                                    )
        elif (end_time is None and
              start_time is None and
              number_entries is not None):
            if self._simulate_start_time is not None:
                time_interval = ("WHERE datetime <= '%s' "
                                 "ORDER BY datetime DESC LIMIT %s"
                                 % (NOW, number_entries))
            else:
                time_interval = (" ORDER BY datetime DESC LIMIT %s"
                                 % number_entries)
        else:
            print >> sys.stderr, ("%s: Invalid time scale change"
                                  % self.__class__.__name__)
            return

        sql_command = "".join([sql_command, time_interval, ';'])
        data = []
        try:
            cursor.execute(sql_command)
            data = cursor.fetchall()
        except Exception, e:
            print >> sys.stderr, ("%s: SQL Command failed: %s"
                                  % (self.__class__.__name__, sql_command))
            self._sql_bad_attempts += 1
            if self._sql_bad_attempts % 10 == 0:
                print >> sys.stderr, ("Ten SQL commands failed, "
                                       "attempting to reconnect "
                                       "to the server.")
                self.reconnect()

        cursor.close()
        return data

    def getBadDataValues(self):
        cursor = self._conn.cursor()
        cursor.execute('select name, missing_value from variable_list ;')
        return dict(cursor.fetchall())

    def getDatabaseStructure(self):
        """
        Get the database structure, return as string. See documentation for
        the string formatting. This information goes into the header of
        outputted files.
        """
        cursor = self._conn.cursor()

        ## Get a list of Tables
        cursor.execute(("SELECT table_name FROM information_schema.tables "
                        "WHERE table_type = 'BASE TABLE' "
                        "AND table_schema NOT IN "
                            "('pg_catalog', 'information_schema');"))
        tables = cursor.fetchall()
        tables = tuple([col[0] for col in tables])

        ## Get a list of constrains on those tables, as a
        ## dict{tbl_name:constrain}
        cursor.execute(("SELECT t.table_name, k.column_name "
                        "FROM information_schema.table_constraints "
                        "T INNER JOIN information_schema.key_column_usage "
                        "K ON T.CONSTRAINT_NAME = k.constraint_name "
                        "WHERE T.CONSTRAINT_TYPE = 'PRIMARY KEY' -- "
                        "AND T.TABLE_NAME = 'table_name' "
                        "ORDER BY T.TABLE_NAME, K.ORDINAL_POSITION;"))
        constraints = dict(cursor.fetchall())

        ## Start building output string
        output = ""
        for table in tables:
            ## Start building string for a specific table.
            tbl_string = "%s=" % table

            ## Get the listing of columns and their data types.
            cursor.execute(("SELECT "
                            "column_name, data_type, is_nullable, "
                            "character_maximum_length, udt_name "
                            "FROM information_schema.columns "
                            "WHERE TABLE_NAME = '%s' "
                            "ORDER BY ordinal_position;" % table))
            columns = cursor.fetchall()

            ## Each column string is (COLUMNS, (col1, type, null?), etc)
            tbl_string = "".join([tbl_string, "('COLUMNS',"])
            for col in columns:
                ## Get column information from returned string.
                col_name = col[0]

                ## Unfortunately data_type from SQL returns ARRAY instead of
                ## integer[] or double precision[], and so these types must be
                ## reconstructed from their raw variable types.
                if col[4] == "_int4":
                    col_base_type = "integer[]"
                elif col[4] == "_float8":
                    col_base_type = "double precision[]"
                else:
                    col_base_type = col[1]

                ## Similarly data_type does not give the length of a char
                ## array. It must be reconstructed as character(#) from the
                ## SQL variable character_maximum_length. Does not affect not
                ## char values.
                col_type = ("%s(%s)" % (col_base_type, col[3])
                            if col[3] is not None
                            else "")

                ## Make some values forced to be something besides NULL
                col_null = 'NOT NULL' if col[2] == 'NO' else ''

                ## (name, compiled_type, am I null?)
                tbl_string = "".join([tbl_string,
                                      ("('%s','%s','%s'),"
                                       % (col_name, col_type, col_null))]
                                    )

            tbl_string = "".join([tbl_string.rstrip(', '), ')'])

            ## Add constraint if applicable
            if table in constraints:
                tbl_string = "".join([tbl_string,
                                      (";('CONSTRAINT', '%s')"
                                       % constraints[table])]
                                    )

            ## Data in table goes after the % character
            tbl_string = "".join([tbl_string, '%'])

            ## Ignore raf_lrt, that is where the Aeros data comes from.
            if table != "raf_lrt":
                cursor.execute("SELECT * from %s;" % table)
                data = cursor.fetchall()
                data = "" if data == [] else tuple(data)

                ## Data is just the string representation of a tuple, allows
                ## for data to be imported using the eval() function
                tbl_string = "".join([tbl_string,
                                      (str(data).
                                        replace("Uncorr'd Raw",
                                                "Uncorr''d Raw")
                                      )]
                                    )

            ## Add the table to the output string
            output = "".join([output, tbl_string, '\n'])

        ## Finally, we have all the tables, end query.
        cursor.close()
        return output.strip('\n')
