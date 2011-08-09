#!/usr/bin/env python
# encoding: utf-8

## File Description
##
## Author: Ryan Orendorff <ryano@ucar.edu>
## Date: 21/07/11 10:31:47
##

## Syntax notes for coders who are not author
## - A double pound (##) is a comment, a single is commented code


## --------------------------------------------------------------------------
## Imports and Globals
## --------------------------------------------------------------------------
import re
import datetime
import sys


## --------------------------------------------------------------------------
## Functions
## --------------------------------------------------------------------------

def _SqlFromHeader(header):
  cmd_list = ()
  for cnt, line in enumerate(header.split('\n')):
    if len(line) > 0 and line[0] == "#":
      if len(line) > 1 and line[1] == "!":
        tbl = re.match("^#!\s*(\w+)\s*=\s*(.*)%(.*)$", line)
        if tbl:
          tbl_name = tbl.groups()[0]
          tbl_info = ()
          for info in tbl.groups()[1].split(';'):
            tbl_info += (eval(info), )
          if tbl.groups()[2] != "":
            tbl_data = eval(tbl.groups()[2])
          else:
            tbl_data = None
          tbl_cmds = [col[0] for col in tbl_info]
        else:
          print >>sys.stderr, "Table information line improperly formatted. Line %s" % cnt

        SQL_CMD = "CREATE TABLE %s (" % tbl_name

        VARS = ""
        if "COLUMNS" in tbl_cmds:
          for col in tbl_info[tbl_cmds.index('COLUMNS')][1:]:
            VARS += "%s %s %s, " % (col[0], col[1], col[2])

        COLUMNS_DATA_TYPE = [[col[0], col[1]] for col in  tbl_info[tbl_cmds.index('COLUMNS')][1:]]

        if "CONSTRAINT" in tbl_cmds:
          info = tbl_info[tbl_cmds.index('CONSTRAINT')][1:]
          VARS += "CONSTRAINT %s_pkey PRIMARY KEY (%s)" % (tbl_name,info[0])

        VARS = VARS.rstrip(', ')

        SQL_CMD += "%s);" % VARS
        cmd_list +=  (SQL_CMD,)
        if "RULE" in tbl_cmds:
          info = tbl_info[tbl_cmds.index("RULE")][1:]
          cmd_list += ("CREATE RULE %s AS ON %s TO %s DO %s;" % (info[0], info[1], tbl_name, info[2]),)

        if tbl_data != None:
          INSERT_CMD="INSERT INTO %s VALUES (" % tbl_name
          for values in tbl_data:
            DATA = ""
            for counter, value in enumerate(values):
              if COLUMNS_DATA_TYPE[counter][1] == "text":
                DATA += "'%s'," % value
              elif COLUMNS_DATA_TYPE[counter][1] == "double precision":
                DATA += "%s," % value
              elif COLUMNS_DATA_TYPE[counter][1] == "integer":
                DATA += "%s," % value
              elif COLUMNS_DATA_TYPE[counter][1] == "timestamp without time zone":
                DATA += "TIMESTAMP '%s'" % value
              elif "character" in COLUMNS_DATA_TYPE[counter][1]:
                DATA += "'%s'," % value
              elif "[]" in COLUMNS_DATA_TYPE[counter][1]:
                DATA += "'%s'," % str(value).replace('[', '{').replace(']', '}')
              else:
                print >>sys.stderr, "Problem creating insert statement for value %s, unknown type" % value
            DATA = DATA.rstrip(', ')
            cmd_list += (INSERT_CMD + DATA + ");", )

  return cmd_list

def _concatTime(labels, data):
  if labels[0] == "YEAR":
    labels = ['DATE'] + labels[3:]
    data = [ ["%s-%s-%s" % (col[0], col[1], col[2])] + col[3:] for col in data]
  if labels[1] == "HOUR":
    labels = ['DATETIME'] + labels[4:]
    data = [ [datetime.datetime.strptime("%s %s:%s:%s" % (col[0], col[1], col[2], col[3]), '%Y-%m-%d %H:%M:%S')] + col[4:] for col in data]
  elif labels[1] == "UTC":
    labels = ['DATETIME'] + labels[4:]
    data = [ datetime.datetime.strptime("%s %s" % (col[0], col[1]), '%Y-%d-%m %H:%M:%S') + col[2:] for col in data]
  return tuple(labels), data

def _parseIntoHeaderLabelsData(file_str):
  try:
    file_pieces = re.match(r"^(#.*?\n)?(?=[^#]*?\n)(.*?)\n(.*)$", file_str, re.S)
    header = file_pieces.groups()[0]
    labels = file_pieces.groups()[1].upper().split(',')
    data   = [row.split(',') for row in file_pieces.groups()[2].split('\n') if row != ""]

    return header, labels, data
  except Exception, e:
    print >>sys.stderr, "Could not parse file into header and data portions."
    print >>sys.stderr, e

## --------------------------------------------------------------------------
## Classes
## --------------------------------------------------------------------------


class NRTFile(object):
  def __init__(self, file_name=""):
    self.header = ""
    self.labels = ""
    self.variables = ""
    self.data = ""
    self.file_name = ""

    if file_name != "":
      try:
        file_str = open(file_name, "r").read()
      except:
        print >>sys.stderr, "Could not open file %s" % file_name
      header, labels, data = _parseIntoHeaderLabelsData(file_str)
      labels, data = _concatTime(labels, data)
      self.header = header
      self.labels = labels
      self.variables = labels[1:]
      self.data = data
      self.file_name = file_name

  def setHeader(self, sql_structure):
    for line in sql_structure.split('\n'):
      self.header += "#! %s\n" % line
    self.header = self.header.rstrip('\n')

  def setLabels(self, labels):
    self.labels = labels
    self.variables = labels[1:]

  def setData(self, data):
    self.data = data

  def getSql(self):
    return _SqlFromHeader(self.header)

  def write(self, file_name="", header=None, labels=None, data=None):
    if header != None:
      self.setHeader(header)
      header=self.header
    else:
      header = ""
    if labels != None:
      self.setLabels(labels)
      labels=self.labels
    if data != None:
      self.setData(data)
      data=self.data
    if file_name == "":
      file_name = self.file_name
    self.file_name = file_name

    label_str = 'YEAR,MONTH,DAY,HOUR,MINUTE,SECOND'
    for variable in labels[1:]:
      label_str += ',%s' % variable.upper()

    data_str = ""
    for row in data:
      line = ""
      for value in row:
        if isinstance(value, datetime.datetime):
          line += value.strftime("%Y,%m,%d,%H,%M,%S,")
        else:
          line += '%s,' % str(value)
      data_str += line.rstrip(', ') + '\n'

    try:
      f = open(file_name, 'w')
    except Exception, e:
      print >>sys.stderr, "Could not open file %s for writing." % file_name
      print e
      return

    try:
      f.write(header + '\n' + label_str + '\n' + data_str)
    except Exception, e:
      print >>sys.stderr, "Could not write to file %s" % file_name
      print e

    try:
      f.close()
    except:
      pass




## --------------------------------------------------------------------------
## Start command line interface (main)
## --------------------------------------------------------------------------

if __name__ == "__main__":
  nfile = NRTFile(sys.argv[1])

  #print "-----------------------------------------------------------------" + \
        #"\nSQL Commands\n" + \
        #"-----------------------------------------------------------------"
  #if nfile.header != None:
    #print nfile.getSql()
  print "-----------------------------------------------------------------" + \
        "\nLabels\n" + \
        "-----------------------------------------------------------------"
  print nfile.labels
  print "-----------------------------------------------------------------" + \
        "\nData\n" + \
        "-----------------------------------------------------------------"
  print nfile.data

