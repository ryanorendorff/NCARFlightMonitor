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

## Regular expressions to parse files.
import re

## Time
import datetime

## For sys.stderr
import sys


## --------------------------------------------------------------------------
## Functions
## --------------------------------------------------------------------------

def _SqlFromHeader(header):
  """
  Creates SQL commands from a header file.
  """
  ## Start of entire command list, including table creation and data entry.
  cmd_list = ()
  for cnt, line in enumerate(header.split('\n')):

    ## only get information that starts with "#!"
    if len(line) > 0 and line[0] == "#":
      if len(line) > 1 and line[1] == "!":
        ## Split into table name; columns, rules, and constraints; and data
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
          print >>sys.stderr, "Table information line " + \
                              "improperly formatted. Line %s" % cnt

        ## First command for a table
        SQL_CMD = "CREATE TABLE %s (" % tbl_name

        ## Create table SQL command var sets, including name, type, and null
        VARS = ""
        if "COLUMNS" in tbl_cmds:
          for col in tbl_info[tbl_cmds.index('COLUMNS')][1:]:
            VARS += "%s %s %s, " % (col[0], col[1], col[2])

        ## Get a list of the column datatypes for later use.
        COLUMNS_DATA_TYPE = [[col[0], col[1]] for col in
                              tbl_info[tbl_cmds.index('COLUMNS')][1:]]

        ## Add contrains to end of table creation command
        if "CONSTRAINT" in tbl_cmds:
          info = tbl_info[tbl_cmds.index('CONSTRAINT')][1:]
          VARS += "CONSTRAINT %s_pkey PRIMARY KEY (%s)" % (tbl_name,info[0])

        ## Finish table creation command, add to command list
        VARS = VARS.rstrip(', ')
        SQL_CMD += "%s);" % VARS

        cmd_list +=  (SQL_CMD,)

        ## Create rule SQL command
        if "RULE" in tbl_cmds:
          info = tbl_info[tbl_cmds.index("RULE")][1:]
          cmd_list += ("CREATE RULE %s AS ON %s TO %s DO %s;" % \
                        (info[0], info[1], tbl_name, info[2]),)

        ## Create table insertion commands
        if tbl_data is not None:
          INSERT_CMD="INSERT INTO %s VALUES (" % tbl_name

          ## Format values, all flanked with ''
          for values in tbl_data:
            DATA = ""
            for counter, value in enumerate(values):
              ## change input string based on column datatype
              if COLUMNS_DATA_TYPE[counter][1] == "text":
                DATA += "'%s'," % value
              elif COLUMNS_DATA_TYPE[counter][1] == "double precision":
                DATA += "%s," % value
              elif COLUMNS_DATA_TYPE[counter][1] == "integer":
                DATA += "%s," % value
              elif COLUMNS_DATA_TYPE[counter][1] == "timestamp " + \
                                                    "without time zone":
                DATA += "TIMESTAMP '%s'," % value
              elif "character" in COLUMNS_DATA_TYPE[counter][1]:
                DATA += "'%s'," % value
              elif "[]" in COLUMNS_DATA_TYPE[counter][1]:
                ## Arrays are surrounded by {}
                DATA += "'%s'," % str(value).replace('[', '{').replace(']', '}')
              else:
                print >>sys.stderr, "Problem creating insert statement " +\
                                    "for value %s, unknown type" % value

            DATA = DATA.rstrip(', ')
            cmd_list += (INSERT_CMD + DATA + ");", )

  ## Finally done creating insert commands
  return cmd_list

def _concatTime(labels, data):
  """
  Take Year,Month,...,Second columns and combine them into a datetime type
  string.
  """
  ## Compact date columns
  if labels[0] == "YEAR":
    labels = ['DATE'] + labels[3:]
    data = [ ["%s-%s-%s" % (col[0], col[1], col[2])] + col[3:] for col in data]

  ## Compact time columns
  if labels[1] == "HOUR":
    labels = ['DATETIME'] + labels[4:]
    data = [ [datetime.datetime.strptime("%s %s:%s:%s" % (col[0], col[1], col[2], col[3]), '%Y-%m-%d %H:%M:%S')] + col[4:] for col in data]
  elif labels[1] == "UTC":
    labels = ['DATETIME'] + labels[4:]
    data = [ datetime.datetime.strptime("%s %s" % (col[0], col[1]), '%Y-%d-%m %H:%M:%S') + col[2:] for col in data]

  return tuple(labels), data  ## data is an list, not tuple

def _parseIntoHeaderLabelsData(file_str):
  try:
    ## Get pieces based on structure
    ## name=tbl_information1;tbl_information2%tbl_data
    file_pieces = re.match(r"^(#.*?\n)?(?=[^#]*?\n)(.*?)\n(.*)$",
                           file_str, re.S)
    header = file_pieces.groups()[0]
    labels = file_pieces.groups()[1].upper().split(',')
    data   = [row.split(',') for row in file_pieces.groups()[2].rstrip('\n\r').split('\n') if row != ""]

    return header, labels, data
  except Exception, e:
    print >>sys.stderr, "Could not parse file into header and data portions."
    print >>sys.stderr, e

## --------------------------------------------------------------------------
## Classes
## --------------------------------------------------------------------------


class NRTFile(object):
  """
  A class to deal with .asc files. Files that exist can be loaded into this
  class and they can be written out using this class.
  """
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
    """
    Set SQL header structure using SQL database structure string, see
    NDatabase.getDatabaseStructure.
    """
    for line in sql_structure.split('\n'):
      self.header += "#! %s\n" % line
    self.header = self.header.rstrip('\n')

  def setLabels(self, labels):
    """
    Set variable list. Do not include datetime unless it was specifically
    added. This is a list/tuple.
    """
    self.labels = labels
    self.variables = labels[1:]

  def setData(self, data):
    """
    Add in data matrix.
    """
    self.data = data

  def getSql(self):
    """
    Return the header information parsed as SQL command list.
    """
    return _SqlFromHeader(self.header)

  def write(self, file_name="", header=None, labels=None, data=None):
    """
    Write file to destination, with any combination of header, label, and
    data information.
    """
    ## Replace information if provided to function
    if header is not None:
      self.setHeader(header)
      header=self.header + "\n"
    else:
      header = ""
    if labels is not None:
      self.setLabels(labels)
      labels=self.labels
    if data is not None:
      self.setData(data)
      data=self.data
    if file_name == "":
      file_name = self.file_name
    self.file_name = file_name

    ## Files always start with the time in the label
    label_str = 'YEAR,MONTH,DAY,HOUR,MINUTE,SECOND'

    ## Add in the rest of the variables to the labels list
    for variable in labels[1:]:
      label_str += ',%s' % variable.upper()

    label_str += "\n"

    ## Start outputting data
    data_str = ""
    for row in data:
      line = ""
      for value in row:
        if isinstance(value, datetime.datetime):
          line += value.strftime("%Y,%m,%d,%H,%M,%S,")
        else:
          line += '%s,' % str(value)
      data_str += line.rstrip(', ') + '\n'

    ## Try really hard to write the file.
    try:
      f = open(file_name, 'w')
    except IOError, e:
      print >>sys.stderr, "Could not open file %s for writing." % file_name
      return

    try:
      f.write("%s%s%s" % (header, label_str, data_str))
    except IOError, e:
      print >>sys.stderr, "Could not write to file %s" % file_name

    ## Only needed if open did not work.
    try:
      f.close()
    except:
      pass




## --------------------------------------------------------------------------
## Start command line interface (main)
## --------------------------------------------------------------------------

if __name__ == "__main__":
  """
  Test of NRTFile, where the first command line parameter is the input file.
  """
  nfile = NRTFile(sys.argv[1])

  print "-----------------------------------------------------------------" + \
        "\nSQL Commands\n" + \
        "-----------------------------------------------------------------"
  if nfile.header is not None:
    print nfile.getSql()
  print "-----------------------------------------------------------------" + \
        "\nLabels\n" + \
        "-----------------------------------------------------------------"
  print nfile.labels
  print "-----------------------------------------------------------------" + \
        "\nData\n" + \
        "-----------------------------------------------------------------"
  print nfile.data

