#!/usr/bin/env python
# encoding: utf-8

## Process fO3 files for data type or to replace blank cells.
##
## Author: Ryan Orendorff <ryano@ucar.edu>
## Date: 15/07/2011
##

## Syntax notes for coders who are not author
## - A double pound (##) is a comment, a single is commented code


## --------------------------------------------------------------------------
## Imports and Globals
## --------------------------------------------------------------------------
import re   ## Regular expression engine


## --------------------------------------------------------------------------
## Functions
## --------------------------------------------------------------------------

def __printHelp(error="", exit_code = 0):
  """Prints usage and error if applicable"""

  status = 0
  if error != "":
    print sys.argv[0] + ": error: " + error

  print """\
usage: parse_o3.py [--help] [--types|--fill|--fill-with=RPL] [--write=FILE]
                   O3_DATA_FILE

Required arguments (if more than one chosen, output is a concat of options):
    -t, --types:    Prints the data type of each variable in the file to
                      stdout.
     -f, --fill:    Replaces blank spaces with "-9999" to stdout.
                      This can produce a large amount of output. It is up to
                      the user to redirect the output of this command as
                      desired.
--fill-with RPL:    Same as --fill, except changes the blank cells to RPL.

Optional arguments:
      -h,--help:    Display help.
   --write FILE:    Write to FILE instead of to stdout.\
"""

  exit(exit_code)
## END __printHelp()

## --------------------------------------------------------------------------
## Classes
## --------------------------------------------------------------------------

class ParseO3:
  """Parse an f03 file"""

  def __init__(self, file_path):
    self.__file = self.__loadDataFile(file_path)
    self.__types = ""


  def __str__(self):
    return self.__file


  def __convertTo24Hour(self, matchobj):
    """
    Used to convert the date and time string into columns, see
    replaceBlankCells.
    """
    hour = 0
    if matchobj.group(7) == "AM":
      hour = int(matchobj.group(4))
    else:
      hour = int(matchobj.group(4)) + 12

    ## Year\tMonth\tDay\tHour\tMin\tSec
    return matchobj.group(3) + "\t"\
         + matchobj.group(1) + "\t"\
         + matchobj.group(2) + "\t"\
         + str(hour) + "\t"\
         + matchobj.group(5) + "\t"\
         + matchobj.group(6)
  ## END convertTo24Hour


  def __loadDataFile(self, file_path):
    """Load file_path into class"""

    try: ## Hopefully o3_raw_file is available
      o3_raw_file = open(file_path, 'r').read()
    except:
      __printHelp("File " + args[0] + " does not exist.", 3)
    return re.sub("[\r]", "", o3_raw_file)
  ## END __loadDataFile


  def determineTypes(self):
    """Determine the types in the f03 file"""
    if self.__types != "":
      return self.__types

    head, o3_data = self.__file.split("\n",1)

    ## Empty dictionary for variable name, data type pair.
    variables = {}

    ## Use to both create the dictionary and then to sort it. Read-only
    var_names = tuple(head.split('\t'))

    number_of_vars = 0
    for var in var_names:
      variables[var] = ""
      number_of_vars += 1

    ## Start scanning the file
    for line in tuple(o3_data.split('\n')):
      column = 0
      skip_count = 0

      for value in line.split('\t'):
        ## If all of the variable is filled don't change it.
        if variables[var_names[column]] != "" and skip_count == column:
          skip_count += 1
          column += 1

          ## If all the variables are typed stop looking through file.
          if skip_count == number_of_vars:
            break
          else:
            continue


        ## Determine what data type a value is. Ignores any white space
        ## placed before the value.

        ## Only digits
        if re.match("\s*[+-]*\d+$", value):
          variables[var_names[column]] = "integer"
        ## Only digits and one period
        elif re.match("\s*[+-]*\d+\.\d+$", value):
          variables[var_names[column]] = "float"
        ## mm/dd/yyyy
        elif re.match("\s*\d{1,2}/\d{1,2}/\d{4}", value):
          variables[var_names[column]] = "gregorian date"
        ## HH:MM:SS AM/PM
        elif re.match("\s*\d{1,2}:\d{1,2}:\d{1,2} [AP]M", value):
          variables[var_names[column]] = "time"
        ## Explicitly NaN
        elif value == "NaN":
          variables[var_names[column]] = "NaN"
        ## Includes something besides digits
        elif re.match("\s*[\w/:\s]+$", value):
          variables[var_names[column]] = "string"
        ## For debug
        else:
          variables[var_names[column]] = value

        column += 1
      ## END for

      ## Outer break for if all variables are typed.
      if skip_count == number_of_vars:
        break
    ## END for

    ## Replace all blank keys with "empty"
    for key, item in variables.iteritems():
      if item == "" or item == " ":
        variables[key] = "empty"

    o3_types = ""
    ## Print in order for convenience of user,
    ## otherwise dictionary is unsorted.
    for item in var_names:
      ## Alteration due to --fill expansion of date/time columns
      if item == "Date":
        o3_types += "Year: integer\nMonth: integer\nDay: integer\n"
        continue
      elif item == "Time":
        o3_types += "Hour: integer\nMin: integer\nSec: integer\n"
        continue

      o3_types += item + ": " + variables[item] + "\n"

    ## Must use str() to concatenate string + int
    o3_types += "Number of variables: " + str(number_of_vars)
    self.__types = o3_types
    return o3_types
  ## END determineTypes()


  def replaceBlankCells(self, blank_replacement="-9999"):
    """ Replaces all blank cells (\t\t), by default with -9999"""

    o3_filled = re.sub("^Date\tTime", "Year\tMonth\tDay\tHour\tMin\tSec",\
      self.__file)
    o3_filled = re.sub('(\d{1,2})/(\d{1,2})/(\d{4})\t(\d{1,2}):(\d{1,2}):'\
      + '(\d{1,2}) ([AP]M)', self.__convertTo24Hour, o3_filled)
    o3_filled = re.sub("\t(?=\t)|\t(?=\n)", "\t"+blank_replacement, o3_filled)
    return o3_filled.rstrip("\n") ## Remove newline at end of table

  ## END replaceBlankCells()


## --------------------------------------------------------------------------
## Start command line interface (main)
## --------------------------------------------------------------------------
if __name__ == "__main__":

  import getopt, sys  ## Used to get command line arguments
  from NcarChem import variable

  try:
    opts, args = getopt.getopt(sys.argv[1:], "htf", ["help", "types", \
                "fill", "fill-with=", "write="])
  except getopt.GetoptError, err:
    __printHelp(str(err), 1)

  dict_opts = dict(opts)

  if ("--help" in dict_opts or "-h" in dict_opts):
    __printHelp()

  if ("--write" in dict_opts):
    output = open(dict_opts["--write"], 'w')
  else:
    output = sys.stdout


  if len(args) == 0:
    __printHelp("No file specified", 2)

  ## Start parsing the file.
  fO3 = ParseO3(args[0])

  for option, value in opts:
    if option in ("-t", "--types"):
      print >>output, fO3.determineTypes()
    elif option in ("-f", "--fill"):
      print >>output, fO3.replaceBlankCells()
    elif option in "--fill-with":
      print >>output, fO3.replaceBlankCells(value)
    elif option in "--write":
      pass
    else:
      __printHelp("Unhandled option " + option, 4)

  if output != sys.stdout:
    output.close()
