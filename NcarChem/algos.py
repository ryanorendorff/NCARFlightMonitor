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


## --------------------------------------------------------------------------
## Functions
## --------------------------------------------------------------------------



## --------------------------------------------------------------------------
## Classes
## --------------------------------------------------------------------------

class NAlgorithm(object):

  def __init__(self, processing_fn = None, *variables)
    pass


class NAlgorithm(object):

  def __init__(self, processing_fn = None, *data)
    self._data = data
    self._process = processing_fn


  def __str__(self):
    return str(self._data), str(processing_fn)

## --------------------------------------------------------------------------
## Start command line interface (main)
## --------------------------------------------------------------------------
