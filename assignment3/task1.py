#!/usr/bin/python

# Description: Task 1 of Assignment 3 of MET CS 755 
# Author: Sayantan Mukherjee 
# Usage : Task1 <input> <output>

from __future__ import print_function

from operator import add
import sys

from pyspark import * 
from pyspark.sql import * 

## Referenced from SPARK-Examples-CS755 
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: Task1 <inputfile> <output> ", file=sys.stderr)
        exit(-1)

    sc = SparkContext ( appName="Task1" )
    sc_lines = sc.textFile(sys.argv[1],1) 
    columns  = sc_lines.map(lambda x: x.split(','))

    def correctRows(l):
        if ( len(l) == 17 ):
            return l 

    # 
    # The only purpose of this function is to return 1, regardless of the arguments used. 
    # We will be using this function when we reduce
    # 
    def one( a , b ):
        return 1 

    correctCols = columns.filter(correctRows)
    # first create a key of medallion and hack_license
    # map each occurance of medallion + hack_licence with value of 1
    # Then reduce by one(), which basically returns 1 and only has a unque set of medallion + hack_license. This gives us number of drivers for each medallion
    # Again reduce by medallion (i.e. split the composite key and only take the first portion of the key) and reduce by adding
    # Sort by value, descending and then take top(10)
    counts = correctCols.map(lambda p: ( p[0] + '_' + p[1] , 1 )).reduceByKey(one).map( lambda p: ( p[0].split('_')[0],1)).reduceByKey(add).sortBy(lambda p: p[1],ascending=False) 

    # Outputs to standard console
    driverCountList = counts.take(10) 
    for key,val in driverCountList:
        print ( key ,",", val )

    sc.stop() 
