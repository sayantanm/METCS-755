#!/usr/bin/python
from __future__ import print_function
import math 
from dateutil.parser import parse

f = open ( "taxi-data-very-small.csv","r") 

def getCellID(lat,lon):
    return (str(round(float(lat), 2)) + "_"+str(round(float(lon), 2)))

# Monday - 0 
# Sunday - 6 
for l in f:
    c = l.split(',')
    print (c[8],',',c[9])
    print (getCellID(c[8],c[9]))
    print ("\n")
    d = parse(c[3])
    print (d.weekday())
