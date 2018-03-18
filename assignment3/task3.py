#!/usr/bin/python

# Description: Task 3 of Assignment 3 of MET CS 755 
# Author: Sayantan Mukherjee 
# Usage : Task3 <input> <gpsinput> <output>

from __future__ import print_function

from operator import add
import sys
import math 
from dateutil.parser import parse

from pyspark import * 
from pyspark.sql import * 
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType 

## Referenced from SPARK-Examples-CS755 
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: Task3 <inputfile> <gpsinput> <output> ", file=sys.stderr)
        exit(-1)

    # This configuration is needed to run locally. 
    # Remember to comment them off when running on AWS EMR
    conf = SparkConf().setAppName("Task3")
    conf = (conf.setMaster('local[*]')
	    .set('spark.executor.memory', '4G')
	    .set('spark.driver.memory', '4G')
	    .set('spark.driver.maxResultSize', '5G'))
    sc = SparkContext(conf=conf)
    # sc = SparkContext ( appName="Task2" )
    sqlContext = SQLContext(sc)
    sc_lines = sc.textFile(sys.argv[1],1) 
    columns  = sc_lines.map(lambda x: x.split(','))


    gps_lines   = sc.textFile(sys.argv[2],1)
    gps_columns = gps_lines.map(lambda x: x.split('||')) 

    # The following functions are referenced from SPARK-Examples-CS755 by Prof Kia Teymourian
    # isFloat()
    # isInt() 
    # correctRows()
    # getCellID()
    # Exception Handling  and removing wrong data lines 
    def isFloat(value):
        try:
            float(value)
            return True
        except:
            return False

    def isInt(value):
        try:
            int(value)
            return True
        except:
            return False

    def correctRows(l):
        if ( len(l) == 17 ):
            if ( isFloat(l[8]) and isFloat(l[9]) ):
                return l 

    def getCellID ( lat , lon ) :
        return (str(round(float(lat), 3)) + "_" + str(round(float(lon), 3)))

    # Filter out bad data
    filteredData = columns.filter(correctRows).filter(lambda p: float(p[8])!=0.0 ).filter(lambda p: float(p[9])!=0.0) 

    # Create dataframe from filtered data 
    df = sqlContext.createDataFrame ( filteredData , ['medallion','hack_license','pickup_datetime','dropoff_datetime',
                                                     'trip_time_in_secs','trip_distance','pickup_longitude','pickup_latitude',
                                                     'dropoff_longitude','dropoff_latitude','payment_type','fare_amount','surcharge',
                                                     'mta_tax','tip_amount','tolls_amount','total_amount'] ) 
    # Create a table from data frame
    sqlContext.registerDataFrameAsTable ( df, "taxi_table" )

    # Create a dataframe from of GPS data
    # Create a table from the dataframe. 
    # 28.49196642190459||-81.50452231100718||Chain of Lakes Middle School||school____________________________________public___
    gps_df = sqlContext.createDataFrame ( gps_columns , [ 'lat' , 'long' , 'poi' , 'category' ] ) 
    sqlContext.registerDataFrameAsTable ( gps_df , 'gps_table' ) 

    # Use SQL to find the information we want. 
    # In here we're getting hourly count of dropoffs 
    HighDropDF = sqlContext.sql("select distinct concat(round(dropoff_latitude,3),'_',round(dropoff_longitude,3)) as grid_cell,\
            date_format(dropoff_datetime,'HH') as hour,date_format(dropoff_datetime,'dd-MM-y') as dropoff_date,\
            count(dropoff_datetime) as number_of, collect_list(distinct poi) as list_of_poi \
            from taxi_table join gps_table \
            on concat(round(dropoff_latitude,3),'_',round(dropoff_longitude,3)) = concat(round(lat,3),'_',round(long,3))\
            group by grid_cell,hour,dropoff_date order by dropoff_date desc")

    # rdd = HighDropDF.rdd 
    # rdd = rdd.map ( ( lambda p: "%s , %s , %s , %s , %s" % ( p[0],p[1],p[2],p[3],p[4] )) )
    # rdd.saveAsTextFile ( sys.argv[3] ) 


    sqlContext.registerDataFrameAsTable ( HighDropDF , "highdrop_table" ) 

    # In this SQL we're getting the daily hourly average
    DailyAveDF = sqlContext.sql ( "select distinct dropoff_date , hour, avg(number_of) as daily_average \
                                        from highdrop_table\
                                        group by dropoff_date,hour") 

    sqlContext.registerDataFrameAsTable ( DailyAveDF , "dailyave_table" ) 

    HighDropRateDF = sqlContext.sql ( "select a.*,b.*,a.number_of/b.daily_average as ratio \
                                    from highdrop_table a join dailyave_table b on a.hour = b.hour \
                                    and a.dropoff_date = b.dropoff_date\
                                    order by ratio desc limit 20")

    HighDropRateRDD = HighDropRateDF.rdd 
    HighDropRateRDD = HighDropRateRDD.map ( lambda p: "%s , %s , %s , %s , %s , %s" % ( p[0],p[1],p[2],p[8],p[3],p[4] ))
    HighDropRateRDD.saveAsTextFile ( sys.argv[3] ) 

    sc.stop() 
