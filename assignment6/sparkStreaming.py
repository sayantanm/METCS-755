
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
import sys
import requests
from operator import add

def aggregate_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)


# create spark configuration
conf = SparkConf()
conf.setAppName("TwitterStreamApplication")

# create spark context with the above configuration
sc = SparkContext(conf=conf)

# sc.setLogLevel("INFO")
# sc.setLogLevel("ERROR")
sc.setLogLevel("FATAL")


# create the Streaming Context from the above spark context with interval size 2 seconds
ssc = StreamingContext(sc, 2)


# setting a checkpoint to allow RDD recovery
ssc.checkpoint("checkpoint_TwitterApp")

# read data from port 9009
dataStream = ssc.socketTextStream("localhost", 9009)

# split each tweet into words
words = dataStream.window(300, 20 ).flatMap(lambda line: line.split("||"))

hashtags = words.filter(lambda w: ('user_name_fc:' in w))\
                .flatMap( lambda u: u.split ( "user_name_fc:" ))\
                .filter ( lambda u: str(u) != '' ) \
                .map ( lambda x: x.split ( "<===>" ) )\
                .filter ( lambda x: x[1] != '' )\
                .map ( lambda x: ( x[0] , int(x[1]) ) )\
                .reduceByKey(add)

# adding the count of each hashtag to its last count
tags_totals = hashtags.updateStateByKey(aggregate_count)\
                     .transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))

# Debuging code 
tags_totals.pprint(20)

# start the streaming computation
ssc.start()
# wait for the streaming to finish
ssc.awaitTermination( timeout=72000 )


