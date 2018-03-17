package edu.bu.metcs.HadoopEx;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class MapReduceTest {

    public static class Map extends Mapper<LongWritable, Text, LongWritable, IntWritable> {

        private final static Text medallion = new Text(); // 0
        private final static Text hack_license = new Text(); // 1


        private LongWritable hour_key = new LongWritable() ;

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String Columns[] = line.split(",") ;

            // 2013-01-01 00:19:00
            DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH) ;
            LocalDateTime pickup_date_formated = LocalDateTime.parse(Columns[2] , format ) ; // Hour of the Pickup Time

            Double trip_time_in_secs = 0d ;
            Double trip_distance     = 0d ;
            Double pickup_longitude  = 0d ;
            Double pickup_latitude   = 0d ;
            Double dropoff_longitude = 0d ;
            Double dropoff_latitude  = 0d ;

            try {
                trip_time_in_secs = Double.parseDouble(Columns[4]) ;
                trip_distance     = Double.parseDouble(Columns[5]) ;
                pickup_longitude  = Double.parseDouble(Columns[6]) ;
                pickup_latitude   = Double.parseDouble(Columns[7]) ;
                dropoff_longitude = Double.parseDouble(Columns[8]) ;
                dropoff_latitude  = Double.parseDouble(Columns[9]) ;
            }
            catch (Exception e ){
                System.out.println("Exception! Data not formatted correctly.");
            }

            hour_key = new LongWritable (  pickup_date_formated.getHour() ) ;

            if (trip_time_in_secs > 0 || trip_distance > 0 ) {
                if (pickup_longitude == 0 && pickup_latitude == 0 && dropoff_longitude == 0 && dropoff_latitude == 0) {
                   context.write(hour_key, new IntWritable(1));
                }
            }
        }
    }

    public static class Reduce extends Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {

        private static IntWritable one = new IntWritable(1);

        public void reduce( LongWritable key , Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0 ;

            for (IntWritable value : values) {
                sum += value.get() ;
            }
            one.set( sum ) ;
            context.write( key , one);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = new Job(conf, "taxiGPSErrors");

        job.setJarByClass(MapReduceTest.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // FileInputFormat.addInputPath(job, new Path("/home/biny/IdeaProjects/HadoopExAssign2/taxi-data-sorted-small.csv.bz2") );

        int r = (int) ( Math.random() * 10000 );

        // FileOutputFormat.setOutputPath(job, new Path("/home/biny/IdeaProjects/HadoopExAssign2/ouput/" + Integer.toString( r ) ));

        job.waitForCompletion(true);
    }
}