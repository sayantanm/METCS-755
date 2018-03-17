package edu.bu.metcs.HadoopEx;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Type;
import java.text.DecimalFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DecimalStyle;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Locale;
import java.util.PriorityQueue;

public class Task2 {

    public static class MapTaxiErrors extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String Columns[] = line.split(",") ;

            // 2013-01-01 00:19:00
            // DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH) ;
            // LocalDateTime pickup_date_formated = LocalDateTime.parse(Columns[2] , format ) ; // Hour of the Pickup Time

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

            //hour_key = new LongWritable (  pickup_date_formated.getHour() ) ;

            if (pickup_longitude == 0 && pickup_latitude == 0 && dropoff_longitude == 0 && dropoff_latitude == 0) {
                context.write(new Text( Columns[0]), new Text( "1_1" )); // Count_Error
            } else {
                context.write(new Text( Columns[0]), new Text ( "1_0" ));
            }

        }
    }

    public static class ReduceTaxiErrors extends Reducer<Text, Text, Text, Text >{

        private static Text one = new Text("1");

        public void reduce( Text key , Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Long taxi_count = 0L ;
            Long taxi_error = 0L ;
            Double error_rate = 0d ;

            for (Text value : values) {
                String [] count_error = value.toString().split("_") ;
                taxi_count += Long.parseLong( count_error [0]) ;
                taxi_error += Long.parseLong( count_error [1]) ;
                //sum += value.get() ;
            }
            error_rate = (taxi_error.doubleValue() / taxi_count.doubleValue()) * 100.00 ;

            DecimalFormat decimalFormat = new DecimalFormat("#####.##" ) ;
            // System.out.println( key + " Count " + taxi_count.toString() + " Errors: "
            //        + taxi_error.toString() + " Rate: " + decimalFormat.format(error_rate.doubleValue()) );
            one.set( decimalFormat.format(error_rate.doubleValue())) ;
            context.write( key , one);
        }
    }

    static Comparator <String []> comparator = new Comparator<String[]>() {
        @Override
        public int compare(String[] strings, String[] t1) {
            Double a = Double.parseDouble(strings[1]) ;
            Double b = Double.parseDouble(t1[1]) ;
            if ( a < b ) {
                return -1 ;
            }

            if ( a > b ) {
                return 1 ;
            }
            return 0 ;
        }
    } ;

    static PriorityQueue <String[]> queue = new PriorityQueue<String[]> ( 5 , comparator ) ;

    public static class MapTopFiveTaxi extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String Columns[] = line.split("\\s") ;

            if ( queue.size() == 5 ) {
                String[] qpeek = queue.peek();
                if ( Double.parseDouble(Columns[1]) > Double.parseDouble(qpeek[1])) {
                    queue.poll() ;
                    queue.add(Columns) ;
                }
            } else {
                queue.add( Columns ) ;
            }

            // System.out.println("Second Mapper: " + Columns[0] );
            context.write(new Text( Columns[0]) , new Text(Columns[1]));
        }
    }

    public static class ReduceTopFiveTaxi extends Reducer<Text, Text, Text, Text > {

        private static Text one = new Text("1");

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Empty Reducer.
        }

        protected void cleanup ( Context context ) throws IOException , InterruptedException {

            Iterator <String []> itr = queue.iterator() ;
            while ( itr.hasNext() ){
                String [] qd = itr.next() ;
                context.write( new Text( qd[0]) , new Text( qd[1]));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = new Job(conf, "taxiGPSErrors");

        job.setJarByClass(Task2.class);
        job.setReducerClass(Task2.ReduceTaxiErrors.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // job.setOutputKeyClass(Text.class);
        // job.setOutputValueClass(Text.class);

        job.setMapperClass(Task2.MapTaxiErrors.class);
        // job.setMapperClass(Task2.MapTaxiCount.class);


        // job.setInputFormatClass(TextInputFormat.class);
        // job.setOutputFormatClass(TextOutputFormat.class);

        // FileInputFormat.addInputPath(job, new Path(args[0]));
        // FileOutputFormat.setOutputPath(job, new Path(args[1]));

        int r = (int) ( Math.random() * 10000 );
        // Path inPath  = new  Path("/home/biny/IdeaProjects/HadoopExAssign2/taxi-data-sorted-small.csv.bz2") ;
        // Path outPath = new  Path("/home/biny/IdeaProjects/HadoopExAssign2/ouput/" + Integer.toString( r ) ) ;

        Path inPath  = new  Path(args[0]) ;
        Path outPath = new  Path(args[1]) ;

        // MultipleInputs.addInputPath ( job , inPath , TextInputFormat.class , MapTaxiErrors.class );
        // MultipleInputs.addInputPath ( job , inPath , TextInputFormat.class , MapTaxiCount.class  );
        FileInputFormat.addInputPath(job, inPath ) ;
        FileOutputFormat.setOutputPath(job, outPath);

        // FileInputFormat.addInputPath(job, new Path("/home/biny/IdeaProjects/HadoopExAssign2/taxi-data-sorted-small.csv.bz2") );
        // FileOutputFormat.setOutputPath(job, new Path("/home/biny/IdeaProjects/HadoopExAssign2/ouput/" + Integer.toString( r ) ));

        job.waitForCompletion(true);
        // System.out.println("---------------------> Task2");

        Job topFiveTaxi = new Job ( conf , "topFiveTaxi" ) ;
        topFiveTaxi.setJarByClass(Task2.class);
        FileInputFormat.addInputPath(topFiveTaxi,outPath);
        outPath = outPath.suffix( "/newout/") ;
        FileOutputFormat.setOutputPath(topFiveTaxi, outPath );
        topFiveTaxi.setMapOutputKeyClass(Text.class);
        topFiveTaxi.setMapOutputValueClass(Text.class);
        topFiveTaxi.setMapperClass(MapTopFiveTaxi.class);
        topFiveTaxi.setReducerClass(ReduceTopFiveTaxi.class);
        topFiveTaxi.waitForCompletion(true);
    }
}