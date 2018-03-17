package edu.bu.metcs.HadoopEx;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

public class Task3 {

    // Mapper Class that finds profitable taxis
    public static class MapTaxiProfit extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String Columns[] = line.split(",") ;

            // 2013-01-01 00:19:00
            // DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH) ;
            // LocalDateTime pickup_date_formated = LocalDateTime.parse(Columns[2] , format ) ;
            // Hour of the Pickup Time

            Double trip_time_in_secs = 0d ;
            Double trip_distance     = 0d ;
            Double pickup_longitude  = 0d ;
            Double pickup_latitude   = 0d ;
            Double dropoff_longitude = 0d ;
            Double dropoff_latitude  = 0d ;
            Double total_amount      = 0d ;

            try {
                trip_time_in_secs = Double.parseDouble(Columns[4]) ;
                trip_distance     = Double.parseDouble(Columns[5]) ;
                pickup_longitude  = Double.parseDouble(Columns[6]) ;
                pickup_latitude   = Double.parseDouble(Columns[7]) ;
                dropoff_longitude = Double.parseDouble(Columns[8]) ;
                dropoff_latitude  = Double.parseDouble(Columns[9]) ;
                total_amount      = Double.parseDouble(Columns[16]) ;
            }
            catch (Exception e ){
                System.out.println("Exception! Data not formatted correctly.");
            }

            //hour_key = new LongWritable (  pickup_date_formated.getHour() ) ;

            context.write( new Text( Columns[1]),
                    new Text( trip_time_in_secs.toString() + "_" + total_amount.toString()));
        }
    }

    // Reducer class that finds profitable taxis
    public static class ReduceTaxiProfit extends Reducer<Text, Text, Text, Text > {

        private static Text one = new Text("1");

        public void reduce( Text key , Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Double trip_duration  = 0d ;
            Double total_amount   = 0d ;
            Double profit_per_min = 0d ;

            for (Text value : values) {
                String [] columns = value.toString().split("_") ;
                // System.out.println( "Reducer: " + value.toString() );
                trip_duration += Double.parseDouble( columns [0]) ;
                total_amount += Double.parseDouble( columns [1]) ;
            }
            if ( trip_duration.doubleValue() > 0 ) {
                profit_per_min = total_amount.doubleValue() / ( trip_duration.doubleValue() / 60.00 ) ;
            } else {
                profit_per_min = 0.0 ;
            }

            DecimalFormat decimalFormat = new DecimalFormat("#####.##" ) ;
            // System.out.println( "Profit Per Min: " + profit_per_min.doubleValue() + " decFormat "
            // + decimalFormat.format(profit_per_min.doubleValue()));

            // System.out.println( key + " Count " + taxi_count.toString() + " Errors: "
            //        + taxi_error.toString() + " Rate: " + decimalFormat.format(error_rate.doubleValue()) );
            one.set( decimalFormat.format(profit_per_min.doubleValue())) ;
            context.write( key , one);
        }
    }

    // Comparator for priority queue
    static Comparator<String []> comparator = new Comparator<String[]>() {
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

    // Priority queue holding ten elements
    static PriorityQueue<String[]> queue = new PriorityQueue<String[]> ( 10 , comparator ) ;

    // Mapper class that finds top ten profitable taxis
    public static class MapTopTenTaxi extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String Columns[] = line.split("\\s") ;

            if ( queue.size() == 10 ) {
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

    // Reducer class that does no reducing but does cleanup
    public static class ReduceTopTenTaxi extends Reducer<Text, Text, Text, Text > {

        private static Text one = new Text("1");

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Empty Reducer.
            // The reducer is not doing anything -- by design.
            //
        }

        // Final top ten taxis
        protected void cleanup ( Context context ) throws IOException , InterruptedException {

            Iterator<String []> itr = queue.iterator() ;
            while ( itr.hasNext() ){
                String [] qd = itr.next() ;
                context.write( new Text( qd[0]) , new Text( qd[1]));
            }
        }
    }

    // The main function that does all the work
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = new Job(conf, "TaxiProfit");

        job.setJarByClass(Task3.class);
        job.setReducerClass(Task3.ReduceTaxiProfit.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // job.setOutputKeyClass(Text.class);
        // job.setOutputValueClass(Text.class);

        job.setMapperClass(Task3.MapTaxiProfit.class);
        // job.setMapperClass(Task2.MapTaxiCount.class);

        int r = (int) ( Math.random() * 10000 );
        // Path inPath  = new  Path("/home/biny/IdeaProjects/HadoopExAssign2/taxi-data-sorted-small.csv.bz2") ;
        // Path outPath = new  Path("/home/biny/IdeaProjects/HadoopExAssign2/ouput/" + Integer.toString( r ) ) ;

        Path inPath  = new  Path(args[0]) ;
        Path outPath = new  Path(args[1]) ;

        // MultipleInputs.addInputPath ( job , inPath , TextInputFormat.class , MapTaxiErrors.class );
        // MultipleInputs.addInputPath ( job , inPath , TextInputFormat.class , MapTaxiCount.class  );
        FileInputFormat.addInputPath(job, inPath ) ;
        FileOutputFormat.setOutputPath(job, outPath);

        // FileInputFormat.addInputPath(job,
        // new Path("/home/biny/IdeaProjects/HadoopExAssign2/taxi-data-sorted-small.csv.bz2") );

        // FileOutputFormat.setOutputPath(job,
        // new Path("/home/biny/IdeaProjects/HadoopExAssign2/ouput/" + Integer.toString( r ) ));

        job.waitForCompletion(true);

        // System.out.println("---------------------> Task3");
        Job topTenTaxi = new Job ( conf , "topTenTaxi" ) ;
        topTenTaxi.setJarByClass(Task3.class);
        FileInputFormat.addInputPath(topTenTaxi,outPath);
        outPath = outPath.suffix( "/newout/") ;
        FileOutputFormat.setOutputPath(topTenTaxi, outPath );
        topTenTaxi.setMapOutputKeyClass(Text.class);
        topTenTaxi.setMapOutputValueClass(Text.class);
        topTenTaxi.setMapperClass(Task3.MapTopTenTaxi.class);
        topTenTaxi.setReducerClass(Task3.ReduceTopTenTaxi.class);
        topTenTaxi.waitForCompletion(true);
    }
}
