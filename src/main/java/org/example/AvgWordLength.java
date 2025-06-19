package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;


public class AvgWordLength {

    public static class AvgWordLenghtMapper extends Mapper<Object, Text, Text, IntWritable>{
         private final static Text Length_key= new Text("length");
         private final static Text Count_key= new Text("count");
         private IntWritable one= new IntWritable(1);
         private Text word= new Text();
         public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
             StringTokenizer line= new StringTokenizer(value.toString());
             while (line.hasMoreTokens()){
                 String wrd= line.nextToken().replaceAll("[^a-zA-Z]", "");
                 if(!wrd.isEmpty()){
                     context.write(Length_key, new IntWritable(wrd.length()));
                     context.write(Count_key, one);

                 }
             }
         }

    }
    public static class AvgWordLengthReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        private int totalWords=0;
        private int totalLengeth=0;
        public void reduce(Text key, Iterable<IntWritable> array, Context context) throws IOException, InterruptedException{
            int sum=0;
            for(IntWritable val: array){
                sum+=val.get();
            }
            if(key.toString().equals("count")){
                totalWords=sum;
            } else if(key.toString().equals("length")){
                totalLengeth=sum;
            }

        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (totalLengeth > 0) {
                int avg = totalLengeth / totalWords;
                context.write(new Text("Average Word Length"), new IntWritable(avg));
            }
        }
    }
    public static void main(String args[]) throws Exception{
        Configuration conf= new Configuration();
        // Create configuration and job
        Job job = Job.getInstance(conf, "Average Wrod Length");

        //Set the main class
        job.setJarByClass(AvgWordLength.class);


        //Set Mapper and Reducer class
        job.setMapperClass(AvgWordLenghtMapper.class);
        job.setReducerClass(AvgWordLengthReducer.class);

        // (Optional) Set Combiner class if any
        //    job.setCombinerClass(AvgWordLengthReducer.class);

        // Set Mapper output keys and value types
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Set Reducer (final output) key and value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
        //boolean success = job.waitForCompletion(true);


    }
}
