package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CharacterFrequency {

    public static class CharacterRequencyMapper extends Mapper<Object, Text, Text, IntWritable> {
        private  final Text chtr= new Text();
        private final IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
          String line= value.toString().replaceAll("\\s+", "");
                for(char c : line.toCharArray()){
                    chtr.set(Character.toString(c));
                    context.write(chtr, one);
                }
        }
    }
    public static class CharacterFrequencyReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        private final IntWritable result= new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values,Context context ) throws IOException, InterruptedException {
            int sum=0;
            for (IntWritable val :values){
                sum+= val.get();
            }
            result.set(sum);
            context.write(key, result);

        }
    }

    public static void main(String args[]) throws Exception{
        Configuration conf= new Configuration();

        Job job= Job.getInstance(conf, "Character Frequency Count");
        job.setJarByClass(CharacterFrequency.class);

        job.setMapperClass(CharacterRequencyMapper.class);
        job.setReducerClass(CharacterFrequencyReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true)? 0 : 1);
    }
}
