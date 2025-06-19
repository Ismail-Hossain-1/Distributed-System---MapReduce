package org.example.MRJ;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCooccurrence {

    public static class CooccurrenceMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final IntWritable ONE = new IntWritable(1);
        private Text wordPair = new Text();
        private static final int WINDOW_SIZE = 2;

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().toLowerCase().replaceAll("[^a-zA-Z\\s]", " ");
            String[] words = line.split("\\s+");

            for (int i = 0; i < words.length; i++) {
                String word = words[i];
                if (word.isEmpty()) continue;

                int start = Math.max(0, i - WINDOW_SIZE);
                int end = Math.min(words.length - 1, i + WINDOW_SIZE);

                for (int j = start; j <= end; j++) {
                    if (j == i || words[j].isEmpty()) continue;
                    wordPair.set(word + "," + words[j]);
                    context.write(wordPair, ONE);
                }
            }
        }
    }

    public static class CooccurrenceReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values)
                sum += val.get();
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Word Co-occurrence");

        job.setJarByClass(WordCooccurrence.class);
        job.setMapperClass(CooccurrenceMapper.class);
        job.setReducerClass(CooccurrenceReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));   // Input path
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // Output path

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
