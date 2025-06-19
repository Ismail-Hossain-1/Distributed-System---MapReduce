package org.example.MRJ;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.HashSet;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class LargestShortestWord {

    public static class WordLengthMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final static Text longestKey = new Text("longest");
        private final static Text shortestKey = new Text("shortest");

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split("\\s+");

            for (String word : words) {
                word = word.replaceAll("[^a-zA-Z]", "").toLowerCase(); // Clean the word
                if (!word.isEmpty()) {
                    context.write(longestKey, new Text(word));
                    context.write(shortestKey, new Text(word));
                }
            }
        }
    }


    public static class WordLengthReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int targetLength = (key.toString().equals("longest")) ? 0 : Integer.MAX_VALUE;
            HashSet<String> resultWords = new HashSet<>();

            for (Text value : values) {
                String word = value.toString();
                int length = word.length();

                if (key.toString().equals("longest")) {
                    if (length > targetLength) {
                        resultWords.clear();
                        resultWords.add(word);
                        targetLength = length;
                    } else if (length == targetLength) {
                        resultWords.add(word);
                    }
                } else {
                    if (length < targetLength) {
                        resultWords.clear();
                        resultWords.add(word);
                        targetLength = length;
                    } else if (length == targetLength) {
                        resultWords.add(word);
                    }
                }
            }

            context.write(new Text(key.toString() + " words"), new Text(resultWords.toString()));
        }
    }


        public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Find Longest and Shortest Words");

            job.setJarByClass(LargestShortestWord.class);
            job.setMapperClass(WordLengthMapper.class);
            job.setReducerClass(WordLengthReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));  // Input file path
            FileOutputFormat.setOutputPath(job, new Path(args[1])); // Output directory path

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
}
