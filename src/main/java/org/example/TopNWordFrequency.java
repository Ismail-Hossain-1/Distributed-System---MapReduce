package org.example;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TopNWordFrequency extends Configured implements Tool {

    // ---------- FIRST JOB: Word Count ----------

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken().replaceAll("[^a-zA-Z0-9]", "").toLowerCase());
                if (word.getLength() > 0) {
                    context.write(word, one);
                }
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    // ---------- SECOND JOB: Top N Sort ----------

    public static class SwapMapper extends Mapper<Object, Text, IntWritable, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\\s+");
            if (tokens.length == 2) {
                Text word = new Text(tokens[0]);
                IntWritable count = new IntWritable(Integer.parseInt(tokens[1]));
                context.write(count, word);
            }
        }
    }

    public static class DescendingIntWritableComparator extends IntWritable.Comparator {
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }

        public int compare(IntWritable a, IntWritable b) {
            return -super.compare(a, b);
        }
    }

    public static class TopNReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
        private int N;
        private int count = 0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            N = context.getConfiguration().getInt("N", 10); // default top 10
        }

        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text word : values) {
                if (count++ < N) {
                    context.write(word, key);
                } else {
                    return;
                }
            }
        }
    }

    // ---------- MAIN METHOD ----------

    public int run(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: TopNWordFrequency <input> <temp_output> <final_output> [N]");
            System.exit(2);
        }

        String input = args[0];
        String tempOutput = args[1];
        String finalOutput = args[2];
        int N = (args.length == 4) ? Integer.parseInt(args[3]) : 10;

        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "word count");


        job1.setJarByClass(TopNWordFrequency.class);


        job1.setMapperClass(TokenizerMapper.class);

        job1.setCombinerClass(IntSumReducer.class);
        job1.setReducerClass(IntSumReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(input));
        FileOutputFormat.setOutputPath(job1, new Path(tempOutput));
        boolean success = job1.waitForCompletion(true);

        if (!success) return 1;

        Configuration conf2 = new Configuration();
        conf2.setInt("N", N);
        Job job2 = Job.getInstance(conf2, "top n words");
        job2.setJarByClass(TopNWordFrequency.class);
        job2.setMapperClass(SwapMapper.class);
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setSortComparatorClass(DescendingIntWritableComparator.class);
        job2.setReducerClass(TopNReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path(tempOutput));
        FileOutputFormat.setOutputPath(job2, new Path(finalOutput));
        return job2.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new TopNWordFrequency(), args);
        System.exit(exitCode);
    }
}

