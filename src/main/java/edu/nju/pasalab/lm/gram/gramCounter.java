package edu.nju.pasalab.lm.gram;

import chaski.utils.CommonFileOperations;
import chaski.utils.JobSequence;
import chaski.utils.MapReduceJob;
import chaski.utils.Submittable;
import edu.nju.pasalab.util.parameters;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by YWJ on 2016.8.17.
 * Copyright (c) 2016 NJU PASA Lab All rights reserved.
 */
public class gramCounter {
    //private static final int reducerCount = 5;
    public gramCounter() {

    }

    public static Submittable buildGram(String inputCorpus, String output, int n, parameters ep) throws IOException {
        //String temp = workingRoot + "/gram-temp";
        CommonFileOperations.deleteIfExists(output);
        Configuration conf = new Configuration();
        String jobName = "build gram round " + n + ", Building gram " + inputCorpus + "==>" + output;

        Integer t = n;
        conf.set("gramOrder", t.toString());

        JobSequence sub = new JobSequence();
        MapReduceJob job = new MapReduceJob(conf, jobName);

        /*if(queues.trim().length() == 0) {
            job = new MapReduceJob(jobName);
        } else {
            job = new MapReduceJob(jobName, queues);
        }*/

        job.setBasicInfo(TextInputFormat.class, Text.class, LongWritable.class, TextOutputFormat.class,
                gramCounter.gramMapper.class,
                gramCounter.gramReducer.class,
                gramCounter.gramCombiner.class,
                ep.reducerNum,
                ep.mapperNum,
                inputCorpus,
                output);

        job.setSplitSizeRange(1024000, 16384000);

        //recordNum = job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "REDUCE_OUTPUT_RECORDS").getValue();
        sub.add(job);
        return sub;
    }

    public static long buildGram2(String inputCorpus, String output, int n, parameters ep) throws Exception {
        long recordsNum = 0;

        CommonFileOperations.deleteIfExists(output);
        Configuration conf = new Configuration();
        String jobName = "build gram round " + n + ", Building gram " + inputCorpus + "==>" + output;

        Integer t = n;
        conf.set("gramOrder", t.toString());
        Job job = Job.getInstance(conf, jobName);

        //job.setJarByClass(gramCounter.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setMapperClass(gramCounter.gramMapper.class);
        job.setCombinerClass(gramCounter.gramReducer.class);
        job.setReducerClass(gramCounter.gramCombiner.class);

        job.setNumReduceTasks(ep.reducerNum);
        job.setMaxMapAttempts(ep.mapperNum);
        FileInputFormat.addInputPath(job, new Path(inputCorpus));
        FileOutputFormat.setOutputPath(job, new Path(output));

        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

        job.waitForCompletion(true);
        recordsNum = job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter",
                "REDUCE_OUTPUT_RECORDS").getValue();

        return recordsNum;
    }

    public static class gramMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        static final LongWritable one = new LongWritable(1L);

        private Text gramOrder = new Text();
        //public int n = 0;
        public gramMapper() {
            //this.n = num;
        }

        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
                throws IOException, InterruptedException
        {
            setup(context);
            Configuration conf = context.getConfiguration();
            gramOrder = new Text(conf.get("gramOrder"));
            int n = Integer.parseInt(gramOrder.toString());

            String[] split = value.toString().trim().split("\\s+");
            int i = 0;
            int j = i + n - 1;
            while(((i + n - 1) < split.length) && (j < split.length)) {
                StringBuilder tmp = new StringBuilder(128);
                tmp.append(split[i]);
                for (int index = i + 1; index < j + 1; ++index)
                    tmp.append(" ").append(split[index]);

                context.write(new Text(tmp.toString()), one);
                i += 1;
                j += 1;
            }
        }
    }

    public static class gramCombiner
            extends Reducer<Text, LongWritable, Text, LongWritable> {
        protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context)
                throws IOException, InterruptedException
        {
            long count = 0L;
            for (LongWritable w : values) {
                count += w.get();
            }
            context.write(key, new LongWritable(count));
        }
    }

    public static class gramReducer
            extends Reducer<Text, LongWritable, Text, LongWritable> {
        private LongWritable result = new LongWritable();

        public void reduce(Text key, Iterable<LongWritable> values,
                           Reducer<Text, LongWritable, Text, LongWritable>.Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
}
