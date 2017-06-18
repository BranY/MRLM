package edu.nju.pasalab.lm.smoothing.mkn;

import edu.nju.pasalab.util.CommonFileOperations;
import edu.nju.pasalab.util.FileName;
import edu.nju.pasalab.util.parameters;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by YWJ on 2016.8.28.
 * Copyright (c) 2016 NJU PASA Lab All rights reserved.
 */
public class MKNSmoothingCounter {
    public MKNSmoothingCounter() {

    }

    public static void buildFreq2Freq (String inputCorpus, int n, parameters ep) throws Exception {
        String temp = FileName.getMKNFreq2Freq(ep.lmRootDir, n);
        CommonFileOperations.deleteIfExists(temp);

        Configuration conf = new Configuration();
        String jobName = "build freq2freq " + n + ", Building gram " + inputCorpus + "==>" + temp;

        Job job = Job.getInstance(conf, jobName);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setMapperClass(MKNSmoothingCounter.mknMapper.class);
        job.setCombinerClass(MKNSmoothingCounter.mknReducer.class);
        job.setReducerClass(MKNSmoothingCounter.mknCombiner.class);

        job.setNumReduceTasks(1);
        job.setMaxMapAttempts(ep.mapperNum);
        FileInputFormat.addInputPath(job, new Path(inputCorpus));
        FileOutputFormat.setOutputPath(job, new Path(temp));

        job.waitForCompletion(true);
    }

    public static class mknMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        static final LongWritable one = new LongWritable(1L);

        public mknMapper() {

        }
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
                throws IOException, InterruptedException {

            String[] split = value.toString().trim().split("\\s+");
            context.write(new Text(split[split.length - 1]), one);
        }
    }

    public static class mknCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {
        protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context)
                throws IOException, InterruptedException {

            long count = 0L;
            for (LongWritable w : values) {
                count += w.get();
            }
            context.write(key, new LongWritable(count));
        }
    }

    public static class mknReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
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
