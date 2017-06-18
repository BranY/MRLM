package edu.nju.pasalab.lm.model.gt;

import chaski.utils.BinaryToStringCodec;
import edu.nju.pasalab.lm.smoothing.gt.GTSmoothingDriver;
import edu.nju.pasalab.util.CommonFileOperations;
import edu.nju.pasalab.util.basicMath;
import edu.nju.pasalab.util.parameters;
import org.apache.commons.io.output.NullWriter;
import org.apache.commons.io.output.StringBuilderWriter;
import org.apache.commons.lang.ObjectUtils;
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
 * Created by YWJ on 2016.8.15.
 * Copyright (c) 2016 NJU PASA Lab All rights reserved.
 */
public class gtCounter {

    public gtCounter() {

    }

    public static void trainingGTLanguageModel(String inputCorpus, String output, int n, String encodeObject, parameters ep) throws Exception {
        CommonFileOperations.deleteHDFSIfExists(output);

        Configuration conf = new Configuration();
        String jobName = "build gtLM " + n + " ," + inputCorpus + "==>" + output;

        conf.set("encodeObject", encodeObject);
        Job job = Job.getInstance(conf, jobName);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        //job.setMapOutputValueClass(NullWriter.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setMapperClass(gtCounter.gtLMMapper.class);
        job.setReducerClass(gtCounter.gtLMReducer.class);

        job.setNumReduceTasks(ep.reducerNum);
        job.setMaxMapAttempts(ep.mapperNum);
        FileInputFormat.addInputPath(job, new Path(inputCorpus));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
    }

    public static class gtLMMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private String encodeObject = null;
        static final LongWritable one = new LongWritable(1L);

        public gtLMMapper() {

        }
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
                throws IOException, InterruptedException {
            setup(context);
            Configuration conf = context.getConfiguration();
            encodeObject = conf.get("encodeObject");
            BinaryToStringCodec codec = new BinaryToStringCodec(false);
            GTSmoothingDriver gtSmoothing = (GTSmoothingDriver)codec.decodeObject(encodeObject);

            String[] split = value.toString().trim().split("\\s+");

            StringBuilder sp = new StringBuilder(128);
            sp.append(split[0]);
            for (int i = 1; i < split.length - 1; ++i)
                sp.append(" ").append(split[i]);

            long count = Long.parseLong(split[split.length - 1]);
            long N1 = gtSmoothing.getN1();
            double para1 = gtSmoothing.getPara1();
            long allCount = gtSmoothing.all_r;

            sp.append("\t").append(basicMath.Divide(gtSmoothing.getSmoothedCount(count, N1, para1), allCount * 1.0));

           // context.write(new Text(sp.toString()), null);
            context.write(new Text(sp.toString()), one);
        }
    }

    public static class gtLMReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        //private LongWritable result = new LongWritable();

        public void reduce(Text key, Iterable<LongWritable> values,
                           Reducer<Text, LongWritable, Text, LongWritable>.Context context
        ) throws IOException, InterruptedException {
            /*int sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            result.set(sum);*/
            context.write(key, null);
        }
    }

}
