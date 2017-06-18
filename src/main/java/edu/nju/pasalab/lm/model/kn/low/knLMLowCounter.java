package edu.nju.pasalab.lm.model.kn.low;

import chaski.utils.BinaryToStringCodec;
import edu.nju.pasalab.lm.smoothing.kn.KNSmoothingDriver;
import edu.nju.pasalab.util.BIGDECIMAL;
import edu.nju.pasalab.util.basicMath;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

/**
 * Created by YWJ on 2016.8.28.
 * Copyright (c) 2016 NJU PASA Lab All rights reserved.
 */
public class knLMLowCounter {

    public static class knStep1Mapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private long count = 1L;
        static final LongWritable one = new LongWritable(1L);

        public knStep1Mapper() {

        }
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
                throws IOException, InterruptedException {
            setup(context);
            Configuration conf = context.getConfiguration();
            count = Long.parseLong(conf.get("count"));

            String[] split = value.toString().trim().split("\\s+");


            long partCount = Long.parseLong(split[split.length - 1]);
            double val = basicMath.Divide(partCount, count);
            context.write(new Text(split[0] + " " + val), one);
        }
    }

    public static class knStep1Reducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        public void reduce(Text key, Iterable<LongWritable> values,
                           Reducer<Text, LongWritable, Text, LongWritable>.Context context
        ) throws IOException, InterruptedException {

            context.write(key, null);
        }
    }

    public static class knStep2Mapper extends Mapper<LongWritable, Text, Text, Text> {
        private String encodeObject = null;
        static final LongWritable one = new LongWritable(1L);

        public knStep2Mapper() {

        }

        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            /**
             * key-value format
             * original: w1w2 count
             * now : w1, w1 w2#count#D  #(w1w2)
             */
            setup(context);
            Configuration conf = context.getConfiguration();
            encodeObject = conf.get("encodeObject");
            BinaryToStringCodec codec = new BinaryToStringCodec(false);
            KNSmoothingDriver knSmoothing = (KNSmoothingDriver)codec.decodeObject(encodeObject);

            String[] split = value.toString().trim().split("\\s+");

            StringBuilder sp = new StringBuilder(64);

            long count = Long.parseLong(split[split.length - 1]);

            sp.append(value.toString().trim()).append(" ||| ")
                    .append(count).append(" ||| ")
                    .append(knSmoothing.getD());

            context.write(new Text(split[0]), new Text(sp.toString()));
        }
    }


    public static class knStep2Reducer extends Reducer<Text, Text, Text, LongWritable> {
        //private LongWritable result = new LongWritable();

        public void reduce(Text key, Iterable<Text> values,
                           Reducer<Text, Text, Text, LongWritable>.Context context
        ) throws IOException, InterruptedException {
            long count = 0L;
            HashSet<String> set = new HashSet<>();
            double D = 0.0;

            for (Text val : values) {
                String[] split = val.toString().trim().split(" \\|\\|\\| ");

                String[] str = split[0].trim().split("\\s+");
                set.add(str[str.length - 1]);

                count += Long.parseLong(split[1].trim());
                D = Double.parseDouble(split[2].trim());
            }

            int size = set.size();

            double bow = Math.log10(BIGDECIMAL.divide(D, count*1.0)) + Math.log10(size * 1.0);
            String res = key.toString().trim() + " " + bow;

            context.write(new Text(res), null);
        }
    }
}

