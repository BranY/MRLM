package edu.nju.pasalab.lm.model.mkn.low;

import chaski.utils.BinaryToStringCodec;
import edu.nju.pasalab.lm.smoothing.mkn.MKNSmoothingDriver;
import edu.nju.pasalab.util.BIGDECIMAL;
import edu.nju.pasalab.util.basicMath;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by YWJ on 2016.8.30.
 * Copyright (c) 2016 NJU PASA Lab All rights reserved.
 */
public class mknLMLowCounter {

    public static class mknStep1Mapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private long count = 1L;
        static final LongWritable one = new LongWritable(1L);

        public mknStep1Mapper() {

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

    public static class mknStep1Reducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        public void reduce(Text key, Iterable<LongWritable> values,
                           Reducer<Text, LongWritable, Text, LongWritable>.Context context
        ) throws IOException, InterruptedException {

            context.write(key, null);
        }
    }

    public static class mknStep2Mapper extends Mapper<LongWritable, Text, Text, Text> {
        private String encodeObject = null;
        static final LongWritable one = new LongWritable(1L);

        public mknStep2Mapper() {

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
            MKNSmoothingDriver mknSmoothing = (MKNSmoothingDriver)codec.decodeObject(encodeObject);

            String[] split = value.toString().trim().split("\\s+");

            StringBuilder sp = new StringBuilder(64);

            long count = Long.parseLong(split[split.length - 1]);

            sp.append(value.toString().trim()).append(" ||| ")
                    .append(count).append(" ||| ")
                    .append(mknSmoothing.getD1()).append(" ||| ")
                    .append(mknSmoothing.getD2()).append(" ||| ")
                    .append(mknSmoothing.getD3());

            context.write(new Text(split[0]), new Text(sp.toString()));
        }
    }


    public static class mknStep2Reducer extends Reducer<Text, Text, Text, LongWritable> {
        public void reduce(Text key, Iterable<Text> values,
                           Reducer<Text, Text, Text, LongWritable>.Context context
        ) throws IOException, InterruptedException {
            long count = 0L;
            //HashSet<String> set = new HashSet<>();
            double D1 = 0.0;
            double D2 = 0.0;
            double D3 = 0.0;
            long N1 = 0;
            long N2 = 0;
            long N3plus = 0;

            for (Text val : values) {
                String[] split = val.toString().trim().split(" \\|\\|\\| ");

                long tmp = Long.parseLong(split[1].trim());
                if (tmp == 1) N1 += 1;
                else if (tmp == 2) N2 += 1;
                else if (tmp == 3) N3plus += 1;
                count += tmp;
                D1 = Double.parseDouble(split[2].trim());
                D2 = Double.parseDouble(split[3].trim());
                D3 = Double.parseDouble(split[4].trim());
            }
            double molecular = getBOWMolecular(D1, N1, D2, N2, D3, N3plus);
            double bow = Math.log10(BIGDECIMAL.divide(molecular, count * 1.0));
            String res = key.toString().trim() + " " + bow;
            context.write(new Text(res), null);
        }
    }

    public static double getBOWMolecular(double d1, long n1, double d2, long n2, double d3, long n3Plus) {
        double part1 = BIGDECIMAL.multiply(d1, n1 * 1.0);
        double part2 = BIGDECIMAL.add(part1, BIGDECIMAL.multiply(d2, n2 * 1.0));
        return BIGDECIMAL.add(part2, BIGDECIMAL.multiply(d3, n3Plus * 1.0));
    }
}
