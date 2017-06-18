package edu.nju.pasalab.lm.model.kn.high;

import chaski.utils.BinaryToStringCodec;
import edu.nju.pasalab.lm.smoothing.kn.KNSmoothingDriver;
import edu.nju.pasalab.util.BIGDECIMAL;
import edu.nju.pasalab.util.CommonFileOperations;
import gnu.trove.map.hash.TObjectDoubleHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;

/**
 * Created by YWJ on 2016.8.28.
 * Copyright (c) 2016 NJU PASA Lab All rights reserved.
 */
public class knHighLMStep1Counter {

    public knHighLMStep1Counter(){

    }
    public static class knStep1Mapper extends Mapper<LongWritable, Text, Text, Text> {
        static final LongWritable one = new LongWritable(1L);

        private String encodeObject = "";
        //public int n = 0;
        public TObjectDoubleHashMap<String> prevMap = new TObjectDoubleHashMap<String>(300, .8f);

        public knStep1Mapper() {

        }
        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            URI[] uri = context.getCacheFiles();

            for (URI file : uri) {
                parseCacheFile(file.getPath());
            }
        }

        private void parseCacheFile(String fileName) {
            try {
                InputStream src = CommonFileOperations.openFileForRead(fileName, true);
                BufferedReader in = new BufferedReader(new InputStreamReader(src, "UTF-8"));
                String line = in.readLine();

                while (line != null) {
                    // P w1w2 BOW
                    String[] split = line.trim().split("\t");
                    double val = Double.parseDouble(split[0]);
                    prevMap.put(split[1], val);
                    line = in.readLine();
                }

                src.close();
                in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            setup(context);
            Configuration conf = context.getConfiguration();
            //n = Integer.parseInt(conf.get("order"));
            encodeObject = conf.get("encodeObject");
            if (encodeObject.equals("") || encodeObject == "")
                throw(new NullPointerException());

            BinaryToStringCodec codec = new BinaryToStringCodec(false);
            KNSmoothingDriver knSmoothing = (KNSmoothingDriver) codec.decodeObject(encodeObject);

            String[] split = value.toString().trim().split("\\s+");

            long count = Long.parseLong(split[split.length - 1]);
            StringBuilder sb = new StringBuilder(64);
            sb.append(split[0]);
            for (int i = 1; i < split.length - 1; ++i) sb.append(" ").append(split[i]);

            String keyStr = sb.toString().trim();
            String prefix = keyStr.substring(0, keyStr.lastIndexOf(" ")).trim();
            String suffix = keyStr.substring(keyStr.indexOf(" ")).trim();

            StringBuilder valueStr = new StringBuilder(128);
            valueStr.append(keyStr).append(" ||| ")
                    .append(count).append(" ||| ")
                    .append(knSmoothing.getD()).append(" ||| ")
                    .append(prevMap.get(suffix));

            context.write(new Text(prefix), new Text(valueStr.toString()));
        }

    }

    public static class knStep1Reducer extends Reducer<Text, Text, Text, LongWritable> {

        public void reduce(Text key, Iterable<Text> values,
                           Reducer<Text, Text, Text, LongWritable>.Context context
        ) throws IOException, InterruptedException {
            long prevCount = 0L;
            HashSet<String> set = new HashSet<>();
            ArrayList<String> list = new ArrayList<>();
            for (Text val : values) {

                list.add(val.toString().trim());
                String[] split = val.toString().trim().split(" \\|\\|\\| ");

                String[] str = split[0].trim().split("\\s+");
                set.add(str[str.length - 1]);

                prevCount += Long.parseLong(split[1].trim());
            }
            int size = set.size();

            for (String val : list) {

                String[] split = val.split(" \\|\\|\\| ");
                long count = Long.parseLong(split[1].trim());
                double D = Double.parseDouble(split[2].trim());
                double lowOrder = Double.parseDouble(split[3].trim());

                double lowOrderNormal = Math.pow(10, lowOrder);
                double prob = getPartialProb(count, prevCount, D) + BIGDECIMAL.multiply(bowProb(D, prevCount, size), lowOrderNormal);
                String keyStr = Math.log10(prob) + "\t" + split[0];

                context.write(new Text(keyStr), null);
            }
            cleanup(context);
        }

    }

    public static double getPartialProb(long count, long prevCount, double D) {
        double res = 0.0;
        if ((count - D) <= 0.0) res = 0.0;
        else {
            BIGDECIMAL.divide((count - D), prevCount * 1.0);
        }
        return res;
    }

    public static double bowProb(double D, long prevCount, int size) {
        double p = BIGDECIMAL.divide(D, prevCount * 1.0);
        return BIGDECIMAL.multiply(p, size * 1.0);
    }
}
