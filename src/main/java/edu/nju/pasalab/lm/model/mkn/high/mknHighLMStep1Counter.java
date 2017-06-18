package edu.nju.pasalab.lm.model.mkn.high;

import chaski.utils.BinaryToStringCodec;
import edu.nju.pasalab.lm.smoothing.kn.KNSmoothingDriver;
import edu.nju.pasalab.lm.smoothing.mkn.MKNSmoothingDriver;
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
 * Created by YWJ on 2016.8.30.
 * Copyright (c) 2016 NJU PASA Lab All rights reserved.
 */
public class mknHighLMStep1Counter {
    public mknHighLMStep1Counter(){

    }
    public static class mknStep1Mapper extends Mapper<LongWritable, Text, Text, Text> {
        static final LongWritable one = new LongWritable(1L);

        private String encodeObject = "";
        public int n = 0;
        public TObjectDoubleHashMap<String> prevMap = new TObjectDoubleHashMap<String>(300, .8f);

        public mknStep1Mapper() {

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
            encodeObject = conf.get("encodeObject");

            if (encodeObject.equals("") || encodeObject == "")
                throw(new NullPointerException());

            BinaryToStringCodec codec = new BinaryToStringCodec(false);
            MKNSmoothingDriver mknSmoothing = (MKNSmoothingDriver) codec.decodeObject(encodeObject);

            String[] split = value.toString().trim().split("\\s+");

            long count = Long.parseLong(split[split.length - 1]);
            StringBuilder sb = new StringBuilder(64);
            sb.append(split[0]);
            for (int i = 1; i < split.length - 1; ++i) sb.append(" ").append(split[i]);

            String keyStr = sb.toString().trim();
            String prefix = keyStr.substring(0, keyStr.lastIndexOf(" ")).trim();
            String suffix = keyStr.substring(keyStr.indexOf(" ")).trim();

            // keyStr ||| count ||| D1 ||| D2 ||| D3 ||| Pmkn(lowOrder)
            StringBuilder valueStr = new StringBuilder(128);
            valueStr.append(keyStr).append(" ||| ")
                    .append(count).append(" ||| ")
                    .append(mknSmoothing.getD1()).append(" ||| ")
                    .append(mknSmoothing.getD2()).append(" ||| ")
                    .append(mknSmoothing.getD3()).append(" ||| ")
                    .append(prevMap.get(suffix));

            context.write(new Text(prefix), new Text(valueStr.toString()));
        }

    }

    public static class mknStep1Reducer extends Reducer<Text, Text, Text, LongWritable> {
        public int n = 0;

        public void reduce(Text key, Iterable<Text> values,
                           Reducer<Text, Text, Text, LongWritable>.Context context
        ) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            //n = Integer.parseInt(conf.get("Order"));

            long prevCount = 0L;
            //HashSet<String> set = new HashSet<>();
            long N1 = 0;
            long N2 = 0;
            long N3plus = 0;
            ArrayList<String> list = new ArrayList<>();
            for (Text val : values) {

                list.add(val.toString().trim());
                String[] split = val.toString().trim().split(" \\|\\|\\| ");

                long tmp = Long.parseLong(split[1].trim());
                if (tmp == 1) N1 += 1;
                else if (tmp == 2) N2 += 1;
                else if (tmp > 2) N3plus += 1;
                //count += tmp;
                prevCount += tmp;
            }
            //int size = set.size();

            for (String val : list) {

                String[] split = val.split(" \\|\\|\\| ");
                long count = Long.parseLong(split[1].trim());
                double D1 = Double.parseDouble(split[2].trim());
                double D2 = Double.parseDouble(split[3].trim());
                double D3 = Double.parseDouble(split[4].trim());
                double lowOrder = Double.parseDouble(split[5].trim()); //log10

                double D;
                if (count == 0) D = 0.0;
                else if (count == 1) D = D1;
                else if (count == 2) D = D2;
                else D = D3;

                double firstPart = getPartialProb(count, prevCount, D); //normal
                double molecularBOW = getBOWMolecular(D1, N1, D2, N2, D3, N3plus);
                double BOW = BIGDECIMAL.divide(molecularBOW, count * 1.0);

                double lowOrderNormal = Math.pow(10.0, lowOrder);
                double lastPart = BIGDECIMAL.multiply(BOW, lowOrderNormal); //normal
                double prob = BIGDECIMAL.add(firstPart, lastPart);

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
            res = BIGDECIMAL.divide((count - D), prevCount * 1.0);
        }
        return res;
    }

    public static double getBOWMolecular(double d1, long n1, double d2, long n2, double d3, long n3Plus) {
        double part1 = BIGDECIMAL.multiply(d1, n1 * 1.0);
        double part2 = BIGDECIMAL.add(part1, BIGDECIMAL.multiply(d2, n2 * 1.0));
        return BIGDECIMAL.add(part2, BIGDECIMAL.multiply(d3, n3Plus * 1.0));
    }
}
