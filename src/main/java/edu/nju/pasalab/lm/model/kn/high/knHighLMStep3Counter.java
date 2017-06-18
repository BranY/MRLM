package edu.nju.pasalab.lm.model.kn.high;

import edu.nju.pasalab.util.CommonFileOperations;
import edu.nju.pasalab.util.basicMath;
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
import java.util.Map;

/**
 * Created by YWJ on 2016.8.28.
 * Copyright (c) 2016 NJU PASA Lab All rights reserved.
 */
public class knHighLMStep3Counter {
    public knHighLMStep3Counter() {

    }

    public static class knLMStep3Mapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        static final LongWritable one = new LongWritable(1L);

        //public int n = 0;

        public TObjectDoubleHashMap<String> prevMap = new TObjectDoubleHashMap<String>(300, .8f);
       // public Map<String, String> prevMap = new HashMap<String, String>(300, .8f);
        public knLMStep3Mapper() {

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
                    String[] split = line.trim().split("\t");
                    if (split.length != 2) {
                        System.exit(-1);
                        throw(new ArrayIndexOutOfBoundsException());
                    }

                    double val = Double.parseDouble(split[1].trim());
                    //System.out.println(line);
                    prevMap.put(split[0].trim(), val);

                    line = in.readLine();
                }

                src.close();
                in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
                throws IOException, InterruptedException {
            setup(context);

            String[] split = value.toString().trim().split("\t");

            String keyStr = split[1];
            String prob = split[0];

            double bow = prevMap.get(keyStr);
            String valueStr = prob + "\t" + keyStr + "\t" + bow;
            context.write(new Text(valueStr), one);
        }
    }

    public static class knLMStep3Reducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        public void reduce(Text key, Iterable<LongWritable> values,
                           Reducer<Text, LongWritable, Text, LongWritable>.Context context
        ) throws IOException, InterruptedException {

            context.write(key, null);
        }
    }
}
