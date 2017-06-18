package edu.nju.pasalab.lm.model.gsb;

import edu.nju.pasalab.util.CommonFileOperations;
import edu.nju.pasalab.util.basicMath;
import edu.nju.pasalab.util.parameters;
import gnu.trove.TObjectLongHashMap;
import org.apache.commons.io.output.NullWriter;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;

/**
 * Created by YWJ on 2016.8.27.
 * Copyright (c) 2016 NJU PASA Lab All rights reserved.
 */
public class gsbLMCounter {

    public gsbLMCounter() {

    }

    public static void trainingGSBLanguageModel(String inputCorpus, String preInput, String output,
                                                int n, double alpha, parameters ep) throws Exception {
        CommonFileOperations.deleteHDFSIfExists(output);

        Configuration conf = new Configuration();
        String jobName = "build gsbLM " + n + " ," + inputCorpus + "==>" + output;

        Double val = alpha;
        Integer val1 = n;
        conf.set("alpha", val.toString());
        conf.set("order", val1.toString());

        Job job = Job.getInstance(conf, jobName);

        //DistributedCache
        job.addCacheFile(new Path(preInput).toUri());

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        //job.setMapOutputValueClass(NullWriter.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setMapperClass(gsbLMCounter.gsbLMMapper.class);
        job.setReducerClass(gsbLMCounter.gsbLMReducer.class);

        job.setNumReduceTasks(0);
        job.setMaxMapAttempts(ep.mapperNum);
        FileInputFormat.addInputPath(job, new Path(inputCorpus));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
    }

    public static class gsbLMMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        static final LongWritable one = new LongWritable(1L);

        public double alpha = 0.4;
        public int n = 0;
        public String path[] = null;

        public TObjectLongHashMap<String> prevMap = new TObjectLongHashMap<String>(300, .8f);
        public gsbLMMapper() {

        }

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            URI[] uri = context.getCacheFiles();
            //path = new String[uri.length];
            //for (int i = 0; i < uri.length; ++i) path[i] = uri[i].getPath();

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
                    String[] split = line.trim().split("\\s+");
                    StringBuilder sb = new StringBuilder(64);
                    sb.append(split[0]);

                    for (int i = 1; i < split.length - 1; ++i)
                        sb.append(" ").append(split[i]);
                    long val = Long.parseLong(split[split.length - 1]);
                    prevMap.put(sb.toString(), val);

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
            Configuration conf = context.getConfiguration();
            alpha = Double.parseDouble(conf.get("alpha"));
            n = Integer.parseInt(conf.get("order"));

            String noSpace = value.toString().trim();
            String prev = noSpace.substring(0, noSpace.lastIndexOf(" "));

            String[] split = noSpace.split("\\s+");
            StringBuilder sp = new StringBuilder(128);


            sp.append(split[0]);
            for (int i = 1; i < split.length - 1; ++i)
                sp.append(" ").append(split[i]);

            long count = Long.parseLong(split[split.length - 1]);
            long preCount = prevMap.get(prev);

            //String keyStr = basicMath.Divide(count, preCount) + "\t" + sp.toString();
            StringBuilder keyStr = new StringBuilder(128);
            keyStr.append(basicMath.Divide(count, preCount)).append("\t").append(sp.toString());

            if (n != 4) {
                keyStr.append("\t").append(Math.log10(alpha));
            }
            context.write(new Text(keyStr.toString()), one);
        }
    }

    public static class gsbLMReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
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
