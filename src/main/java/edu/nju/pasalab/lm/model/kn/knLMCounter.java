package edu.nju.pasalab.lm.model.kn;

import edu.nju.pasalab.lm.model.kn.high.knHighLMStep1Counter;
import edu.nju.pasalab.lm.model.kn.high.knHighLMStep2Counter;
import edu.nju.pasalab.lm.model.kn.high.knHighLMStep3Counter;
import edu.nju.pasalab.lm.model.kn.low.knLMLowCounter;
import edu.nju.pasalab.util.CommonFileOperations;
import edu.nju.pasalab.util.FileName;
import edu.nju.pasalab.util.parameters;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Created by YWJ on 2016.8.18.
 * Copyright (c) 2016 NJU PASA Lab All rights reserved.
 */
public class knLMCounter {
    public knLMCounter() {

    }

    public static void trainingKNLanguageModel(int n, long count, String[] encodeObject, parameters ep) throws Exception {
        if (n == 5) {
            String step1In = FileName.getGramPath(ep.lmRootDir, n);
            String step1Out = FileName.getKNPath(ep.lmRootDir, n);
            String cachePath = FileName.getKNPath(ep.lmRootDir, n-1);

            CommonFileOperations.deleteHDFSIfExists(step1Out);

            Configuration conf = new Configuration();
            String jobName = "build high knLM step 1" + n + " ," + step1In + "==>" + step1Out;

            conf.set("encodeObject", encodeObject[n-1]);
            Job job = Job.getInstance(conf, jobName);
            //DistributedCache
            job.addCacheFile(new Path(cachePath).toUri());

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setMapperClass(knHighLMStep1Counter.knStep1Mapper.class);
            job.setReducerClass(knHighLMStep1Counter.knStep1Reducer.class);

            job.setNumReduceTasks(ep.reducerNum);
            job.setMaxMapAttempts(ep.mapperNum);
            FileInputFormat.addInputPath(job, new Path(step1In));
            FileOutputFormat.setOutputPath(job, new Path(step1Out));

            job.waitForCompletion(true);

        } else {
            System.out.println("-----------------KN Language Model Step1 Start------------------");
            String step1In = FileName.getGramPath(ep.lmRootDir, n);
            String step1Out = FileName.getKNStep1Path(ep.lmRootDir, n);
            String cachePath = FileName.getKNPath(ep.lmRootDir, n-1);

            CommonFileOperations.deleteHDFSIfExists(step1Out);
            Configuration step1Conf = new Configuration();
            String step1JobName = "build high knLM step 1" + n + " ," + step1In + "==>" + step1Out;

            step1Conf.set("encodeObject", encodeObject[n-1]);
            Job step1Job = Job.getInstance(step1Conf, step1JobName);
            //DistributedCache
            step1Job.addCacheFile(new Path(cachePath).toUri());

            step1Job.setInputFormatClass(TextInputFormat.class);
            step1Job.setOutputFormatClass(TextOutputFormat.class);
            step1Job.setMapOutputKeyClass(Text.class);
            step1Job.setMapOutputValueClass(Text.class);

            step1Job.setMapperClass(knHighLMStep1Counter.knStep1Mapper.class);
            step1Job.setReducerClass(knHighLMStep1Counter.knStep1Reducer.class);

            step1Job.setNumReduceTasks(ep.reducerNum);
            step1Job.setMaxMapAttempts(ep.mapperNum);
            FileInputFormat.addInputPath(step1Job, new Path(step1In));
            FileOutputFormat.setOutputPath(step1Job, new Path(step1Out));

            step1Job.waitForCompletion(true);
            System.out.println("-----------------KN Language Model Step1 Done------------------");

            System.out.println("-----------------KN Language Model Step2 Start------------------");

            String step2In = FileName.getGramPath(ep.lmRootDir, n+1);
            String step2Out = FileName.getKNStep2Path(ep.lmRootDir, n);
            CommonFileOperations.deleteHDFSIfExists(step2Out);

            Configuration step2Conf = new Configuration();
            String step2JobName = "build high knLM Step2 " + n + " ," + step2In + "==>" + step2Out;
            step2Conf.set("encodeObject", encodeObject[n]);

            Job step2Job = Job.getInstance(step2Conf, step2JobName);

            step2Job.setInputFormatClass(TextInputFormat.class);
            step2Job.setOutputFormatClass(TextOutputFormat.class);
            step2Job.setMapOutputKeyClass(Text.class);

            step2Job.setMapOutputValueClass(Text.class);

            step2Job.setMapperClass(knHighLMStep2Counter.knStep2Mapper.class);
            step2Job.setReducerClass(knHighLMStep2Counter.knStep2Reducer.class);

            step2Job.setNumReduceTasks(ep.reducerNum);
            step2Job.setMaxMapAttempts(ep.mapperNum);
            FileInputFormat.addInputPath(step2Job, new Path(step2In));
            FileOutputFormat.setOutputPath(step2Job, new Path(step2Out));

            step2Job.waitForCompletion(true);
            System.out.println("-----------------KN Language Model Step2 Done------------------");

            System.out.println("-----------------KN Language Model Step3 Start------------------");
            String step3In = FileName.getKNStep1Path(ep.lmRootDir, n);
            String step3CacheFile = FileName.getKNStep2Path(ep.lmRootDir, n);
            String step3Out = FileName.getKNPath(ep.lmRootDir, n);
            CommonFileOperations.deleteHDFSIfExists(step3Out);

            Configuration step3Conf = new Configuration();
            String step3JobName = "build high knLM Step3 " + n + " ," + step3In + "==>" + step3Out;

            Job step3Job = Job.getInstance(step3Conf, step3JobName);
            step3Job.addCacheFile(new Path(step3CacheFile).toUri());

            step3Job.setInputFormatClass(TextInputFormat.class);
            step3Job.setOutputFormatClass(TextOutputFormat.class);
            step3Job.setMapOutputKeyClass(Text.class);
            step3Job.setMapOutputValueClass(LongWritable.class);

            step3Job.setMapperClass(knHighLMStep3Counter.knLMStep3Mapper.class);
            step3Job.setReducerClass(knHighLMStep3Counter.knLMStep3Reducer.class);

            step3Job.setNumReduceTasks(ep.reducerNum);
            step3Job.setMaxMapAttempts(ep.mapperNum);
            FileInputFormat.addInputPath(step3Job, new Path(step3In));
            FileOutputFormat.setOutputPath(step3Job, new Path(step3Out));

            step3Job.waitForCompletion(true);
            System.out.println("-----------------KN Language Model Step3 Done------------------");
            //System.exit(0);
        }
    }

    public static void trainingKNLanguageModel_1(int n, long count, String encodeObject, parameters ep) throws Exception {

        String inputCorpus = FileName.getGramPath(ep.lmRootDir, n);
        String output = FileName.getKNStep1Path(ep.lmRootDir, n);

        String step2In = FileName.getGramPath(ep.lmRootDir, n+1);
        String step2Out = FileName.getKNStep2Path(ep.lmRootDir, n);

        CommonFileOperations.deleteHDFSIfExists(output);
        CommonFileOperations.deleteHDFSIfExists(step2Out);

        Configuration conf = new Configuration();
        String jobName = "build low knLM step 1" + n + " ," + inputCorpus + "==>" + output;

        Long c = count;
        conf.set("count", c.toString());
        Job job = Job.getInstance(conf, jobName);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setMapperClass(knLMLowCounter.knStep1Mapper.class);
        job.setReducerClass(knLMLowCounter.knStep1Reducer.class);

        job.setNumReduceTasks(ep.reducerNum);
        job.setMaxMapAttempts(ep.mapperNum);
        FileInputFormat.addInputPath(job, new Path(inputCorpus));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);

        Configuration conf1 = new Configuration();
        String jobName1 = "build low knLM Step2 " + n + " ," + step2In + "==>" + step2Out;
        conf1.set("encodeObject", encodeObject);

        Job job1 = Job.getInstance(conf1, jobName1);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        job1.setMapOutputKeyClass(Text.class);

        job1.setMapOutputValueClass(Text.class);

        job1.setMapperClass(knLMLowCounter.knStep2Mapper.class);
        job1.setReducerClass(knLMLowCounter.knStep2Reducer.class);

        job1.setNumReduceTasks(ep.reducerNum);
        job1.setMaxMapAttempts(ep.mapperNum);
        FileInputFormat.addInputPath(job1, new Path(step2In));
        FileOutputFormat.setOutputPath(job1, new Path(step2Out));

        job1.waitForCompletion(true);
    }
}
