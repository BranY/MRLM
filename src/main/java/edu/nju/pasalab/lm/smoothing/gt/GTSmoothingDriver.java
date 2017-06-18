package edu.nju.pasalab.lm.smoothing.gt;

import chaski.utils.CompositeSubmittable;
import chaski.utils.JobSequence;
import chaski.utils.JobSet;
import chaski.utils.Submittable;
import edu.nju.pasalab.lm.gram.gramCounter;
import edu.nju.pasalab.util.BIGDECIMAL;
import edu.nju.pasalab.util.CommonFileOperations;
import edu.nju.pasalab.util.FileName;
import edu.nju.pasalab.util.parameters;

import java.io.*;
import java.util.ArrayList;

/**
 * Created by YWJ on 2016.8.17.
 * Copyright (c) 2016 NJU PASA Lab All rights reserved.
 */
public class GTSmoothingDriver implements Serializable {

    final int stopSmoothingThreshold = 1000;

    public parameters ep = null;
    public int n = 0;
    public boolean highcocurrency = false;
    public long all_r = 0L;

    public long N1 = 1L;
    public double para1 = 0.0;
    public double para2 = 0.0;


    public GTSmoothingDriver(parameters e, int order) {
        this.ep = e;
        this.n = order;
        this.highcocurrency = false;
    }

    public GTSmoothingDriver(parameters e, int order, long count) {
        this.ep = e;
        this.n = order;
        this.highcocurrency = false;
        this.all_r = count;
        execute();
    }

    public void execute() {

        try {
            GTSmoothingCounter.buildFreq2Freq(FileName.getGramPath(ep.lmRootDir, this.n),
                    this.n, this. ep);
        } catch (Exception e){
            e.printStackTrace();
        }

        try {
            InputStream src = CommonFileOperations.openFileForRead(FileName.getGTFreq2Freq(ep.lmRootDir, n), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(src, "UTF-8"));

            long N1 = 0;
            String line = in.readLine();

            @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
            ArrayList<Entry> freqList = new ArrayList<>(100);

            while (line != null) {
                String[] split = line.trim().split("\\s+");

                int freq = Integer.parseInt(split[0]);
                int freq2Freq = Integer.parseInt(split[1]);

                if (freq == 1) N1 = freq2Freq;
                freqList.add(new Entry(freq, freq2Freq));

                line = in.readLine();
            }

            src.close();
            in.close();

            setN1(N1);
            double[] arr = getPara(freqList, stopSmoothingThreshold, this.all_r);
            setPara1(arr[0]);
            setPara2(arr[1]);

            System.out.println(arr[0] + "\t" + arr[1] + "\t" + this.all_r);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static double[] getPara(ArrayList<Entry> freqList, int stopSmoothingThreshold, long total) {
        double paras[] = new double[2];

        double arr[] = new double[5];
        for (int i = 0; i < 5; i++) arr[i] = 0.0;

        for (Entry entry: freqList) {
            if (entry.freq < stopSmoothingThreshold) {
                arr[0] += Math.log(entry.freq) * Math.log(entry.fre2Freq);
                arr[1] += Math.log(entry.fre2Freq);
                arr[2] += Math.log(entry.freq);
                arr[3] += Math.log(entry.freq) * Math.log(entry.freq);
            }
        }

        paras[0] = Divide((arr[0] * total - arr[0] * arr[1]), (total * arr[3] - arr[2] * arr[2]));
        paras[1] = Divide((arr[1] - paras[0] * arr[2]), (total * 1.0));

        return paras;
    }

    public void setN1(long value) { this.N1 = value; }
    public void setPara1(double value) { this.para1 = value; }
    public void setPara2(double value) { this.para2 = value; }

    public long getN1() { return this.N1; }
    public double getPara1() { return this.para1; }
    public double getPara2() { return this.para2; }

    public double getSmoothedCount(long r, long N1, double para1) {
        double res ;
        if (r > stopSmoothingThreshold) {
            res = r * 1.0;
        } else if (r == 0) {
            res = 1.0 * N1;
        } else {
            res = r * Math.pow(1 + (1.0 / r * 1.0), para1);
        }
        return res;
    }

    public static double Divide(double a, double b) {
       return BIGDECIMAL.divide(a, b);
    }
}
