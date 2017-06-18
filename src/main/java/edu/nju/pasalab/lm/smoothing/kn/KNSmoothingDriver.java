package edu.nju.pasalab.lm.smoothing.kn;

import edu.nju.pasalab.util.CommonFileOperations;
import edu.nju.pasalab.util.FileName;
import edu.nju.pasalab.util.parameters;

import java.io.*;

/**
 * Created by YWJ on 2016.8.15.
 * Copyright (c) 2016 NJU PASA Lab All rights reserved.
 */
public class KNSmoothingDriver implements Serializable {
    public parameters ep = null;
    public int n = 0;
    public boolean highcocurrency = false;

    public long all_r = 1L;
    public long n1 = 1L;
    public long n2 = 1L;
    public double D = 1L;

    public KNSmoothingDriver(parameters e, int order, long count) {
        this.ep = e;
        this.n = order;
        this.highcocurrency = false;
        this.all_r = count;
        execute();
    }

    public void execute() {
        System.out.println("-----------------KN fre2freq Build Start------------------");
        try {
            KNSmoothingCounter.buildFreq2Freq(FileName.getGramPath(ep.lmRootDir, this.n),
                    this.n, this.ep);
            //String temp = FileName.getKNFreq2Freq(ep.lmRootDir, n);

        } catch (Exception e){
            e.printStackTrace();
        }

        System.out.println("-----------------KN parameters Build Start------------------");
        try {

            InputStream src = CommonFileOperations.openFileForRead(FileName.getKNFreq2Freq(ep.lmRootDir, n), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(src, "UTF-8"));

            long N1 = 0;
            long N2 = 0;
            String line = in.readLine();

            while (line != null ) {
                String[] split = line.trim().split("\\s+");

                int freq = Integer.parseInt(split[0]);
                int freq2Freq = Integer.parseInt(split[1]);

                if (freq == 1) N1 = freq2Freq;
                else if (freq == 2) N2 = freq2Freq;

                line = in.readLine();
            }

            src.close();
            in.close();

            setN1(N1);
            setN2(N2);
            double d;
            if (N1 == 0 && N2 == 0) d = 0.0;
            else d = (N1 * 1.0) / (N1 * 1.0 + 2.0 * N2);
            setD(d);

            System.out.println("-----------------KN parameters Build Done------------------");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void setD(double value) { this.D = value; }
    public void setN1(long val) { this.n1 = val; }
    public void setN2(long val) { this.n2 = val; }

    public double getD() { return this.D; }
    public long getN1() { return this.n1; }
    public long getN2() { return this.n2; }
}
