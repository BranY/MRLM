package edu.nju.pasalab.lm.smoothing.mkn;

import edu.nju.pasalab.util.CommonFileOperations;
import edu.nju.pasalab.util.FileName;
import edu.nju.pasalab.util.parameters;

import java.io.*;

/**
 * Created by YWJ on 2016.8.28.
 * Copyright (c) 2016 NJU PASA Lab All rights reserved.
 */
public class MKNSmoothingDriver implements Serializable {
    public parameters ep = null;
    public int n = 0;
    public boolean highcocurrency = false;

    public long all_r = 1L;
    public long n1 = 1L;
    public long n2 = 1L;
    public long n3 = 1L;
    public long n4 = 1l;

    public double D = 1L;
    public double D1 = 1L;
    public double D2 = 1L;
    public double D3 = 1L;

    public MKNSmoothingDriver(parameters e, int order, long count) {
        this.ep = e;
        this.n = order;
        this.highcocurrency = false;
        this.all_r = count;
        execute();
    }

    public void execute() {

        try {
            MKNSmoothingCounter.buildFreq2Freq(FileName.getGramPath(ep.lmRootDir, this.n),
                    this.n, this.ep);
            //String temp = FileName.getKNFreq2Freq(ep.lmRootDir, n);

        } catch (Exception e){
            e.printStackTrace();
        }

        try {

            InputStream src = CommonFileOperations.openFileForRead(FileName.getMKNFreq2Freq(ep.lmRootDir, n), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(src, "UTF-8"));

            long N1 = 0;
            long N2 = 0;
            long N3 = 0;
            long N4 = 0;

            String line = in.readLine();

            while (line != null ) {
                String[] split = line.trim().split("\\s+");

                int freq = Integer.parseInt(split[0]);
                long freq2Freq = Long.parseLong(split[1]);

                if (freq == 1) N1 = freq2Freq;
                else if (freq == 2) N2 = freq2Freq;
                else if (freq == 3) N3 = freq2Freq;
                else if (freq == 4) N4 = freq2Freq;

                line = in.readLine();
            }

            src.close();
            in.close();

            setN1(N1);
            setN2(N2);
            setN3(N3);
            setN4(N4);

            double d ;
            if (N1 == 0 && N2 == 0) d = 0.0;
            else d = (N1 * 1.0) / (N1 * 1.0 + 2.0 * N2);

            double d1;
            if (N1 == 0) d1 = 0.0;
            else d1 = 1.0 - 2.0 * N2 / N1;

            double d2;
            if (N2 == 0) d2 = 0.0;
            else d2 = 2.0 - 3.0 * N3 / N2;

            double d3;
            if (N3 == 0) d3 = 0.0;
            else d3 = 3.0 - 4.0 * N4 / N3;

            setD(d);
            setD1(d1);
            setD2(d2);
            setD3(d3);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void setN1(long val) { this.n1 = val; }
    public void setN2(long val) { this.n2 = val; }
    public void setN3(long val) { this.n3 = val; }
    public void setN4(long val) { this.n4 = val; }

    public void setD(double val) { this.D = val; }
    public void setD1(double val) { this.D1 = val; }
    public void setD2(double val) { this.D2 = val; }
    public void setD3(double val) { this.D3 = val; }

    public long getN1() { return this.n1; }
    public long getN2() { return this.n2; }
    public long getN3() { return this.n3; }
    public long getN4() { return this.n4; }

    public double getD(){ return this.D; }
    public double getD1(){ return this.D1; }
    public double getD2(){ return this.D2; }
    public double getD3(){ return this.D3; }
}
