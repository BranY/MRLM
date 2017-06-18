package edu.nju.pasalab.lm.gram;

import chaski.utils.*;
import edu.nju.pasalab.util.FileName;
import edu.nju.pasalab.util.parameters;

/**
 * Created by YWJ on 2016.8.17.
 * Copyright (c) 2016 NJU PASA Lab All rights reserved.
 */
public class gramDriver {
    public parameters ep = null;
    public int n = 0;
    public boolean highcocurrency = false;


    public gramDriver(parameters e, boolean high){
        this.ep = e;
        this.n = ep.N;
        this.highcocurrency = high;
    }

    public long[] execute() {

        long[] recordNum = new long[5];
        try {
            for (int i = 1; i <= this.n; ++i) {
                System.out.println("-----------------Build " + i + "-GRAM Start------------------");

                recordNum[i-1] = gramCounter.buildGram2(FileName.getSourceRawCourpus(ep.lmRootDir),
                        FileName.getGramPath(ep.lmRootDir, i), i, this.ep);

                System.out.println("-----------------Build " + i + "-GRAM Done------------------");
            }
        } catch (Exception e){
            e.printStackTrace();
        }

        return recordNum;
    }

    public void execute1() {

        CompositeSubmittable jobs = highcocurrency ? new JobSet() : new JobSequence();
        try {
            for (int i = 1; i <= this.n; ++i) {
                CompositeSubmittable sub = new JobSequence();
                Submittable gramJob = gramCounter.buildGram(FileName.getSourceRawCourpus(ep.lmRootDir),
                        FileName.getGramPath(ep.lmRootDir, i), i, this.ep);

                sub.add(gramJob);

                jobs.add(sub);
            }

            jobs.submit();

        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
