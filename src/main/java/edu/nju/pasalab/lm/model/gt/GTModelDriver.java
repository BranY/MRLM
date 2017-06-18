package edu.nju.pasalab.lm.model.gt;

import chaski.utils.BinaryToStringCodec;
import edu.nju.pasalab.lm.smoothing.gt.GTSmoothingDriver;
import edu.nju.pasalab.util.CommonFileOperations;
import edu.nju.pasalab.util.FileName;
import edu.nju.pasalab.util.parameters;

/**
 * Created by YWJ on 2016.8.15.
 * Copyright (c) 2016 NJU PASA Lab All rights reserved.
 */
public class GTModelDriver {

    public parameters ep = null;
    public int n = 0;
    public long[] count = null;
    public GTModelDriver(parameters e, long[] total) {
        this.ep = e;
        this.n = this.ep.N;
        this.count = total;
        execute(this.ep);
    }

    public void execute(parameters ep) {
        CommonFileOperations.deleteHDFSIfExists(ep.lmRootDir + "/gt");

        for (int i = 0; i < n; ++i) {
            GTSmoothingDriver gtSmoothing = new GTSmoothingDriver(ep, i+1, count[i]);

            BinaryToStringCodec codec = new BinaryToStringCodec(false);
            String str = codec.encodeObject(gtSmoothing);

            try {
                gtCounter.trainingGTLanguageModel(FileName.getGramPath(ep.lmRootDir, i+1),
                        FileName.getGTPath(ep.lmRootDir, i+1), i, str, ep);

            } catch (Exception e){
                e.printStackTrace();
            }
        }
    }
}
