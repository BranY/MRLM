package edu.nju.pasalab.lm.model.gsb;

import edu.nju.pasalab.util.CommonFileOperations;
import edu.nju.pasalab.util.FileName;
import edu.nju.pasalab.util.basicMath;
import edu.nju.pasalab.util.parameters;

import java.io.*;

/**
 * Created by YWJ on 2016.8.27.
 * Copyright (c) 2016 NJU PASA Lab All rights reserved.
 */
public class GSBModelDriver {
    public parameters ep = null;
    public int n = 0;
    public long[] count = null;
    public double alpha = 0.4;

    public GSBModelDriver(parameters e, long[] total, double a){
        this.ep = e;
        this.n = this.ep.N;
        this.count = total;
        this.alpha = a;
        execute(this.ep);
    }

    public void execute(parameters ep) {
        CommonFileOperations.deleteHDFSIfExists(ep.lmRootDir + "/gsb");

        for (int i = 0; i < n; ++i) {

            if (i == 0) {
                try {
                    InputStream src = CommonFileOperations.openFileForRead(FileName.getGramPath(ep.lmRootDir, i+1), true);
                    OutputStream out = CommonFileOperations.openFileForWrite(FileName.getGSBPath(ep.lmRootDir, i+1), true);
                    BufferedReader srcIn = new BufferedReader(new InputStreamReader(src, "UTF-8"));
                    PrintWriter srcOut = new PrintWriter(new OutputStreamWriter(out, "UTF-8"));

                    String line = srcIn.readLine();

                    while (line != null) {
                        String[] split = line.trim().split("\\s+");

                        srcOut.println(basicMath.Divide(1L, count[i]) + "\t" + "unk\t 0.0");
                        if (split[0].equals("<s>") || split[0] == "<s>") {
                            srcOut.println(0.0 + "\t<s>\t" + Math.log10(alpha));
                        } else if (split[0].equals("</s>") || split[0] == "</s>"){

                            int value = Integer.parseInt(split[1]);
                            srcOut.println(basicMath.Divide(value, count[i]) + "\t" + split[0] + "\t 0");
                        } else {
                            int value = Integer.parseInt(split[1]);
                            srcOut.println(basicMath.Divide(value, count[i]) + "\t" + split[0] + "\t" + Math.log10(alpha));
                        }
                        line = srcIn.readLine();
                    }

                    srcOut.flush();
                    srcIn.close();
                    srcOut.close();
                    src.close();
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else {

                try {
                    gsbLMCounter.trainingGSBLanguageModel(FileName.getGramPath(ep.lmRootDir, i+1),
                            FileName.getGramPath(ep.lmRootDir, i), FileName.getGSBPath(ep.lmRootDir, i+1), i, alpha, ep);

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
