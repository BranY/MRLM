package edu.nju.pasalab.lm.model.kn;

import edu.nju.pasalab.util.CommonFileOperations;
import edu.nju.pasalab.util.FileName;
import edu.nju.pasalab.util.basicMath;
import edu.nju.pasalab.util.parameters;
import gnu.trove.TObjectDoubleHashMap;

import java.io.*;

/**
 * Created by YWJ on 2016.8.18.
 * Copyright (c) 2016 NJU PASA Lab All rights reserved.
 */
public class KNModelDriver {
    public parameters ep = null;
    public int n = 0;
    public long[] count = null;
    public String[] encodeKN = null;
    public KNModelDriver(parameters e, long[] total, String[] kn) {
        this.ep = e;
        this.n = this.ep.N;
        this.count = total;
        this.encodeKN = kn;
        execute(this.ep);
    }

    public void execute(parameters ep) {

        CommonFileOperations.deleteHDFSIfExists(ep.lmRootDir + "/kn");
        for (int i = 0; i < n; ++i) {

            if(i == 0) {
                try {
                    knLMCounter.trainingKNLanguageModel_1(i+1, count[i], encodeKN[i+1], ep);

                    InputStream step2 = CommonFileOperations.openFileForRead(FileName.getKNStep2Path(ep.lmRootDir, i+1), true);
                    BufferedReader br_step2 = new BufferedReader(new InputStreamReader(step2, "UTF-8"));
                    TObjectDoubleHashMap<String> bowMap = new TObjectDoubleHashMap<>(300, .8f);
                    String line1 = br_step2.readLine();
                    while (line1 != null) {
                        String[] split = line1.trim().split("\\s+");
                        double val = Double.parseDouble(split[1]);
                        bowMap.put(split[0], val);
                        line1 = br_step2.readLine();
                    }
                    step2.close();
                    br_step2.close();

                    InputStream step1 = CommonFileOperations.openFileForRead(FileName.getKNStep1Path(ep.lmRootDir, i+1), true);
                    BufferedReader br_step1 = new BufferedReader(new InputStreamReader(step1, "UTF-8"));
                    OutputStream out = CommonFileOperations.openFileForWrite(FileName.getKNPath(ep.lmRootDir, i+1), true);
                    PrintWriter prOut = new PrintWriter(new OutputStreamWriter(out, "UTF-8"));

                    prOut.println(basicMath.Divide(1, count[i]) + "\tunk\t0.0");
                    String line = br_step1.readLine();
                    while (line != null) {
                        String[] sp = line.trim().split("\\s+");
                        if (sp[0].equals("<s>") || sp[0] == "<s>") {
                            double bow = bowMap.get("<s>");
                            prOut.println("0.0\t<s>\t" + bow);
                        } else if (sp[0].equals("</s>") || sp[0] == "</s>"){
                            prOut.println(sp[1] + "\t</s>\t0.0");
                        } else {
                            double bow = bowMap.get(sp[0]);
                            prOut.println(sp[1] + "\t" + sp[0] + "\t" + bow);
                        }
                        line = br_step1.readLine();
                    }
                    prOut.flush();
                    prOut.close();
                    out.close();
                    br_step1.close();
                    step1.close();

                } catch (Exception e) {
                    e.printStackTrace();
                }

            } else {
                System.out.println("----------High Order KN LM----------");
                try {
                    knLMCounter.trainingKNLanguageModel(i+1, count[i], encodeKN, ep);

                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        }
    }
}
