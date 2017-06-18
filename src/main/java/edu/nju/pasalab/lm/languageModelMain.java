package edu.nju.pasalab.lm;

import chaski.utils.BinaryToStringCodec;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import edu.nju.pasalab.lm.gram.gramDriver;
import edu.nju.pasalab.lm.model.gsb.GSBModelDriver;
import edu.nju.pasalab.lm.model.gt.GTModelDriver;
import edu.nju.pasalab.lm.model.kn.KNModelDriver;
import edu.nju.pasalab.lm.model.mkn.MKNModelDriver;
import edu.nju.pasalab.lm.smoothing.gt.GTSmoothingDriver;
import edu.nju.pasalab.lm.smoothing.kn.KNSmoothingDriver;
import edu.nju.pasalab.lm.smoothing.mkn.MKNSmoothingDriver;
import edu.nju.pasalab.util.*;

import java.io.*;

/**
 * Created by YWJ on 2016.8.14.
 * Copyright (c) 2016 NJU PASA Lab All rights reserved.
 */
public class languageModelMain {
    public static void main(String[] args){
        System.setProperty("hadoop.home.dir", "D:\\workspace\\winutils");

        Config config = ConfigFactory.parseFile(new File(args[0]));
        parameters ep = configRead.getTrainingSettings(config);
        configRead.printTrainingSetting(ep);

        initialization.execute(ep);

        gramDriver grams = new gramDriver(ep, false);
        long[] count = grams.execute();

        for (int i = 0; i < ep.N; ++i)
            System.out.println(count[i]);

        if (ep.GT) {
            System.out.println("-----------------GT Language Model Build Start------------------");

            /*String[] str = new String[ep.N];
            for (int i = 0; i < ep.N; ++i) {
                GTSmoothingDriver gtSmoothing = new GTSmoothingDriver(ep, i+1, count[i]);
                BinaryToStringCodec codec = new BinaryToStringCodec(false);
                str[i] = codec.encodeObject(gtSmoothing);
            }
            for (int i = 0; i < ep.N; ++i) {
                BinaryToStringCodec codec = new BinaryToStringCodec(false);
                GTSmoothingDriver gtSmoothing = (GTSmoothingDriver)codec.decodeObject(str[i]);

                System.out.println(gtSmoothing.all_r + "\t" + gtSmoothing.getN1() + "\t" +
                        gtSmoothing.getPara1() + "\t" + gtSmoothing.getPara2());
            }*/


            GTModelDriver gtModel = new GTModelDriver(ep, count);

            System.out.println("-----------------GT Language Model Build Done------------------");
        }

        if (ep.GSB){
            System.out.println("-----------------GSB Language Model Build Start------------------");

            GSBModelDriver gsbModel = new GSBModelDriver(ep, count, 0.4);

            System.out.println("-----------------GSB Language Model Build Done------------------");
        }

        if (ep.KN){
            System.out.println("-----------------KN Language Model Build Start------------------");
            String[] str = new String[ep.N];
            for (int i = 0; i < ep.N; ++i) {
                KNSmoothingDriver knSmoothing = new KNSmoothingDriver(ep, i+1, count[i]);
                BinaryToStringCodec codec = new BinaryToStringCodec(false);
                str[i] = codec.encodeObject(knSmoothing);
            }

            KNModelDriver kntModel = new KNModelDriver(ep, count, str);
            System.out.println("-----------------KN Language Model Build Done------------------");
        }

        if (ep.MKN){
            System.out.println("-----------------MKN Language Model Build Start------------------");
            String[] str = new String[ep.N];
            for (int i = 0; i < ep.N; ++i) {
                MKNSmoothingDriver mknSmoothing = new MKNSmoothingDriver(ep, i+1, count[i]);
                BinaryToStringCodec codec = new BinaryToStringCodec(false);
                str[i] = codec.encodeObject(mknSmoothing);
            }

            MKNModelDriver mkntModel = new MKNModelDriver(ep, count, str);
            System.out.println("-----------------MKN Language Model Build Done------------------");
        }

        System.exit(0);
    }
}
