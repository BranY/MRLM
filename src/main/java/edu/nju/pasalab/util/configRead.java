package edu.nju.pasalab.util;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.LoggerFactory;
import org.slf4j.*;

import java.io.File;

/**
 * Created by YWJ on 2016.8.16.
 * Copyright (c) 2016 NJU PASA Lab All rights reserved.
 */
public class configRead {
    public static Logger logger = LoggerFactory.getLogger(configRead.class);

    public configRead(Config config) {

        getTrainingSettings(config);
       //printTrainingSetting();
    }

    public static parameters getTrainingSettings(Config config) {
        parameters ep = new parameters();
        /**
         * language modle
         */
        ep.lmRootDir =config.getString("lm.lmRootDir");
        ep.lmInputFile = config.getString("lm.lmInputFileName");
        ep.N = config.getInt("lm.N");

        ep.GT = config.getBoolean("lm.GT");
        ep.KN = config.getBoolean("lm.KN");
        ep.MKN = config.getBoolean("lm.MKN");
        ep.GSB = config.getBoolean("lm.GSB");

        ep.mapperNum = config.getInt("lm.mapper");
        ep.reducerNum = config.getInt("lm.reducer");
        return ep;
    }

    public static void printTrainingSetting(parameters ep){
        System.out.println("----------------------------- Language Model Parameter Settings ---------------------------");
        System.out.printf("--inputFileName %54s\n", ep.lmInputFile);
        System.out.printf("--lm working root directory %36s\n", ep.lmRootDir);
        System.out.printf("--N %54d\n", ep.N);
        System.out.printf("--GT %56b\n", ep.GT);
        System.out.printf("--KN %57b\n", ep.KN);
        System.out.printf("--MKN %56b\n", ep.MKN);
        System.out.printf("--GSB %56b\n", ep.GSB);
        System.out.printf("--MapperNum % 46d\n", ep.mapperNum);
        System.out.printf("--ReducerNum % 45d\n", ep.reducerNum);
        System.out.println("--------------------------------------------------------------------------------------------");

        logger.info("--------------------------------------------------------------------------------------------");
        logger.info("----------------------------- Language Model Parameter Settings ---------------------------");
        logger.info(String.format("--inputFileName %54s", ep.lmInputFile));
        logger.info(String.format("--lm working root directory %36s", ep.lmRootDir));
        logger.info(String.format("--N %54d", ep.N));
        logger.info(String.format("--GT %56b", ep.GT));
        logger.info(String.format("--KN %57b", ep.KN));
        logger.info(String.format("--MKN %56b", ep.MKN));
        logger.info(String.format("--GSB %56b", ep.GSB));
        logger.info(String.format("--MapperNum % 46d", ep.mapperNum));
        logger.info(String.format("--ReducerNum % 45d", ep.reducerNum));
        logger.info("--------------------------------------------------------------------------------------------\n");

    }
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "D:\\workspace\\winutils");

        String configFile = "data/training.conf";
        Config config = ConfigFactory.parseFile(new File(configFile));

        parameters ep = configRead.getTrainingSettings(config);

        configRead.printTrainingSetting(ep);

    }
}
