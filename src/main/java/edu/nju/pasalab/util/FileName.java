package edu.nju.pasalab.util;

/**
 * Created by YWJ on 2016.12.14.
 * Copyright (c) 2016 NJU PASA Lab All rights reserved.
 */
public class FileName {


    private String packageHDFS;
    private String rootDir;

    public String getRootDir() {
        return rootDir;
    }
    public void setRootDir(String rootDir) {
        this.rootDir = rootDir;
    }

    public FileName(String packageHDFS, String rootDir) {
        super();
        this.packageHDFS = packageHDFS;
        this.rootDir = rootDir;
    }


    public static String getSourceRawCourpus(String rootDir) {
        return rootDir + "/source-raw-corpus";
    }

    public static String getGramPath(String rootDir, int entry) {
        return rootDir + "/gram/" + String.format("%08d", entry);
    }

    public static  String getGTFreq2Freq(String rootDir, int entry) {
        return rootDir + "/gt/freq2Freq/" + String.format("%08d", entry);
    }

    public static String getKNFreq2Freq(String rootDir, int entry) {
        return rootDir + "/kn/freq2Freq/" + String.format("%08d", entry);
    }

    public static String getMKNFreq2Freq(String rootDir, int entry) {
        return rootDir + "/mkn/freq2Freq/" + String.format("%08d", entry);
    }

    public static String getGTPath(String rootDir, int entry){
        return rootDir + "/gt/result/" + String.format("%08d", entry);
    }

    public static String getGSBPath(String rootDir, int entry) {
        return (entry == 1) ? rootDir + "/gsb/result/" + String.format("%08d", entry) + "/part-00000"
                : rootDir + "/gsb/result/" + String.format("%08d", entry);
    }

    public static String getKNStep1Path(String rootDir, int entry) {
        return (entry == 1) ? rootDir + "/kn/step1/" + String.format("%08d", entry)
                : rootDir + "/kn/step1/" + String.format("%08d", entry);
    }

    public static String getKNStep2Path(String rootDir, int entry) {
        return (entry == 1) ? rootDir + "/kn/step2/" + String.format("%08d", entry)
                : rootDir + "/kn/step2/" + String.format("%08d", entry);
    }

    public static String getKNPath(String rootDir, int entry) {
        return (entry == 1) ? rootDir + "/kn/result/" + String.format("%08d", entry) + "/part-00000"
                : rootDir + "/kn/result/" + String.format("%08d", entry);
    }

    public static String getMKNStep1Path(String rootDir, int entry) {
        return (entry == 1) ? rootDir + "/mkn/step1/" + String.format("%08d", entry)
                : rootDir + "/mkn/step1/" + String.format("%08d", entry);
    }

    public static String getMKNStep2Path(String rootDir, int entry) {
        return (entry == 1) ? rootDir + "/mkn/step2/" + String.format("%08d", entry)
                : rootDir + "/mkn/step2/" + String.format("%08d", entry);
    }

    public static String getMKNPath(String rootDir, int entry) {
        return (entry == 1) ? rootDir + "/mkn/result/" + String.format("%08d", entry) + "/part-00000"
                : rootDir + "/mkn/result/" + String.format("%08d", entry);
    }
}
