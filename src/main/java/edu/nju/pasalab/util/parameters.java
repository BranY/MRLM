package edu.nju.pasalab.util;

import java.io.Serializable;

/**
 * Created by YWJ on 2016.8.16.
 * Copyright (c) 2016 NJU PASA Lab All rights reserved.
 */
public class parameters implements Serializable {

    public parameters() {

    }

    public String lmRootDir = "";
    public String lmInputFile = "Monolingual corpus";
    public int N = 5;

    public Boolean GT = false;
    public Boolean KN = false;
    public Boolean MKN = false;
    public Boolean GSB = false;

    public int mapperNum = 5;
    public int reducerNum = 1;
}
