package edu.nju.pasalab.lm.smoothing.gt;

/**
 * Created by YWJ on 2016.8.17.
 * Copyright (c) 2016 NJU PASA Lab All rights reserved.
 */
public class Entry {
    public int freq = 0;
    public int fre2Freq = 0;

    public Entry(int a, int b) {
        this.freq = a;
        this.fre2Freq = b;
    }
}
