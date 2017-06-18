package edu.nju.pasalab.util;

/**
 * Created by YWJ on 2016.8.17.
 * Copyright (c) 2016 NJU PASA Lab All rights reserved.
 */
public class basicMath {

    public static double Divide(double a, double b) {
        return Math.log10(a) - Math.log10(b);
    }

    public static double Divide(long a, long b) {
        return Math.log10(a*1.0) - Math.log10(b*1.0);
    }

    public static double Divide(int a, long b) {
        return Math.log10(a*1.0) - Math.log10(b*1.0);
    }

    public static double Divide(int a, int b) {
        return Math.log10(a*1.0) - Math.log10(b*1.0);
    }
}
