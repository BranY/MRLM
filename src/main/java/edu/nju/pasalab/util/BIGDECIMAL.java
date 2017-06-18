package edu.nju.pasalab.util;

import java.math.BigDecimal;

/**
 * Created by YWJ on 2016.8.17.
 * Copyright (c) 2016 NJU PASA Lab All rights reserved.
 */
public class BIGDECIMAL {

    public static double add(double v1, double v2) {
        BigDecimal b1 = BigDecimal.valueOf(v1);
        BigDecimal b2 = BigDecimal.valueOf(v2);
        return b1.add(b2).doubleValue();
    }

    public static double subtract(double v1, double v2) {
        BigDecimal b1 = BigDecimal.valueOf(v1);
        BigDecimal b2 = BigDecimal.valueOf(v2);
        return b1.subtract(b2).doubleValue();
    }

    public static double multiply(double v1, double v2) {
        BigDecimal b1 = BigDecimal.valueOf(v1);
        BigDecimal b2 = BigDecimal.valueOf(v2);
        return b1.multiply(b2).doubleValue();
    }

    public static double divide(double v1, double v2) {
        BigDecimal b1 = BigDecimal.valueOf(v1);
        BigDecimal b2 = BigDecimal.valueOf(v2);
        int scale = 16;

        return b1.divide(b2, BigDecimal.ROUND_HALF_EVEN).doubleValue();
    }
}
