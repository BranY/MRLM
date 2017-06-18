package test;

/**
 * Created by YWJ on 2016.6.18.
 * Copyright (c) 2016 NJU PASA Lab All rights reserved.
 */
public class testJava {
    public static void main(String[] args) {
        String str = "A\tB\tC";
        String[] sp = str.split("\t");

        System.out.println(sp[0] + " " + sp[1] + " " + sp[2]);

        String str1 ="A B C";
        String suffix = str1.substring(str1.indexOf(" ")).trim();

        System.out.println(suffix);
    }
}
