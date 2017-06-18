package edu.nju.pasalab.util;

import java.io.*;

/**
 * Created by YWJ on 2016.8.17.
 * Copyright (c) 2016 NJU PASA Lab All rights reserved.
 */
public class filterDriver {
    String encoding = "UTF-8";


    public filterDriver(String encoding) {
        this.encoding = encoding;
    }

    public void execute(InputStream src, OutputStream out) throws IOException {
        BufferedReader srcIn = new BufferedReader(new InputStreamReader(src, this.encoding));
        PrintWriter srcOut = new PrintWriter(new OutputStreamWriter(out, this.encoding));

        while (true) {
            String sline = srcIn.readLine();
            if (sline == null) break;

            if (sline.trim().length() > 0) {
                //StringBuilder sb = new StringBuilder(200);
                //sb.append("<s> ").append(sline).append(" </s>");
                String str = "<s> " + sline + " </s>";
                srcOut.println(str);
            }
        }
        srcOut.flush();
        srcIn.close();
        srcOut.close();
    }
}
