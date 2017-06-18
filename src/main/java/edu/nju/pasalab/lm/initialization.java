package edu.nju.pasalab.lm;

import edu.nju.pasalab.util.CommonFileOperations;
import edu.nju.pasalab.util.FileName;
import edu.nju.pasalab.util.filterDriver;
import edu.nju.pasalab.util.parameters;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Created by YWJ on 2016.8.17.
 * Copyright (c) 2016 NJU PASA Lab All rights reserved.
 */
public class initialization {

    public static void execute(parameters ep) {
        System.out.println("-----------------Initialization Start------------------");

        try {
            CommonFileOperations.deleteHDFSIfExists(FileName.getSourceRawCourpus(ep.lmRootDir));
            InputStream src = CommonFileOperations.openFileForRead(ep.lmInputFile, true);
            OutputStream out = CommonFileOperations.openFileForWrite(FileName.getSourceRawCourpus(ep.lmRootDir), true);
            filterDriver filter = new filterDriver("UTF-8");

            filter.execute(src, out);

            src.close();
            out.close();
        } catch (IOException e){
            e.printStackTrace();
        }
        System.out.println("-----------------Initialization Done------------------");
    }
}
