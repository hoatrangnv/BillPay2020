/*
 * Copyright 2011 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.bank.manage;

import org.apache.log4j.Logger;
import com.viettel.mmserver.base.ProcessManager;

public class FileManager {

    private static FileManager instance;
    private Logger logger;
    private int getProcessId;
    private int importProcessId;

    public static FileManager getInstance() throws Exception {
        if (instance == null) {
            instance = new FileManager();
        }
        return instance;
    }

    public FileManager() throws Exception {
        logger = Logger.getLogger(FileManager.class);
        GetBankFile getter = new GetBankFile("GetFileBank");
        getProcessId = getter.getId();
        ImportBankFile importer = new ImportBankFile("ImportFileBank");
        importProcessId = importer.getId();
    }

    public void start() {
        logger.info("+++  SYSTEM IS STARTING  +++");
        ProcessManager.getInstance().getMmProcess(getProcessId).start();
        ProcessManager.getInstance().getMmProcess(importProcessId).start();
    }

    public void stop() {
        logger.info("+++  SYSTEM IS STOPPING  +++");
        ProcessManager.getInstance().getMmProcess(getProcessId).stop();
        ProcessManager.getInstance().getMmProcess(importProcessId).stop();
    }
}
