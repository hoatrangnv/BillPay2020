/*
 * Copyright (C) 2010 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.bank.manage;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.viettel.mmserver.base.ProcessThreadMX;
import java.io.File;
import java.util.Date;
import java.util.*;
//import com.viettel.security.PassTranformer;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;

/**
 *
 * @author trungnd8@viettel.com.vn
 * @since Mar 11, 2011
 * @version 1.0
 */
public class GetBankFile extends ProcessThreadMX {

    private File localDir = null;
    private int fileCount = 0;
    private final String separator = "/";
    private final int ERROR_DELAY = 2000;
    private final String EXTEND = "_temp";
    JSch jsch = new JSch();
    Session sessionSftp = null;
    Channel channelSftp = null;
    ChannelSftp cSftp = null;
    private String lDir;
    private String ftpDir;
    private String ftpBkDir;
    private String ftpHost;
    private String ftpUser;
    private String ftpPass;
    private int ftpPort;
    private int delayTime = 0;
    private String backupType;

    public GetBankFile(
            String threadName) throws Exception {
        super(threadName);
        registerAgent("GetBankFile:Name=" + threadName);
        this.threadName = threadName;
    }

    public int cycle() {
        try {
            if (connectFtpServer() < 0) {
                return -1;
            }
            String sFtpBackupDir = ftpBkDir;
            if (!sFtpBackupDir.startsWith("/")) {
                sFtpBackupDir = separator + ftpBkDir;
            }
            cSftp.cd(sFtpBackupDir);
            if (ftpDir.startsWith("/")) {
                cSftp.cd(ftpDir);
            } else {
                cSftp.cd("/" + ftpDir);
            }
            if (getFileProcess() < 0) {
                return -1;
            }
            return 0;
        } catch (Exception ex) {
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            logger.error("Had exception in cycle: " + sw.toString());
            return -1;
        }
    }

    int connectFtpServer() {
        if (sessionSftp == null || !sessionSftp.isConnected()) {
            try {
                sessionSftp = jsch.getSession(ftpUser, ftpHost, ftpPort);
                sessionSftp.setPassword(ftpPass);
                java.util.Properties config = new java.util.Properties();
                config.put("StrictHostKeyChecking", "no");
                sessionSftp.setConfig(config);
                sessionSftp.connect();
                channelSftp = sessionSftp.openChannel("sftp");
                channelSftp.connect();
                cSftp = (ChannelSftp) channelSftp;
            } catch (Exception ex) {
                StringWriter sw = new StringWriter();
                ex.printStackTrace(new PrintWriter(sw));
                logger.error("Had exception in connectFtpServer host " + ftpHost + ": " + sw.toString());
                clearFtpConnect();
                return -1;
            }
        }
        return 0;
    }

    int clearFtpConnect() {
        try {
            if (sessionSftp != null) {
                if (sessionSftp.isConnected()) {
                    sessionSftp.disconnect();
                }
                sessionSftp = null;
            }
            return 0;
        } catch (Exception ex) {
            logger.error("Had exception in clearFtpConnect host " + ftpHost + ": " + ex.toString());
            sessionSftp = null;
            return -1;
        }
    }

    private int createFolder(String strFolder) {
        try {
            File inFolder = new File(strFolder);
            if (inFolder.exists()) {
                if (!inFolder.isDirectory()) {
                    return -1;
                }
            } else {
                String parentName = inFolder.getParent();
                if (parentName == null) {
                    inFolder.mkdir();
                } else {
                    File foldertomake = new File(parentName);
                    if (foldertomake.exists()) {
                        inFolder.mkdir();
                    } else {
                        if (createFolder(parentName) == -1) {
                            return -1;
                        }
                        if (createFolder(strFolder) == -1) {
                            return -1;
                        }
                    }
                }
            }
            return 0;
        } catch (Exception e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            logger.error("Had exception in createFolder: " + sw.toString());
            return -1;
        }
    }

    int getFileProcess() {
        try {
            Vector files;
            files = cSftp.ls(ftpDir);
            fileCount = 0;
            if (files.isEmpty()) {
                logger.info("List file is empty, so return 0 ");
                return 0;   //no error
            }
            logger.info("[List file, folder]: " + files.size());
            for (int i = 0; i < files.size(); i++) {
                buStartTime = new Date();
                com.jcraft.jsch.ChannelSftp.LsEntry lsEntry = (com.jcraft.jsch.ChannelSftp.LsEntry) files.get(i);
                if (lsEntry.getFilename().startsWith("MEPS") || lsEntry.getFilename().startsWith("BMEPS")) {
                    if (!cSftp.isConnected()) {
                        logger.error("[Error - getFileProcess] The connection was droped");
                        break;
                    }
                    if (getOneFile(lsEntry) < 0) {
                        return -1;
                    }
                    if (ftpBackup(lsEntry)) {
                        File tempFile = new File(lDir + separator + lsEntry.getFilename() + EXTEND);
                        if (localRename(tempFile)) {
                            fileCount++;
                            logger.info("Complete get file: " + lsEntry.getFilename());
                        } else {
                            logger.warn("[Warning] Can not save file to local folder : " + lsEntry.getFilename());
                        }
                    }
                } else {
                    logger.warn(lsEntry.getFilename() + " not demanding file");
                }
                buStartTime = null;
            }
            logger.info("Total file got: " + fileCount);
            return 0;
        } catch (Exception ex) {
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            logger.error("[Error - getFileProcess]: " + sw.toString());
            return -1;
        }
    }

    boolean ftpBackup(LsEntry lsEntry) {
        try {
            cSftp.cd(ftpDir);
            String backupFilename = lsEntry.getFilename();
//            String sBkDir = "";
            if (backupType.equals("Delete".toLowerCase())) {
                cSftp.rm(lsEntry.getFilename());
            } else {
                String sDateFormat = "yyyyMMdd";
                SimpleDateFormat dateForm = new SimpleDateFormat(sDateFormat);
                String sDate = dateForm.format(new Date());
                String sBackupDateDir = ftpBkDir + sDate;
                try {
                    cSftp.cd(sBackupDateDir);
                } catch (Exception e) {
                    logger.warn("Cannot cd to backup dir may be not create folder, so now try to create: "
                            + sBackupDateDir + " " + e.toString());
                    cSftp.mkdir(sBackupDateDir);
                }
                cSftp.cd(ftpDir);
                logger.info("Start bk file: " + lsEntry.getFilename() + " to " + sBackupDateDir);
                cSftp.rename(ftpDir + "/" + lsEntry.getFilename(), sBackupDateDir + "/" + backupFilename);
            }
            return true;
        } catch (Exception ex) {
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            logger.error("[Error - ftpBackup] fileName " + lsEntry.getFilename() + ": " + sw.toString());
            return false;
        }
    }

    boolean localRename(File file) {
        boolean success = false;
        try {
            int fileNameLength = file.getName().trim().length();
            String fileNameToRemove = file.getName().trim().substring(0, fileNameLength - 5);
            success = file.renameTo(new File(localDir, fileNameToRemove));
            return success;
        } catch (Exception ex) {
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            logger.error("[Error - localRename]: " + sw.toString());
            return false;
        }
    }

    int getOneFile(LsEntry lsEntry) {
        String tempFileName = "";
        try {
            tempFileName = lDir + separator + lsEntry.getFilename() + EXTEND;
            cSftp.get(lsEntry.getFilename(), lDir + "/" + lsEntry.getFilename() + EXTEND);
            logger.info("Dowloaded temp file: " + lsEntry.getFilename() + EXTEND);
            return 0;
        } catch (Exception ex) {
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            logger.error("[Error - getOneFile]: " + sw.toString());
            try {
                this.logger.info("[Error]:Can not download file " + lsEntry.getFilename());
                File f = new File(tempFileName);
                if (f.exists()) {
                    f.delete();
                }
            } catch (Exception e) {
                logger.info("Can not delete tempFileName " + e.toString());
            }
            return -1;
        }
    }

    @Override
    protected void process() {
        try {
            if (cycle() < 0) {
                clearFtpConnect();
                Thread.sleep(ERROR_DELAY);
            } else {
                Thread.sleep(delayTime);
            }
        } catch (Exception ex) {
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            logger.error("[Error - process]: " + sw.toString());
        }
    }

    @Override
    public void stop() {
        logger.info("[System]: This thread is stopped ");
        super.stop();
    }

    @Override
    public void start() {
        logger.info("[System]: This thread is started ");
        super.start();
    }

    @Override
    protected void prepareStart() {
        try {
            logger.info("Prepare starting");
            delayTime = Integer.parseInt(ResourceBundle.getBundle("cfgBankFile").getString("delayTime"));
            lDir = ResourceBundle.getBundle("cfgBankFile").getString("lDir");
            ftpDir = ResourceBundle.getBundle("cfgBankFile").getString("ftpDir");
            ftpBkDir = ResourceBundle.getBundle("cfgBankFile").getString("ftpBkDir");
            ftpHost = ResourceBundle.getBundle("cfgBankFile").getString("ftpHost");
            ftpUser = ResourceBundle.getBundle("cfgBankFile").getString("ftpUser");
            ftpPass = ResourceBundle.getBundle("cfgBankFile").getString("ftpPass");
            ftpPort = Integer.parseInt(ResourceBundle.getBundle("cfgBankFile").getString("ftpPort"));
            backupType = ResourceBundle.getBundle("cfgBankFile").getString("backupType");
            File dir = null;
            dir = new File(lDir);
            if (!dir.exists()) {
                if (createFolder(lDir) < 0) {
                    logger.warn("Error While creating localDir " + lDir + " so now stop");
                    stop();
                }
            }
            localDir = dir;
        } catch (Exception ex) {
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            logger.error("Had exception in prepareStart: " + sw.toString() + " now must stop");
            stop();
        }
    }

    public static void main(String[] args) {
        try {
            GetBankFile getter = new GetBankFile("GetFileBank");
            getter.start();
            ImportBankFile importer = new ImportBankFile("ImportFileBank");
            importer.start();
        } catch (Exception e) {
            System.out.println("Have error when init GetFileBank, ImportFileBank " + e.toString());
        }
    }
}
