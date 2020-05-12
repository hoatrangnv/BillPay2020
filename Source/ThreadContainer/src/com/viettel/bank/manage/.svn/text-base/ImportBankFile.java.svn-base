package com.viettel.bank.manage;

import com.viettel.mmserver.base.ProcessThreadMX;
import com.viettel.paybonus.database.DbImportBankFile;
import com.viettel.paybonus.obj.BankFileDetail;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.ResourceBundle;

public class ImportBankFile extends ProcessThreadMX {

    private final Object sychronizingUnresolve = new Object();
    private SimpleDateFormat df = new SimpleDateFormat("yyMMddHHmmss");
    private int delayTime = 0;
    private String unrateDir;
    private String backupDir;
    private String importDir;
    private String tempDir;
    private String backupType;
    private DbImportBankFile db;
    private String[] standard = null;
    private String fileWildcard;
    private String line = null;

    public ImportBankFile(String threadName)
            throws Exception {
        super(threadName);
        registerAgent("Import:Name=" + threadName);
        this.threadName = threadName;
    }

    public int preImport() {
        try {
            File dir = null;
            dir = new File(importDir);
            if (!dir.exists()) {
                logger.error("[Error] Import dir " + importDir + " is not existed.");
                return -1;
            }
            dir = new File(importDir);
            if (!dir.exists()) {
                if (createFolder(importDir) < 0) {
                    logger.error("[Error] Backup dir not existed and can not create backup dir "
                            + importDir);
                    return -1;
                }
            }
            dir = new File(unrateDir);
            if (!dir.exists()) {
                if (createFolder(unrateDir) < 0) {
                    logger.error("[Error] Cannot create unrated dir " + unrateDir);
                    return -1;
                }
            }
            dir = new File(tempDir);
            if (!dir.exists()) {
                if (createFolder(tempDir) < 0) {
                    logger.error("[Error] Cannot create unrated dir " + tempDir);
                    return -1;
                }
            }
            return 0;
        } catch (Exception ex) {
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            logger.error("Had exception in preImport: " + sw.toString());
            return -1;
        }
    }

    public int cycle() {
        int totalBackup = 0;
        int totalBackupFail = 0;
        int totalUnrate = 0;
        int totalUnrateFail = 0;
        int totalImportFail = 0;
        int totalFile = 0;
        long stTime1;
        String[] listFile = new String[0];
        try {
            logger.info("Start new cycle, now list file from importDir " + importDir);
            if (preImport() < 0) {
                logger.info("preImport return -1 so please check config file for this thread:"
                        + this.threadName + " now go to sleep " + delayTime + " ms");
                return -1;
            }
            File impDir = new File(importDir);
//             Synchronizing list files in the import dir
            synchronized (sychronizingUnresolve) {
                listFile = impDir.list();
            }
            if (listFile != null) {
                totalFile = listFile.length;
            } else {
                logger.warn("listFile is null in folder " + importDir + " so go to sleep now delay time " + delayTime + " ms");
                return -1;
            }
//             Check if there isn't any file in import dir
            if (totalFile <= 0) {
                logger.warn("Can not find any files in importDir " + importDir + " so go to sleep now delay time " + delayTime + " ms");
                return -1;
            }
//             Start to import each file in the list             
            logger.info("Total files listed in this cycle is <" + totalFile + ">");
            stTime1 = System.currentTimeMillis();
            for (int iCount = 0; iCount < totalFile; iCount++) {
                //20111109 move buStartTime to monitor each file process
                buStartTime = new Date();
                String sFileName = listFile[iCount];
//                 Check file               
                logger.info("Check file name for file " + sFileName);
                if (checkFileName(sFileName) < 0) {
                    logger.warn("File " + sFileName + " is not in format name so now move to unrate, and continue for next file");
                    totalUnrate++;
                    if (unrateFile(sFileName) < 0) {
                        logger.warn("Cannot unrate file: " + sFileName);
                        totalUnrateFail++;
                    } else {
                        logger.info("File: " + sFileName + " is unrated to " + unrateDir);
                    }
                    continue;
                }
                int importResult = importProcess(sFileName);
                if (importResult < 0) {
                    logger.info("Fail to import file <" + sFileName + "> importResult " + importResult + " now unrate file");
                    totalImportFail++;
                    totalUnrate++;
//                    Move to unrated dir if importing unsuccessful                     
                    if (unrateFile(sFileName) < 0) {
                        totalUnrateFail++;
                        logger.warn("Cannot unrate file: " + sFileName);
                    } else {
                        logger.info("File: " + sFileName + " is unrated to " + unrateDir);
                    }
                } else {
                    logger.info("Success to import file <" + sFileName + "> importResult " + importResult + " now backup file");
                    totalBackup++;
                    int bkResult = backupFile(sFileName);
                    logger.info("Result backupfile < " + sFileName + "> " + bkResult);
                    if (bkResult >= 0) {
                        logger.info("File: " + sFileName + " is backup to " + backupDir);
                    } else {
                        totalBackupFail++;
                        logger.info("Fail to backup file <" + sFileName + " now try to unrate file");
                        totalUnrate++;
                        if (unrateFile(sFileName) < 0) {
                            logger.warn("Cannot unrate file: " + sFileName);
                            totalUnrateFail++;
                        } else {
                            logger.info("File: " + sFileName + " is unrated to " + unrateDir);
                        }
                    }
                }
                //20111109 move buStartTime to monitor each file process
                buStartTime = null;
            }
            logTime("Total time import list " + totalFile + " files is ", stTime1);
            logger.info("Total files " + totalFile
                    + " total file ImportSuccess " + totalBackup
                    + " total file ImportFail " + totalImportFail
                    + " total file BackupFail " + totalBackupFail
                    + " total file Unrate " + totalUnrate
                    + " total file UnrateFail " + totalUnrateFail
                    + " now go to sleep  " + delayTime + " ms");
            return 0;
        } catch (Exception ex) {
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            logger.error("Had exception in cycle: " + sw.toString() + " now go to sleep " + delayTime + " ms");
            return -1;
        }
    }

    private int countFileLine(String fileName) {
        long startTime = System.currentTimeMillis();
        int result = 0;
        int readChars = 0;
        byte[] buf = new byte[1024];
        InputStream inStream = null;
        FileInputStream fileStream = null;
        boolean endsWithoutNewLine = false;
        try {
            fileStream = new FileInputStream(fileName);
            inStream = new BufferedInputStream(fileStream);
            while ((readChars = inStream.read(buf)) != -1) {
                for (int i = 0; i < readChars; ++i) {
                    if (buf[i] == '\n') {
                        result++;
                    }
                }
                endsWithoutNewLine = (buf[readChars - 1] != '\n');
            }
            if (endsWithoutNewLine) {
                result++;
            }
        } catch (Exception ex) {
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            logger.error("Had exception in countFileLine: " + sw.toString());
            result = -1;
        } finally {
            try {
                if (fileStream != null) {
                    fileStream.close();
                }
                if (inStream != null) {
                    inStream.close();
                }
            } catch (Exception ex) {
                logger.error("Fail close resource" + ex.toString());
            }
            long totalTime = System.currentTimeMillis() - startTime;
            logger.info("Time to count file: " + fileName + ": " + totalTime + " result " + result);
            return result;
        }
    }

    /*
     * This function is to import each file in list
     */
    private int importProcess(String sFileName) {
        long timeSt = System.currentTimeMillis();
        int result = 0;
        int total = 0;
        boolean eof = false;
        Date fileTime = null;
        String header = "";
        String trailer = "";
        String fileSeq = "";
        int totalRecord = 0;
        long totalPay = 0;
        long totalCom = 0;
        long bankFileInfoId = 0;
        int countDetail = 0;
        ArrayList<BankFileDetail> listDetail = new ArrayList<BankFileDetail>();
        int resultInsertInfo;
        int resultInsertDetail;
        FileReader fileReader = null;
        BufferedReader bufferedReader = null;
        try {
            fileReader = new FileReader(importDir + File.separator + sFileName);
            bufferedReader = new BufferedReader(fileReader);
            logger.info("Start read file: " + sFileName);
            int lineNums = countFileLine(importDir + File.separator + sFileName);
            if (lineNums < 0) {
                logger.warn("Count file line " + lineNums);
                return -1;
            }
            logger.info("Count line in file < " + sFileName + "> " + lineNums);
            while (!eof && total < lineNums) {
                line = bufferedReader.readLine();
                if (line == null) {
                    eof = true;
                } else if (line.trim().length() > 0) {
                    total++;
//                    Check is header
                    if (sFileName.startsWith("MEPS")) {
                        fileTime = new Date();
                        fileSeq = sFileName.substring(sFileName.length() - 10, sFileName.length() - 4);
                        if (line.startsWith("0")) {
                            header = line;
                            logger.info("Header " + header + " in file " + sFileName);
                            bankFileInfoId = db.getSequence("BANK_FILE_INFO_SEQ", sFileName);
                        } else if (line.startsWith("9")) {
                            trailer = line;
                            logger.info("Trailer " + trailer + " in file " + sFileName);
                            totalRecord = Integer.valueOf(trailer.substring(1, 9));
                            totalPay = Long.valueOf(trailer.substring(9, 24));
//                            totalCom = Long.valueOf(trailer.substring(26, 36));
                        } else if (line.startsWith("2")) {
                            logger.debug("Line detail " + total + ": " + line + " in file " + sFileName);
                            BankFileDetail detail = new BankFileDetail();
                            detail.setBankFileInfoId(bankFileInfoId);
                            detail.setFileTime(fileTime);
                            detail.setValuePay(Long.valueOf(line.substring(27, 38)));
                            detail.setReference(line.substring(81, 92));
                            detail.setValueCom(totalCom);
                            detail.setTransId(line.substring(61, 66));
                            detail.setTerminalId(line.substring(51, 61));
                            detail.setTerminalLocation(line.substring(66, 81));
                            listDetail.add(detail);
                            countDetail++;
                        }
                    } else {//for BMEPS file BCI
                        fileTime = df.parse(sFileName.substring(sFileName.length() - 16, sFileName.length() - 4));
                        if (line.startsWith("0")) {
                            header = line;
                            logger.info("Header " + header + " in file " + sFileName);
                            fileSeq = header.substring(header.length() - 5);
                            bankFileInfoId = db.getSequence("BANK_FILE_INFO_SEQ", sFileName);
                        } else if (line.startsWith("9")) {
                            trailer = line;
                            logger.info("Trailer " + trailer + " in file " + sFileName);
                            totalRecord = Integer.valueOf(trailer.substring(1, 8));
                            totalPay = Long.valueOf(trailer.substring(8, 22));
                            totalCom = Long.valueOf(trailer.substring(24, 36));
                        } else if (line.startsWith("1")) {
                            logger.debug("Line detail " + total + ": " + line + " in file " + sFileName);
                            BankFileDetail detail = new BankFileDetail();
                            detail.setBankFileInfoId(bankFileInfoId);
                            detail.setFileTime(fileTime);
                            detail.setReference(line.substring(1, 12));
                            detail.setValuePay(Long.valueOf(line.substring(12, 26)));
                            detail.setValueCom(Long.valueOf(line.substring(28, 42)));
                            detail.setTransId(line.substring(58, 65));
                            detail.setTerminalId(line.substring(65, 75));
                            detail.setTerminalLocation(line.substring(75, 91));
                            listDetail.add(detail);
                            countDetail++;
                        }
                    }
                } else {
                    continue;
                }
            }
            resultInsertInfo = db.insertBankFileInfo(bankFileInfoId, sFileName, fileTime, header, trailer, fileSeq,
                    totalRecord, totalPay, totalCom, importDir, backupDir, unrateDir, "0", "Ok");
            if (resultInsertInfo < 0) {
                logger.error("[Error - importProcess] FileName " + sFileName + " fail to call insertBankFileInfo");
                result = -1;
            } else {
                resultInsertDetail = db.insertBankFileDetail(listDetail, sFileName);
                if (resultInsertDetail < 0) {
                    logger.error("[Error - importProcess] FileName " + sFileName + " fail to call insertBankFileDetail");
                    result = -1;
                }
            }
        } catch (Exception ex) {
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            logger.error("[Error - importProcess] FileName " + sFileName + " detail: " + sw.toString());
            result = -1;
        } finally {
            logTime("Time to importProcess fileName < " + sFileName + "> ", timeSt);
            try {
                if (bufferedReader != null) {
                    bufferedReader.close();
                }
                if (fileReader != null) {
                    fileReader.close();
                }
            } catch (Exception e) {
                logger.warn("Failt to close fileReader or bufferReader fileName " + sFileName + e.toString());
            }
            logger.info("[Total record]:" + total);
            logger.info("[count detail record]:" + countDetail);
            return result;
        }
    }

    /*
     * This function is to backup file after importing successful
     */
    private int backupFile(String sFileName) {
        try {
            File file = new File(importDir + File.separator + sFileName);
            String sDateFormat = "yyyyMMdd";
            String sBackupFolder = backupDir;
            String sBkDir = "";
            /*
             * Create main backup folder
             */
            if (sBackupFolder != null) {
                File backupDir = new File(sBackupFolder);
                if (!backupDir.exists()) {
                    if (createFolder(sBackupFolder) < 0) {
                        logger.error("[Error - backupFile]: " + "Cannot make back up dir!");
                        return -1;
                    }
                }
                /*
                 * Create date backup folder
                 */
                SimpleDateFormat dateForm = new SimpleDateFormat(sDateFormat);
                String sDate = dateForm.format(new Date());
                String sBackupDateDir = sBackupFolder + "/" + sDate;
                File backupDateDir = new File(sBackupDateDir);
                if (!backupDateDir.exists()) {
                    if (createFolder(sBackupDateDir) < 0) {
                        logger.error("[Error - backupFile]: " + "Cannot make back up DAILY dir!");
                        return -1;
                    }
                }
                sBkDir = sBackupFolder + "/" + sDate;
            } else {
                /*
                 * No found backup folder from config Default DAILY style
                 */
                SimpleDateFormat dateForm = new SimpleDateFormat("yyyyMMdd");
                String sDate = dateForm.format(new Date());
                File backupNonDir = new File(sDate);
                if (!backupNonDir.exists()) {
                    if (createFolder(sDate) < 0) {
                        logger.error("[Error - backupFile]: " + "Cannot make non-style back up dir!");
                        return -1;
                    }
                }
                sBkDir = sDate;
            }
            /*
             * Move to backup dir if not delete
             */
            if (!backupType.equals("Delete".toLowerCase())) {
                File destDir = new File(sBkDir);
                String sFileRemove = file.getName();
                if (!file.renameTo(new File(destDir, sFileRemove))) {
                    logger.error("[Error - backupFile]: " + "Cannot move to backup dir!");
                    return -1;
                }
            } /*
             * Delete file with DELETE
             */ else {
                if (!file.delete()) {
                    logger.error("[Error - backupFile]: " + "Cannot delete file!");
                    return -1;
                }
            }
            return 0;
        } catch (Exception ex) {
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            logger.error("Had exception in backupFile: " + sw.toString());
            return -1;
        }
    }

    /*
     * This function is to move file to unrated dir when error appear while
     * importing
     */
    private int unrateFile(String sFileName) {
        try {
            File dir = new File(unrateDir);
            File fileName = new File(importDir + File.separator + sFileName);
            boolean success = false;
            String fileNameToRemove = fileName.getName();
            if (fileName.isFile()) {
                success = fileName.renameTo(new File(dir, fileNameToRemove));
                if (success) {
                    return 0;
                } else {
                    return -1;
                }
            } else {
                logger.warn("The " + sFileName + " is not a file, so not unrate");
                return 0;
            }
        } catch (Exception ex) {
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            logger.error("Had exception in unrateFile fileName <" + sFileName + "> " + sw.toString());
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

    @Override
    protected void prepareStart() {
        try {
            logger.info("Prepare starting");
            delayTime = Integer.parseInt(ResourceBundle.getBundle("cfgBankFile").getString("delayTime"));
            unrateDir = ResourceBundle.getBundle("cfgBankFile").getString("unrateDir");
            importDir = ResourceBundle.getBundle("cfgBankFile").getString("importDir");
            backupDir = ResourceBundle.getBundle("cfgBankFile").getString("backupDir");
            tempDir = ResourceBundle.getBundle("cfgBankFile").getString("tempDir");
            backupType = ResourceBundle.getBundle("cfgBankFile").getString("backupType");
            fileWildcard = ResourceBundle.getBundle("cfgBankFile").getString("fileWildcard");
            if (!fileWildcard.equals("")) {
                standard = (fileWildcard + "*a").split("\\*");
            }
            db = new DbImportBankFile("dbImportBank", logger);
        } catch (Exception ex) {
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            logger.error("Had exception in prepareStart: " + sw.toString() + " now must stop");
            stop();
        }
    }

    private String getValueByIndex(String input, int startIndex, int endIndex, String fileName) {
        String strValue = "";
        try {
            if (input == null || input.trim().length() <= 0) {
                return strValue;
            } else {
                strValue = input.substring(startIndex, endIndex);
            }
            return strValue;
        } catch (Exception ex) {
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            logger.error("Had exception in getValueByIndex with file name <"
                    + fileName + "> " + " input " + input + " " + sw.toString());
            return strValue;
        }
    }

    @Override
    protected void process() {
        try {
            cycle();
            int delayTime1 = delayTime > 0
                    ? delayTime : 1000;
            Thread.sleep(delayTime1);
        } catch (Exception ex) {
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            logger.error("Had exception in process: " + sw.toString());
        }
    }

    @Override
    public void stop() {
        logger.info("[System]: ImportThread is stopped ");
        super.stop();
    }

    @Override
    public void start() {
        logger.info("[System]: ImportThread is started ");
        super.start();
    }

    public int checkFileName(String fileName) {
        try {
            if (fileName == null) {
                return -1;
            }
            if (standard.length > 2) {
                for (int i = 1; i < standard.length - 1; i++) {
                    String check = standard[i];
                    if (!check.equals("")) {
                        if (fileName.toLowerCase().indexOf(check.toLowerCase()) < 0) {
                            return -1;
                        }
                    }
                }
                /*
                 * Check start
                 */
                if (!standard[0].equals("")) {
                    if (standard[0].contains("|")) {
                        String[] temp = standard[0].split("\\|");
                        boolean ck = false;
                        for (int j = 0; j < temp.length; j++) {
                            if (fileName.toLowerCase().startsWith(temp[j].toLowerCase())) {
                                ck = true;
                                break;
                            }
                        }
                        if (!ck) {
                            return -1;
                        }
                    } else {
                        if (!fileName.toLowerCase().startsWith(standard[0].toLowerCase())) {
                            return -1;
                        }
                    }
                }
                /*
                 * Check end
                 */
                if (!standard[standard.length - 2].equals("")) {
                    if (!fileName.toLowerCase().endsWith(standard[standard.length - 2].toLowerCase())) {
                        return -1;
                    }
                }
            } else {
                if (!fileName.toLowerCase().equals(standard[0].toLowerCase())) {
                    return -1;
                }
            }
            return 0;
        } catch (Exception ex) {
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            logger.error("Had exception in checkFileName file " + fileName + " " + sw.toString());
            return -1;
        }
    }

    public void logTime(String strLog, long timeSt) {
        long timeEx = System.currentTimeMillis() - timeSt;
        logger.info(strLog + ": " + timeEx + " ms");
    }

    public static void main(String[] args) {
        try {
            ImportBankFile importer = new ImportBankFile("TestImport");
            importer.start();
        } catch (Exception e) {
            System.out.println("Have error when init ImportFileThread " + e.toString());
        }
    }
}
