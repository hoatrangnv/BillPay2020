/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.channel.manage;

import com.itextpdf.text.PageSize;
import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbUpdateContractProcessor;
import com.viettel.paybonus.obj.RequestChannel;
import com.viettel.paybonus.service.Exchange;
import com.viettel.paybonus.service.PdfBussinessDAL;
import com.viettel.threadfw.manager.AppManager;
import java.util.List;
import org.apache.log4j.Logger;
import com.viettel.threadfw.process.ProcessRecordAbstract;
import com.viettel.vas.util.ExchangeClientChannel;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import org.apache.commons.net.ftp.FTPClient;

/**
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class UpdateContract extends ProcessRecordAbstract {

    Exchange pro;
    DbUpdateContractProcessor db;
    String path;

    public UpdateContract() {
        super();
        logger = Logger.getLogger(UpdateContract.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
        db = new DbUpdateContractProcessor("dbsm", logger);
        path = System.getProperty("user.dir").replace("/bin", "");
        logger.info("Path contains temp file is: " + path);
    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        return listRecord;
    }

    @Override
    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
        List<Record> listResult = new ArrayList<Record>();

        for (Record record : listRecord) {
            RequestChannel bn = (RequestChannel) record;
            listResult.add(bn);
            if ("0".equals(bn.getResultCode())) {
                logger.info("Step 1: Create channel first... " + bn.getStaffId() + " id " + bn.getId());
                Date sysdate = new Date();
//                Create contract...
                String HOST_FTP = "10.229.42.55";
                String USER = "scan_doc";
                String PASS = "8dw29Jk$3d";
                logger.info("HOST: " + HOST_FTP + "\nUSER: " + USER + "\nPASS: " + PASS);
                FTPClient ftpClient = new FTPClient();
                ftpClient.connect(HOST_FTP);
                ftpClient.login(USER, PASS);
                ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);// Used for
                ftpClient.setFileTransferMode(FTPClient.BINARY_FILE_TYPE);
                ftpClient.enterLocalPassiveMode();
                ftpClient.setSoTimeout(30000); // default 1
                ftpClient.setDataTimeout(30000); // default 1
                int reply = ftpClient.getReplyCode();
                logger.info("ReplyCode from FTP server: " + reply);

                SimpleDateFormat sdf = new SimpleDateFormat("yyMMdd");
                String dateDir = sdf.format(sysdate);

//                String frontBI = "D:\\Restart\\ThreadContainer\\etc\\tmpImage\\" + bn.getImgUrl1();
//                String backBI = "D:\\Restart\\ThreadContainer\\etc\\tmpImage\\" + bn.getImgUrl2();
//                String signBI = "D:\\Restart\\ThreadContainer\\etc\\tmpImage\\" + bn.getImgUrl3();

                String frontBI = path + "/etc/tmpImage/" + bn.getImgUrl1();
                String backBI = path + "/etc/tmpImage/" + bn.getImgUrl2();
                String signBI = path + "/etc/tmpImage/" + bn.getImgUrl3();

                String dirImg = bn.getImgPath().split("\\/")[1];
                FileOutputStream fos = new FileOutputStream(frontBI);
                ftpClient.retrieveFile("/mBCCS_CONTRACT_CHANNEL_IMG/" + dirImg + "/" + bn.getImgUrl1(), fos);

                fos = new FileOutputStream(backBI);
                ftpClient.retrieveFile("/mBCCS_CONTRACT_CHANNEL_IMG/" + dirImg + "/" + bn.getImgUrl2(), fos);

                fos = new FileOutputStream(signBI);
                ftpClient.retrieveFile("/mBCCS_CONTRACT_CHANNEL_IMG/" + dirImg + "/" + bn.getImgUrl3(), fos);

                fos.close();

                ftpClient.changeWorkingDirectory("AUTO_GEN_CONTRACT_POS");
                if (!dateDir.isEmpty()) {
                    ftpClient.makeDirectory(dateDir);
                    ftpClient.changeWorkingDirectory(dateDir);
                }

                com.itextpdf.text.Document documentPos = new com.itextpdf.text.Document(PageSize.A4);
                SimpleDateFormat sdf1 = new SimpleDateFormat("yyyyMMddHH24mmss");
                Date createTimeContract = sysdate;
                String posContractName = sdf1.format(createTimeContract) + "_" + bn.getStaffCode() + "_POS_Contract.pdf";
                logger.info("Begin generate contract for channel...contractName: " + posContractName);
//                PdfBussinessDAL pdfReg = new PdfBussinessDAL("D:\\Restart\\ThreadContainer\\etc\\tmpUpload\\" + posContractName, documentPos);
                PdfBussinessDAL pdfReg = new PdfBussinessDAL(path + "/etc/tmpUpload/" + posContractName, documentPos);
                SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMdd");
                Long contractNo = db.getSequence("CONTRACT_NO_SEQ", "dbsm");
                pdfReg.createContractRegistrationPoint(bn, sdf2.format(sysdate).substring(0, 4) + "-" + String.format("%04d", contractNo), sdf2.format(sysdate), frontBI, backBI,
                        signBI, ftpClient, path);

                String emolaContractName = "";
                if (bn.getChannelWallet() != null
                        && bn.getIsdnWallet() != null) {
                    emolaContractName = sdf1.format(createTimeContract) + "_" + bn.getStaffCode() + "_eMola_Contract.pdf";
                    logger.info("Begin generate emola contract for channel...contractName: " + emolaContractName);
                    com.itextpdf.text.Document documentEmola = new com.itextpdf.text.Document(PageSize.A4);
//                    PdfBussinessDAL pdf = new PdfBussinessDAL("D:\\Restart\\ThreadContainer\\etc\\tmpUpload\\" + emolaContractName, documentEmola);
                    PdfBussinessDAL pdf = new PdfBussinessDAL(path + "/etc/tmpUpload/" + emolaContractName, documentEmola);
                    pdf.createEMolaContract(bn, sdf2.format(sysdate), signBI, ftpClient, path);
                }

//                File fileFrontBI = new File("D:\\Restart\\ThreadContainer\\etc\\tmpImage\\" + bn.getImgUrl1());
//                File fileBackBI = new File("D:\\Restart\\ThreadContainer\\etc\\tmpImage\\" + bn.getImgUrl2());
//                File fileSignBI = new File("D:\\Restart\\ThreadContainer\\etc\\tmpImage\\" + bn.getImgUrl3());

                File fileFrontBI = new File(path + "/etc/tmpImage/" + bn.getImgUrl1());
                File fileBackBI = new File(path + "/etc/tmpImage/" + bn.getImgUrl2());
                File fileSignBI = new File(path + "/etc/tmpImage/" + bn.getImgUrl3());
                fileFrontBI.delete();
                fileBackBI.delete();
                fileSignBI.delete();
                //Step 3: Update path contract 
                String path = "/u01/scan_doc/AUTO_GEN_CONTRACT_POS/" + dateDir + "/" + posContractName;
                if (emolaContractName.length() > 0) {
                    path = path + "|" + "/u01/scan_doc/AUTO_GEN_CONTRACT_POS/" + dateDir + "/" + emolaContractName;
                }
                db.updateContractPath(path, bn.getStaffCode());

                if (ftpClient.isConnected()) {
                    try {
                        ftpClient.logout();
                        ftpClient.disconnect();
                    } catch (IOException f) {
                        f.printStackTrace();
                    }
                }
                bn.setStatus(1L);
                bn.setContractStatus(1L);
                bn.setDescription("Generate contract for channel successfully. Channel Code: " + bn.getStaffCode());
            } else {
                logger.warn("After validate respone code is fail actionId " + bn.getId()
                        + " so continue with other transaction");
                continue;
            }
        }
        listRecord.clear();
        return listResult;
    }

    @Override
    public void printListRecord(List<Record> listRecord) throws Exception {
        StringBuilder br = new StringBuilder();
        br.setLength(0);
        br.append("\r\n").
                append("|\tID|").
                append("|\tREQUEST_USER_ISDN\t|").
                append("|\tSTAFF_ID\t|").
                append("|\tCHANNEL_NAME\t|").
                append("|\tCHANNEL_ISDN\t|").
                append("|\tCHANNEL_PREFIX\t|").
                append("|\tCHANNEL_WALLET\t|").
                append("|\tISDN_WALLET\t|").
                append("|\tDISTRICT\t|").
                append("|\tPRECINCT\t|").
                append("|\tLAST_UPDATE_KEY\t|");
        for (Record record : listRecord) {
            RequestChannel bn = (RequestChannel) record;
            br.append("\r\n").
                    append("|\t").
                    append(bn.getId()).
                    append("||\t").
                    append(bn.getRequestUserIsdn()).
                    append("||\t").
                    append(bn.getStaffId()).
                    append("||\t").
                    append(bn.getChannelName()).
                    append("||\t").
                    append(bn.getChannelIsdn()).
                    append("||\t").
                    append(bn.getChannelPrefix()).
                    append("||\t").
                    append(bn.getChannelWallet()).
                    append("||\t").
                    append(bn.getIsdnWallet()).
                    append("||\t").
                    append(bn.getDistrict()).
                    append("||\t").
                    append(bn.getPrecint()).
                    append("||\t").
                    append(bn.getLastUpdateKey());
        }
        logger.info(br);
    }

    @Override
    public List<Record> processException(List<Record> listRecord, Exception ex) {
        logger.warn("TEMPLATE process exception record: " + ex.toString());
//        for (Record record : listRecord) {
//            logger.info("TEMPLATE let convert to recort type you want and then set errCode, errDesc at here");
////            MoRecord moRecord = (MoRecord) record;
////            moRecord.setMessage("Thao tac that bai!");
////            moRecord.setErrCode("-5");
//        }
        return listRecord;
    }

    @Override
    public boolean startProcessRecord() {
        return true;
    }
}
