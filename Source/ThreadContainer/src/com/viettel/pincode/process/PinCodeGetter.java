/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.pincode.process;

import com.viettel.paybonus.process.*;
import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbPinCodeGetter;
import com.viettel.paybonus.obj.PinCode;
import com.viettel.paybonus.service.EmailUtils;
import com.viettel.paybonus.service.ZipUtils;
import com.viettel.security.PassTranformer;
import java.util.List;
import org.apache.log4j.Logger;
import com.viettel.threadfw.process.ProcessRecordAbstract;
import encrypta.Encrypta;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;

/**
 * @author LinhNBV
 */
public class PinCodeGetter extends ProcessRecordAbstract {

    DbPinCodeGetter db;
    SimpleDateFormat sdf;
    SimpleDateFormat sdf2;
    String outputBaseFolderPinCode;
    String pincodeIsdnReceiveNotification;
    String[] arrPinCodeIsdnReceiverNotification;
    String pinCodeAgentReceiveNewFormatFile;
    String[] arrAgent;
    String pinCodeEncryptFollowBank;
    String[] arrAgentEncrypt;
    ArrayList<HashMap> lstAgentEncrypt;

    public PinCodeGetter() {
        super();
        logger = Logger.getLogger(LimitControl.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        db = new DbPinCodeGetter("dbsm", logger);
        sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        sdf2 = new SimpleDateFormat("yyyyMMdd");
        outputBaseFolderPinCode = ResourceBundle.getBundle("configPayBonus").getString("pincodeOutputFolder");
        pincodeIsdnReceiveNotification = ResourceBundle.getBundle("configPayBonus").getString("pincodeIsdnReceiveNotification");
        arrPinCodeIsdnReceiverNotification = pincodeIsdnReceiveNotification.split("\\|");
        pinCodeAgentReceiveNewFormatFile = ResourceBundle.getBundle("configPayBonus").getString("pinCodeAgentReceiveNewFormatFile");
        arrAgent = pinCodeAgentReceiveNewFormatFile.split("\\|");
        pinCodeEncryptFollowBank = ResourceBundle.getBundle("configPayBonus").getString("pinCodeEncryptFollowBank");
        arrAgentEncrypt = pinCodeEncryptFollowBank.split("\\|");
        lstAgentEncrypt = new ArrayList<HashMap>();
        for (String tmp : arrAgentEncrypt) {
            String[] arrTmp = tmp.split("\\:");
            HashMap map = new HashMap();
            map.put(arrTmp[0].trim(), arrTmp[1].trim());
            lstAgentEncrypt.add(map);
        }
    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        for (Record record : listRecord) {
            PinCode moRecord = (PinCode) record;
            moRecord.setNodeName(holder.getNodeName());
            moRecord.setClusterName(holder.getClusterName());
        }
        return listRecord;
    }

    @Override
    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
        List<Record> listResult = new ArrayList<Record>();
        HashMap<Integer, PrintWriter> mapOutputFileId = new HashMap<Integer, PrintWriter>();
        String outputFolderPinCode = null;
        boolean isNewFormat;
        boolean isEncrypt;
        Encrypta encryptBank = null;
        for (Record record : listRecord) {
            PinCode bn = (PinCode) record;
            isNewFormat = false;
            isEncrypt = false;
            outputFolderPinCode = outputBaseFolderPinCode;
            listResult.add(bn);
            if ("0".equals(bn.getResultCode())) {
                logger.info("Step 1: Get information of order PinCode, type of Ecard, quantiy, from serial to serial, "
                        + "saleTransOrderId: " + bn.getSaleTransOrderId());
                long saleTransId = db.getSaleTransId(bn.getSaleTransOrderId(), "sale_trans_order");
                if (saleTransId <= 0) {
                    logger.info("Can not find saleTransId, continue find on table agent_trans_order_his, "
                            + "saleTransOrderId: " + bn.getSaleTransOrderId());
                    saleTransId = db.getSaleTransId(bn.getSaleTransOrderId(), "agent_trans_order_his");
                }
                if (saleTransId <= 0) {
                    logger.info("Can not find saleTransId, stop process, "
                            + "saleTransOrderId: " + bn.getSaleTransOrderId());
                    bn.setResultCode("E01");
                    bn.setDescription("Can not find saleTransId.");
                    continue;
                }
                String emailInfo = db.getEmailPassword(bn.getReceiverId());
                if (emailInfo.isEmpty() || emailInfo.length() <= 0) {
                    logger.info("Agent don't have information about email address and password of zip file, "
                            + "saleTransOrderId: " + bn.getSaleTransOrderId() + ", receiverId: " + bn.getReceiverId());
                    bn.setResultCode("E03");
                    bn.setDescription("Agent don't have information about email address and password.");
                    continue;
                }
                String[] arrEmailPasswordInfo = emailInfo.split("\\|");
                if (arrEmailPasswordInfo.length != 4) {
                    logger.info("Length of array email not enough, "
                            + "saleTransOrderId: " + bn.getSaleTransOrderId() + ", receiverId: " + bn.getReceiverId());
                    bn.setResultCode("E04");
                    bn.setDescription("Length of array email not enough.");
                    continue;
                }
                String email = "", passwordLv3 = "";//passwordLv1 = "" , passwordLv2 = "",
                try {
                    String emailDecrypt = PassTranformer.decrypt(arrEmailPasswordInfo[0]);
                    String[] arrEmailDecrypt = emailDecrypt.split("\\#");
                    email = arrEmailDecrypt[0] + "@" + arrEmailDecrypt[1].split("\\@")[1];
//                    String passLv1Decrypt = PassTranformer.decrypt(arrEmailPasswordInfo[1]);
//                    String[] arrPassLv1Decrypt = passLv1Decrypt.split("\\_");
//                    passwordLv1 = arrPassLv1Decrypt[0];

//                    String passLv2Decrypt = PassTranformer.decrypt(arrEmailPasswordInfo[2]);
//                    String[] arrPassLv2Decrypt = passLv2Decrypt.split("\\_");
//                    passwordLv2 = arrPassLv2Decrypt[0];

                    String passLv3Decrypt = PassTranformer.decrypt(arrEmailPasswordInfo[3]);
                    String[] arrPassLv3Decrypt = passLv3Decrypt.split("\\_");
                    passwordLv3 = arrPassLv3Decrypt[0];
                } catch (Exception ex) {
                    email = "";
//                    passwordLv1 = "";
//                    passwordLv2 = "";
                    passwordLv3 = "";
                    ex.printStackTrace();
                    logger.info("Exception when decrypt email and password..., saleTransOrderId: " + bn.getSaleTransOrderId() + ", receiverId: " + bn.getReceiverId());
                    bn.setResultCode("E07");
                    bn.setDescription("Exception when decryp email and password.");
                    continue;
                }
                if (email.isEmpty() || passwordLv3.isEmpty()) {// || passwordLv2.isEmpty() || passwordLv3.isEmpty()
                    bn.setResultCode("E08");
                    bn.setDescription("Email and password after decrypt is empty.");
                    continue;
                }
                for (String tmpReceiverId : arrAgent) {
                    if (String.valueOf(bn.getReceiverId()).equalsIgnoreCase(tmpReceiverId)) {
                        logger.info("Agent in list receive new format file pincode, "
                                + "saleTransOrderId: " + bn.getSaleTransOrderId() + ", receiverId: " + bn.getReceiverId());
                        isNewFormat = true;
                        break;
                    }
                }
                for (int i = 0; i < lstAgentEncrypt.size(); i++) {
                    if (lstAgentEncrypt.get(i).containsKey(String.valueOf(bn.getReceiverId()))) {
                        logger.info("Receiver: " + bn.getReceiverId() + ", have config ecrypt follow algorithm of bank: "
                                + lstAgentEncrypt.get(i).get(String.valueOf(bn.getReceiverId())) + ", saleTransOrderId: " + bn.getSaleTransOrderId());
                        isEncrypt = true;
                        encryptBank = new Encrypta();
                        break;
                    }
                }
                List<PinCode> listSerialPinCode = db.getListSerialPinCode(saleTransId);
                if (listSerialPinCode.isEmpty()) {
                    bn.setResultCode("E02");
                    bn.setDescription("Can not find list serial of order Pincode.");
                    continue;
                }
                //Make folder saleTransOrderId
                File file = new File(outputFolderPinCode + bn.getSaleTransOrderId());
                if (file.mkdir()) {
                    logger.info("Make directory for order success, "
                            + "saleTransOrderId: " + bn.getSaleTransOrderId());
                } else {
                    logger.info("Make directory for order fail, "
                            + "saleTransOrderId: " + bn.getSaleTransOrderId());
                }
                outputFolderPinCode = outputFolderPinCode + bn.getSaleTransOrderId() + "/";
                logger.info("outputFolderPinCode: " + outputFolderPinCode
                        + " ,saleTransOrderId: " + bn.getSaleTransOrderId());
                int sequenceFile = 1;
                for (int i = 0; i < listSerialPinCode.size(); i++) {
                    PinCode pinCode = listSerialPinCode.get(i);
                    logger.info("Step 2: Start make file follow order pincode, stockModelCode: " + pinCode.getStockModelCode() + ", quantity: "
                            + pinCode.getQuantity() + ", fromSerial: " + pinCode.getFromSerial() + ", toSerial: " + pinCode.getToSerial()
                            + "saleTransId: " + saleTransId);
                    String tableName = pinCode.getStockModelCode().split("\\_")[0];
                    ArrayList<PinCode> lstPincode = db.getPinCodeBySerial(pinCode.getFromSerial(), pinCode.getToSerial(), tableName);
                    if (lstPincode.isEmpty()) {
                        logger.info("List PinCode is empty, stockModelCode: " + pinCode.getStockModelCode() + ", quantity: "
                                + pinCode.getQuantity() + ", fromSerial: " + pinCode.getFromSerial() + ", toSerial: " + pinCode.getToSerial()
                                + "saleTransId: " + saleTransId);
                        bn.setResultCode("E05");
                        bn.setDescription("List PinCode is empty, fromSerial: " + pinCode.getFromSerial() + ", toSerial: " + pinCode.getToSerial()
                                + "saleTransId: " + saleTransId);
                        file.delete();
                    }
                    if (lstPincode.size() != pinCode.getQuantity()) {
                        logger.warn("Pincode and quantity not match, stockModelCode: " + pinCode.getStockModelCode() + ", quantity: "
                                + pinCode.getQuantity() + ", fromSerial: " + pinCode.getFromSerial() + ", toSerial: " + pinCode.getToSerial()
                                + "saleTransId: " + saleTransId + ", size: " + lstPincode.size());
                        bn.setResultCode("E06");
                        bn.setDescription("Pincode and quantity not match, fromSerial: " + pinCode.getFromSerial() + ", toSerial: " + pinCode.getToSerial()
                                + "saleTransId: " + saleTransId);
                        break;
                    }
                    String endDate = "", cardType = "";
                    int faceValue = 0;
                    for (int j = 0; j < lstPincode.size(); j++) {
                        PrintWriter print = mapOutputFileId.get(i);
                        if (print == null) {
                            if (isEncrypt) {
                                logger.info("Receiver: " + bn.getReceiverId() + ", have config encrypt follow algorithm of bank, saleTransOrderId: " + bn.getSaleTransOrderId());
                                String header = lstPincode.get(j).getHeader();
                                cardType = header.substring(header.indexOf("FaceValue"), header.indexOf("StartDate")).trim().split("\\:")[1].trim();
                                faceValue = Integer.parseInt(cardType);
                                while (cardType.length() < 4) {
                                    cardType = "0" + cardType;
                                }
                                cardType = "M" + cardType;
                                endDate = header.substring(header.indexOf("StopDate"), header.indexOf("Currency")).trim().split("\\:")[1].trim();
                                String path = outputFolderPinCode + cardType + "_" + sdf2.format(new Date()) + "_" + i + "_MOVITEL_1.txt";
                                FileWriter outFile = new FileWriter(path, false);
                                PrintWriter out = new PrintWriter(outFile);
                                mapOutputFileId.put(i, out);
                                String strSequenceFile = String.valueOf(sequenceFile);
                                while (strSequenceFile.length() < 4) {
                                    strSequenceFile = "0" + strSequenceFile;
                                }
                                String firstLine = "0" + sdf2.format(new Date()) + strSequenceFile;
                                out.println(firstLine);
                                ++sequenceFile;
                                logger.info("First line: " + firstLine + ", start write list pincode encrypt...saleTransOrderId: " + bn.getSaleTransOrderId());
                                if (encryptBank != null) {
                                    out.println("1" + endDate + encryptBank.Encrypta_File("00" + PassTranformer.decrypt(lstPincode.get(j).getPincode()), "MOVITEL") + "000" + lstPincode.get(j).getSerial() + cardType);
                                }
                            } else {
                                logger.info("Receiver: " + bn.getReceiverId() + ", make file pincode normal, no need encrypt, saleTransOrderId: " + bn.getSaleTransOrderId());
                                String path = outputFolderPinCode + tableName + "_" + sdf.format(new Date()) + "_" + i + ".txt";
                                FileWriter outFile = new FileWriter(path, false);
                                PrintWriter out = new PrintWriter(outFile);
                                mapOutputFileId.put(i, out);
                                String header = lstPincode.get(j).getHeader();
                                String preHeader = header.substring(0, header.indexOf("Quantity"));
                                String tailHeader = header.substring(header.indexOf("CardPrefix"), header.length());
                                StringBuilder dr = new StringBuilder();
                                header = dr.append(preHeader).append(tailHeader).toString();
                                String preSequence = header.substring(0, header.indexOf("Start_Sequence"));
                                String tailSequence = header.substring(header.indexOf("[BEGIN]"), header.length());
                                dr.setLength(0);
                                dr.append(preSequence).append(tailSequence).toString();
                                if (isNewFormat) {
                                    String newHeader = lstPincode.get(j).getHeader();
                                    String strQuantity = newHeader.substring(newHeader.indexOf("Quantity"), newHeader.indexOf("CardPrefix")).trim();
                                    String[] arrQuantity = strQuantity.split("\\:");
                                    String tmpQuantity = arrQuantity[0] + ":" + pinCode.getQuantity();
                                    newHeader = newHeader.replace(strQuantity, tmpQuantity);
                                    String strSequence = newHeader.substring(newHeader.indexOf("Start_Sequence"), newHeader.indexOf("[BEGIN]")).trim();
                                    String[] arrSequence = strSequence.split("\\:");
                                    String tmpSequence = arrSequence[0] + ":" + pinCode.getFromSerial();
                                    newHeader = newHeader.replace(strSequence, tmpSequence);
                                    out.println(newHeader);
                                } else {
                                    out.println(dr.toString());
                                }
                                out.println(lstPincode.get(j).getSerial() + " " + PassTranformer.decrypt(lstPincode.get(j).getPincode()));
                            }
                        } else {
                            if (isEncrypt) {
                                if (encryptBank != null) {
                                    print.println("1" + endDate + encryptBank.Encrypta_File("00" + PassTranformer.decrypt(lstPincode.get(j).getPincode()), "MOVITEL") + "000" + lstPincode.get(j).getSerial() + cardType);
                                }
                            } else {
                                print.println(lstPincode.get(j).getSerial() + " " + PassTranformer.decrypt(lstPincode.get(j).getPincode()));
                            }

                        }
                    }
                    if (isEncrypt) {
                        String strQuantity = String.valueOf(pinCode.getQuantity());
                        while (strQuantity.length() < 8) {
                            strQuantity = "0" + strQuantity;
                        }
                        String strTotalAmount = String.valueOf(pinCode.getQuantity() * faceValue);
                        while (strTotalAmount.length() < 14) {
                            strTotalAmount = "0" + strTotalAmount;
                        }
                        String endLine = "9" + strQuantity + strTotalAmount;
                        PrintWriter print = mapOutputFileId.get(i);
                        if (print != null) {
                            print.println(endLine);
                        }
                    }
                }
                if ("E05".equalsIgnoreCase(bn.getResultCode()) || "E06".equalsIgnoreCase(bn.getResultCode())) {
                    file.delete();
                    continue;
                }
                for (Map.Entry<Integer, PrintWriter> entry : mapOutputFileId.entrySet()) {
                    PrintWriter value = entry.getValue();
                    if (!isEncrypt) {
                        value.println("[END]");
                    }

                    value.flush();
                    value.close();
                }
                mapOutputFileId.clear();

                logger.info("Step 3: Compress file to zip and set password, saleTransId: " + saleTransId);
                ZipUtils.compressWithPassword(outputFolderPinCode, passwordLv3, bn.getSaleTransOrderId() + "");
//                ZipUtils.compressWithPassword(outputFolderPinCode, passwordLv2, bn.getSaleTransOrderId() + "_2");
//                File[] lstFile = file.listFiles();
//                for (File tmpFile : lstFile) {
//                    if (("order_pincode_" + bn.getSaleTransOrderId() + "_1.zip").equals(tmpFile.getName())) {
//                        boolean rs = tmpFile.delete();
//                        logger.info("Step 3.1: Start delete tmp first zip file, saleTransId: " + saleTransId + ", result: " + rs);
//                    }
//                }
//                ZipUtils.compressWithPassword(outputFolderPinCode, passwordLv3, String.valueOf(bn.getSaleTransOrderId()));
//                lstFile = file.listFiles();
//                for (File tmpFile : lstFile) {
//                    if (("order_pincode_" + bn.getSaleTransOrderId() + "_2.zip").equals(tmpFile.getName())) {
//                        boolean rs = tmpFile.delete();
//                        logger.info("Step 3.2: Start delete tmp first zip file, saleTransId: " + saleTransId + ", result: " + rs);
//                    }
//                }
                String amountTax = db.getAmountTaxOfOrder(saleTransId);

                logger.info("Step 4: Send email, saleTransId: " + saleTransId);
                HashMap<String, String> param = new HashMap();
                //config email...
                param.put("EMAIL_SSL", "YES");
                param.put("EMAIL_KEYSTORE_PASSWORD", "45037153e4312bba");
                param.put("EMAIL_HOST", "125.235.240.36");
                param.put("EMAIL_PORT", "465");
                param.put("EMAIL_ADDRESS", "pincode@movitel.co.mz");
                param.put("EMAIL_PASSWORD", "Movitel@2019##");
//                param.put("EMAIL_ADDRESS", "payment_notification@movitel.co.mz");
//                param.put("EMAIL_PASSWORD", "Movitel@2018");
                param.put("EMAIL_KEYSTORE_FILE", "");
                param.put("EMAIL_ATTACHMENT_FILE", "");

                param.put("SEND_EMAIL", email);
                param.put("EMAIL_SUBJECT", "Movitel Order Pincode Nr." + bn.getSaleTransOrderId());
                param.put("EMAIL_CONTENT", "Caro Parceiro,\n"
                        + "<br>\n"
                        + "<br/>"
                        + "Receba o email Pin code referente a ordem Nr." + bn.getSaleTransOrderId() + " de (" + amountTax + " MT), criado no dia " + bn.getCreateTime() + ".\n"
                        + "<br>\n"
                        + "<br/>"
                        + "<br>\n"
                        + "<i>\n"
                        + "&nbsp;&nbsp;&nbsp;&nbsp;Contacto: 860168168<br/>\n"
                        + "&nbsp;&nbsp;&nbsp;&nbsp;Email: pincode@movitel.co.mz<br/>\n"
                        + "</i>\n"
                        + "<br/><br/>\n"
                        + "Obrigado por usar os nossos servicos!");
                param.put("FILE_PATH", outputFolderPinCode + "order_pincode_" + bn.getSaleTransOrderId() + ".zip");

                String kq = EmailUtils.sendInformEmail(param);
                this.logger.info("Send Email To:" + (String) param.get("SEND_EMAIL") + ", subject: " + (String) param.get("EMAIL_SUBJECT"));
                if (!kq.equals("0")) {
                    logger.error("Send Email is Error: " + kq);
                } else {
                    logger.info("Send Email is Success, send sms to agent...");
//                    Date sysdate = new Date();
//                    SimpleDateFormat sdfOrder = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
                    String agentCode = "";
                    String agentName = "";
                    String agentOrderInfo = db.getAgentInfo(bn.getReceiverId());
                    if (agentOrderInfo.length() > 0) {
                        String[] arrAgentInfo = agentOrderInfo.split("\\|");
                        if (arrAgentInfo.length == 2) {
                            agentCode = arrAgentInfo[0];
                            agentName = arrAgentInfo[1];
                        }
                    }

                    String message = "Ordem Nr." + bn.getSaleTransOrderId() + " (" + amountTax + " MT) em " + bn.getCreateTime() + " para " + agentCode + " – " + agentName + " De Recargas Eletronicas foi enviado por email. Por favor, verifique a Caixa de email. Obrigado!";
                    for (String isdn : arrPinCodeIsdnReceiverNotification) {
                        db.sendSms(isdn, message, "86952");
                    }
                    String telAgent = db.getTelByStaffCode(agentCode);
                    db.sendSms(telAgent, message, "86952");

                }

                bn.setResultCode("0");
                bn.setDescription("Successful.");

            } else {
                logger.warn("After validate respone code is fail saleTransOrderId " + bn.getSaleTransOrderId()
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
                append("|\tSALE_TRANS_ORDER_ID\t|").
                append("|\tRECEIVER_ID\t|").
                append("|\tCREATE_STAFF_ID");
        for (Record record : listRecord) {
            PinCode bn = (PinCode) record;
            br.append("\r\n").
                    append("|\t").
                    append(bn.getSaleTransOrderId()).
                    append("||\t").
                    append(bn.getReceiverId()).
                    append("||\t").
                    append(bn.getCreateStaffId());
        }
        logger.info(br);
    }

    @Override
    public List<Record> processException(List<Record> listRecord, Exception ex) {
        ex.printStackTrace();
        logger.warn("TEMPLATE process exception record: " + ex.toString());
        return listRecord;
    }

    @Override
    public boolean startProcessRecord() {
        return true;
    }
}
