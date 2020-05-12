/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbChangeSimPostPaid4GProcessor;
import com.viettel.paybonus.obj.RequestChangeSim;
import com.viettel.paybonus.obj.SubInfo;
import com.viettel.paybonus.service.Exchange;
import com.viettel.threadfw.manager.AppManager;
import java.util.List;
import org.apache.log4j.Logger;
import com.viettel.threadfw.process.ProcessRecordAbstract;
import com.viettel.vas.util.ExchangeClientChannel;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.ResourceBundle;

/**
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class ChangeSimPostPaid4G extends ProcessRecordAbstract {

    Exchange pro;
    DbChangeSimPostPaid4GProcessor db;
    String strReasonId;
    String lstIsdnReceiveError;
    String[] arrIsdnReceiveError;
    String changeSimSucessfully;
    SimpleDateFormat sdf2 = new SimpleDateFormat("dd/MM/yyyy");
    String smsProfileInvalid;
    String smsSimInfoInvalid;
    String smsCusInfoInvalid;
    String smsSubNotAcitive;
    String smsProductCodeInvalid;
    String smsSimNotSale;
    String smsSimSerialEmpty;
    String smsChangeSimFailed;
    String smsSim4GAlreadyUsed;
    String smsProfileInvalidPT;
    String smsSimInfoInvalidPT;
    String smsCusInfoInvalidPT;
    String smsSubNotAcitivePT;
    String smsProductCodeInvalidPT;
    String smsSimNotSalePT;
    String smsSimSerialEmptyPT;
    String smsChangeSimFailedPT;
    String smsSim4GAlreadyUsedPT;
    Long k4sNO;

    public ChangeSimPostPaid4G() {
        super();
        logger = Logger.getLogger(ChangeSimPostPaid4G.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
        db = new DbChangeSimPostPaid4GProcessor();
        strReasonId = ResourceBundle.getBundle("configPayBonus").getString("changeSimReasonId");
        lstIsdnReceiveError = ResourceBundle.getBundle("configPayBonus").getString("lstIsdnReceiveError");
        arrIsdnReceiveError = lstIsdnReceiveError.split("\\|");
        changeSimSucessfully = ResourceBundle.getBundle("configPayBonus").getString("changeSimSucessfully");
        //get message change sim 
        //enlish
        smsProfileInvalid = ResourceBundle.getBundle("configPayBonus").getString("smsProfileInvalid");
        smsSimInfoInvalid = ResourceBundle.getBundle("configPayBonus").getString("smsSimInfoInvalid");
        smsCusInfoInvalid = ResourceBundle.getBundle("configPayBonus").getString("smsCusInfoInvalid");
        smsSubNotAcitive = ResourceBundle.getBundle("configPayBonus").getString("smsSubNotAcitive");
        smsProductCodeInvalid = ResourceBundle.getBundle("configPayBonus").getString("smsProductCodeInvalid");
        smsSimNotSale = ResourceBundle.getBundle("configPayBonus").getString("smsSimNotSale");
        smsSimSerialEmpty = ResourceBundle.getBundle("configPayBonus").getString("smsSimSerialEmpty");
        smsChangeSimFailed = ResourceBundle.getBundle("configPayBonus").getString("smsChangeSimFailed");
        smsSim4GAlreadyUsed = ResourceBundle.getBundle("configPayBonus").getString("smsSim4GAlreadyUsed");
        //purtogal
        smsProfileInvalidPT = ResourceBundle.getBundle("configPayBonus").getString("smsProfileInvalidPT");
        smsSimInfoInvalidPT = ResourceBundle.getBundle("configPayBonus").getString("smsSimInfoInvalidPT");
        smsCusInfoInvalidPT = ResourceBundle.getBundle("configPayBonus").getString("smsCusInfoInvalidPT");
        smsSubNotAcitivePT = ResourceBundle.getBundle("configPayBonus").getString("smsSubNotAcitivePT");
        smsProductCodeInvalidPT = ResourceBundle.getBundle("configPayBonus").getString("smsProductCodeInvalidPT");
        smsSimNotSalePT = ResourceBundle.getBundle("configPayBonus").getString("smsSimNotSalePT");
        smsSimSerialEmptyPT = ResourceBundle.getBundle("configPayBonus").getString("smsSimSerialEmptyPT");
        smsChangeSimFailedPT = ResourceBundle.getBundle("configPayBonus").getString("smsChangeSimFailedPT");
        smsSim4GAlreadyUsedPT = ResourceBundle.getBundle("configPayBonus").getString("smsSim4GAlreadyUsedPT");
        k4sNO = 2L;

    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        for (Record record : listRecord) {
            RequestChangeSim moRecord = (RequestChangeSim) record;
            moRecord.setNodeName(holder.getNodeName());
            moRecord.setClusterName(holder.getClusterName());
        }
        return listRecord;
    }

    @Override
    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
        List<Record> listResult = new ArrayList<Record>();
        String staffCode;
        long timeSt;
        boolean isSim4G;
        Long actionAuditId;
        String newImsi;
        String oldImsi;
        String newEkiValue;
        String oldSerial;
        SubInfo subInfo;
        String productCode;
        String resultModImsi;
//        String resultModEps;
        String resultModTPLOPTGPRS;
        String resultRemoveKI;
        String resultChangeSim;
        String shopCode;
        boolean isUssd;
        int simStatus;
        boolean oldSimIs4G;
//        Long staffId;

        for (Record record : listRecord) {
            timeSt = System.currentTimeMillis();
            staffCode = "";
            isSim4G = false;
            actionAuditId = 0L;
            newImsi = "";
            newEkiValue = "";
            subInfo = null;
            oldImsi = "";
            productCode = "";
            oldSerial = "";
            resultModImsi = "";
//            resultModEps = "";
            resultModTPLOPTGPRS = "";
            resultRemoveKI = "";
            resultChangeSim = "";
            shopCode = "";
            isUssd = false;
            simStatus = 9;
            oldSimIs4G = false;

            RequestChangeSim bn = (RequestChangeSim) record;
            listResult.add(bn);
            if ("0".equals(bn.getResultCode())) {
                if (bn.getChannelType() == null || bn.getChannelType().trim().length() <= 0) {
                    logger.warn("Channel type is null or empty, request_changesim_id " + bn.getId());
                    bn.setResultCode("03");
                    bn.setDescription("Channel type is null or empty");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    //Send SMS to customer 
                    sendSmsToCustomer(bn.getIsdn(), smsChangeSimFailed, smsChangeSimFailedPT, bn.getUssd_loc(), isUssd);
                    continue;
                }

                if ("USSD".equalsIgnoreCase(bn.getChannelType())) {
                    logger.warn("ChangeSim using USSD, request_changesim_id " + bn.getId());
                    isUssd = true;
                }
                if (!isUssd) {
                    staffCode = bn.getStaffCode();
                    if ("".equals(staffCode)) {
                        logger.warn("staffCode in request_changesim_4g is null or empty, request_changesim_id " + bn.getId());
                        bn.setResultCode("01");
                        bn.setDescription("staffCode in request_changesim_4g is null or empty");
                        bn.setDuration(System.currentTimeMillis() - timeSt);
                        continue;
                    }
                }

                if (bn.getIsdn() == null || bn.getIsdn().trim().length() <= 0) {
                    logger.warn("ISDN is null or empty, id " + bn.getId());
                    bn.setResultCode("02");
                    bn.setDescription("ISDN is null or empty");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    //Send SMS to customer 
                    sendSmsToCustomer(bn.getIsdn(), smsCusInfoInvalid, smsCusInfoInvalidPT, bn.getUssd_loc(), isUssd);
                    continue;
                }
                if (bn.getNewSerial() == null || bn.getNewSerial().trim().length() <= 0) {
                    logger.warn("Serial Sim 4G is null or empty, request_changesim_id " + bn.getId());
                    bn.setResultCode("03");
                    bn.setDescription("Serial Sim 4G is null or empty");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    //Send SMS to customer 
                    sendSmsToCustomer(bn.getIsdn(), smsSimSerialEmpty, smsSimSerialEmptyPT, bn.getUssd_loc(), isUssd);
                    continue;
                }

//                Step 1: Check serial sim 4G based on stockModelId SIM 4G
//                stockModelId = db.getStockModelIdBySerial(bn.getNewSerial());// >> getStockModel of SIM based on Serial
                isSim4G = db.isSim4G(bn.getNewSerial(), 207607L);//When deploy server getStockModelId real and replace stockModelId test...
                if (!isSim4G) {
                    logger.warn("Serial of sim isn't 4G or empty, request_changesim_id " + bn.getId());
                    bn.setResultCode("04");
                    bn.setDescription("Serial of sim isn't 4G");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    //Send SMS to customer 
                    sendSmsToCustomer(bn.getIsdn(), smsSimSerialEmpty, smsSimSerialEmptyPT, bn.getUssd_loc(), isUssd);
                    continue;
                }
//                if (isUssd) {
                    /*
                 if (!db.checkIdNoOfSubscriber(bn.getIsdn(), bn.getIdNo())) {
                 logger.warn("Invalid ID_No that not map to current id of customer, request_changesim_id " + bn.getId());
                 bn.setResultCode("21");
                 bn.setDescription("Invalid ID_No that not map to current id of customer");
                 bn.setDuration(System.currentTimeMillis() - timeSt);

                 continue;
                 }
                 */
//                    if (!db.checkCorrectOldProfile(bn.getIsdn())) {
//                        logger.warn("Invalid Profile so not support to change sim, request_changesim_id " + bn.getId());
//                        bn.setResultCode("22");
//                        bn.setDescription("Invalid Profile so not support to change sim");
//                        bn.setDuration(System.currentTimeMillis() - timeSt);
//                        //Send SMS to customer 
//                        sendSmsToCustomer(bn.getIsdn(), smsProfileInvalid, smsProfileInvalidPT, bn.getUssd_loc(), isUssd);
//                        continue;
//                    }
//                }
//                Step 1.1: Get info of SIM 4G based on serial
                String[] simInfo = db.getImsiEkiSimBySerial(bn.getNewSerial(), bn.getIsdn());
                if (simInfo == null) {
                    logger.warn("Cannot get info of sim, request_changesim_id " + bn.getId());
                    bn.setResultCode("05");
                    bn.setDescription("Cannot get info of sim");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    //Send SMS to customer 
                    sendSmsToCustomer(bn.getIsdn(), smsSimInfoInvalid, smsSimInfoInvalidPT, bn.getUssd_loc(), isUssd);
                    continue;
                } else {
                    newImsi = simInfo[0];
                    newEkiValue = simInfo[1];
                    if (newImsi == null || newEkiValue == null || newImsi.isEmpty() || newEkiValue.isEmpty()) {
                        logger.warn("Cannot get info of sim, request_changesim_id " + bn.getId());
                        bn.setResultCode("05");
                        bn.setDescription("Cannot get info of sim");
                        bn.setDuration(System.currentTimeMillis() - timeSt);
                        //Send SMS to customer 
                        sendSmsToCustomer(bn.getIsdn(), smsSimInfoInvalid, smsSimInfoInvalidPT, bn.getUssd_loc(), isUssd);
                        continue;
                    }
                }

//                Step 2: Generate actionAuditId for save Log
                actionAuditId = db.getSequence("ACTION_AUDIT_SEQ", "cm_pos");
//                Step 3: Get subscriber info
                subInfo = db.getSubscriberInfo(bn.getIsdn());
                if (subInfo == null) {
                    logger.warn("Cannot get subscriber info, request_changesim_id " + bn.getId() + ", isdn: " + bn.getIsdn());
                    bn.setResultCode("06");
                    bn.setDescription("Cannot get subscriber info");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    //Send SMS to customer 
                    sendSmsToCustomer(bn.getIsdn(), smsCusInfoInvalid, smsCusInfoInvalidPT, bn.getUssd_loc(), isUssd);
                    continue;
                }
                if (!"000".equals(subInfo.getActStatus())) {
                    logger.warn("ActStatus of subscriber is invalid, request_changesim_id " + bn.getId() + ", isdn: " + bn.getIsdn());
                    bn.setResultCode("07");
                    bn.setDescription("ActStatus of subscriber is invalid");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    //Send SMS to customer 
                    sendSmsToCustomer(bn.getIsdn(), smsSubNotAcitive, smsSubNotAcitivePT, bn.getUssd_loc(), isUssd);
                    continue;
                }
                oldImsi = subInfo.getImsi();
                oldSerial = subInfo.getSerial();
                oldSimIs4G = db.isSim4G(oldSerial, 207607L);
                if (oldImsi.equals(newImsi)) {
                    logger.warn("oldImsi and newImsi is duplicate, request_changesim_id " + bn.getId() + ", isdn: " + bn.getIsdn());
                    bn.setResultCode("08");
                    bn.setDescription("oldImsi and newImsi is duplicate");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    //Send SMS to customer 
                    sendSmsToCustomer(bn.getIsdn(), smsChangeSimFailed, smsChangeSimFailedPT, bn.getUssd_loc(), isUssd);
                    continue;
                }
//                Get and check ProductCode mapping
                productCode = subInfo.getProductCode();
                if (productCode == null || productCode.trim().length() <= 0) {
                    logger.warn("productCode is null or empty, request_changesim_id " + bn.getId() + ", isdn: " + bn.getIsdn());
                    bn.setResultCode("09");
                    bn.setDescription("productCode is null or empty");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    //Send SMS to customer 
                    sendSmsToCustomer(bn.getIsdn(), smsProductCodeInvalid, smsProductCodeInvalidPT, bn.getUssd_loc(), isUssd);
                    continue;
                }
//                Step 4: Check newImsi already sold    
                simStatus = db.getSimStatus(bn.getNewSerial());
                if (simStatus == 1) {
                    logger.warn("SIM 4G not yet sale, serial: " + bn.getNewSerial()
                            + " isdn " + bn.getIsdn());
                    bn.setResultCode("21");
                    bn.setDescription("SIM 4G not yet sale");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    sendSmsToCustomer(bn.getIsdn(), smsSimNotSale, smsSimNotSalePT, bn.getUssd_loc(), isUssd);
                    continue;
                } else if (simStatus == 2) {
                    logger.warn("SIM 4G already used, serial: " + bn.getNewSerial()
                            + " isdn " + bn.getIsdn());
                    bn.setResultCode("22");
                    bn.setDescription("SIM 4G already used");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    sendSmsToCustomer(bn.getIsdn(), smsSim4GAlreadyUsed, smsSim4GAlreadyUsedPT, bn.getUssd_loc(), isUssd);
                    continue;
                } else if (simStatus != 0) {
                    logger.warn("Can not get status of new Sim: " + bn.getNewSerial()
                            + " isdn " + bn.getIsdn());
                    bn.setResultCode("23");
                    bn.setDescription("Can not get status of new Sim");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    sendSmsToCustomer(bn.getIsdn(), smsSimSerialEmpty, smsSimSerialEmptyPT, bn.getUssd_loc(), isUssd);
                    continue;
                }
//                Step 6: ChangeSIM
//                Step 6.1: Check KI
                String checkKI = pro.checkKiSim("258" + bn.getIsdn(), newImsi);
                //Error when query KI >>> KI not load...etc....
                if (!"0".equals(checkKI)) {
                    if ("ERR3048".equals(checkKI)) {
                        //KI not load >>> Add KI
                        String addKI = pro.addKiSim("258" + bn.getIsdn(), newEkiValue, newImsi, k4sNO + "", isSim4G);
                        if (!"0".equals(addKI)) {
                            logger.info("Add KI SIM 4G fail.");
                            bn.setResultCode("12");
                            bn.setDescription("Add KI sim 4G fail, errorCode: " + checkKI);
                            bn.setDuration(System.currentTimeMillis() - timeSt);
                            sendSmsToCustomer(bn.getIsdn(), smsChangeSimFailed, smsChangeSimFailedPT, bn.getUssd_loc(), isUssd);
                            continue;
                        }
                    } else {
                        logger.info("Query fail Error isn't KI not load, isdn: " + bn.getIsdn() + ", imsi: " + newImsi);
                        bn.setResultCode("13");
                        bn.setDescription("Query KI not success, errorCode: " + checkKI);
                        bn.setDuration(System.currentTimeMillis() - timeSt);
                        sendSmsToCustomer(bn.getIsdn(), smsChangeSimFailed, smsChangeSimFailedPT, bn.getUssd_loc(), isUssd);
                        continue;
                    }
                }
//                Step 6.2: HLR_HW_MODI_IMSI
                resultModImsi = pro.modImsi("258" + bn.getIsdn(), newImsi);
                if (!"0".equals(resultModImsi) && !"ERR3050".equals(resultModImsi)) {//ERR3050: New IMSI in use...
                    logger.error("Fail to Mod IMSI " + bn.getIsdn() + ", newImsi: " + newImsi);
                    bn.setResultCode("15");
                    bn.setDescription("Fail to Mod IMSI. responseCode: " + resultModImsi);
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    sendSmsToCustomer(bn.getIsdn(), smsChangeSimFailed, smsChangeSimFailedPT, bn.getUssd_loc(), isUssd);

                    continue;
                }
//                Step 6.3: HLR_HW_MOD_EPS
//                resultModEps = pro.modEPS("258" + bn.getIsdn(), "TRUE", 400000000, 800000000);
//                if (!"0".equals(resultModEps)) {
//                    logger.error("Fail to Mod EPS " + bn.getIsdn() + ", newImsi: " + newImsi);
//                    //Rollback MOD IMSI
//                    rollbackModIMSI(bn, timeSt, newEkiValue, newImsi, oldImsi, k4sNO, isSim4G);
//
//                    bn.setResultCode("16");
//                    bn.setDescription("Fail to Mod EPS. responseCode: " + resultModEps);
//                    bn.setDuration(System.currentTimeMillis() - timeSt);
//                    sendSmsToCustomer(bn.getIsdn(), smsChangeSimFailed, smsChangeSimFailedPT, bn.getUssd_loc(), isUssd);
//                    continue;
//                }
//                Step 6.4: HLR_HW_MOD_TPLOPTGPRS
//                LinhNBV 20200403: For case ChangeSim 3G -> 4G: Call HLR_HW_MOD_TPLOPTGPRS with TPLID = 3 (Old is 2). 4G -> 4G no need.
                if (isSim4G && !oldSimIs4G) {
                    resultModTPLOPTGPRS = pro.modTPLOPTGPRS("258" + bn.getIsdn(), "TRUE", "3", "TRUE");
                    if (!"0".equals(resultModTPLOPTGPRS)) {
                        logger.error("Fail to Mod TPLOPTGPRS " + bn.getIsdn() + ", newImsi: " + newImsi);
                        //Rollback MOD IMSI
                        rollbackModIMSI(bn, timeSt, newEkiValue, newImsi, oldImsi, k4sNO, isSim4G);

                        bn.setResultCode("17");
                        bn.setDescription("Fail to Mod TPLOPTGPRS. responseCode: " + resultModTPLOPTGPRS);
                        bn.setDuration(System.currentTimeMillis() - timeSt);
                        sendSmsToCustomer(bn.getIsdn(), smsChangeSimFailed, smsChangeSimFailedPT, bn.getUssd_loc(), isUssd);
                        continue;
                    }
                }
//                Step 6.5: HLR_HW_REMOVE_KI
                resultRemoveKI = pro.removeKI(oldImsi);
                if (!"0".equals(resultRemoveKI)) {
                    logger.warn("Fail to Remove KI " + bn.getIsdn() + ", newImsi: " + newImsi);
                }
//                Step 6.6: OCSHW_CHANGESIM
                resultChangeSim = pro.changeSim("258" + bn.getIsdn(), newImsi);
                if (!"0".equals(resultChangeSim)) {
                    logger.error("Fail to call OCS_CHANGESIM " + bn.getIsdn() + ", newImsi: " + newImsi);
                    //Rollback MOD IMSI
                    rollbackModIMSI(bn, timeSt, newEkiValue, newImsi, oldImsi, k4sNO, isSim4G);

                    bn.setResultCode("19");
                    bn.setDescription("Fail to call OCS_CHANGESIM. responseCode: " + resultChangeSim);
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    sendSmsToCustomer(bn.getIsdn(), smsChangeSimFailed, smsChangeSimFailedPT, bn.getUssd_loc(), isUssd);
                    continue;
                }
//                Step 5.1: Update subMB, sub_sim_mb
                db.updateSubMbChangeSim(subInfo.getSubId(), bn.getIsdn(), newImsi, bn.getNewSerial());
                if (!db.checkSubSimMb(subInfo.getSubId(), oldImsi)) {
//                    Update sub_sim_mb
                    db.updateSubSimMb(subInfo.getSubId(), oldImsi);
                } else {
//                    insert sub_sim_mb
                    db.insertSubSimMb(subInfo.getSubId(), newImsi);
                }
//                Step 5.2: Update Stock_Sim and update Stock_Total
                db.updateStockSim(2L, 2L, bn.getNewSerial(), bn.getIsdn());
//                Step 6: Save actionLog
                if (!isUssd) {
                    db.insertActionAudit(actionAuditId, bn.getIsdn(), bn.getNewSerial(),
                            "Change SIM Post Paid 3G --->>> 4G successfully for " + bn.getIsdn() + ".", subInfo.getSubId(), shopCode, staffCode);
                } else {
                    db.insertActionAudit(actionAuditId, bn.getIsdn(), bn.getNewSerial(),
                            "Change SIM Post Paid 3G --->>> 4G successfully for " + bn.getIsdn() + ".", subInfo.getSubId(), "USSD", "USSD");
                }
//                Step 6.1: Send sms
                String tempMsg = changeSimSucessfully;
                tempMsg = tempMsg.replace("%SIM_SERIAL%", bn.getNewSerial());
                if (!isUssd) {
                    String tel = db.getTelByStaffCode(staffCode);
                    db.sendSms(tel, tempMsg, "86904");
                    logger.info("isdn " + bn.getIsdn() + " Message send to staff: " + tempMsg);
                }
//                Send sms customer...                
                db.sendSms(bn.getIsdn(), tempMsg, "86904");
                logger.info("isdn " + bn.getIsdn() + " Message send to customer: " + tempMsg);
                bn.setResultCode("0");
                bn.setDescription("Change SIM Successfully.");
                bn.setDuration(System.currentTimeMillis() - timeSt);
                bn.setOldImsi(oldImsi);
                bn.setOldSerial(oldSerial);
                bn.setActionAuditId(actionAuditId);
                continue;
            } else {
                logger.warn("After validate respone code is fail actionId " + bn.getActionAuditId()
                        + " id " + bn.getId() + " isdn: " + bn.getIsdn()
                        + " so continue with other transaction");
                bn.setDescription("After validate respone code is fail");
                bn.setDuration(System.currentTimeMillis() - timeSt);
                continue;
            }
        }
        listRecord.clear();
        return listResult;
    }

    @Override
    public void printListRecord(List<Record> listRecord) throws Exception {
        StringBuilder br = new StringBuilder();
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        br.setLength(0);
        br.append("\r\n").
                append("|\tID|").
                append("|\tISDN|").
                append("|\tNEW_SERIAL|").
                append("|\tID_NO\t|").
                append("|\tSTATUS\t|").
                append("|\tCREATE_TIME\t|").
                append("|\tSTAFF_CODE\t|");
        for (Record record : listRecord) {
            RequestChangeSim bn = (RequestChangeSim) record;
            br.append("\r\n").
                    append("|\t").
                    append(bn.getId()).
                    append("||\t").
                    append(bn.getIsdn()).
                    append("||\t").
                    append(bn.getNewSerial()).
                    append("||\t").
                    append(bn.getIdNo()).
                    append("||\t").
                    append(bn.getStatus()).
                    append("||\t").
                    append((bn.getCreateTime() != null ? sdf.format(bn.getCreateTime()) : null)).
                    append("||\t").
                    append(bn.getStaffCode());
        }
        logger.info(br);
    }

    @Override
    public List<Record> processException(List<Record> listRecord, Exception ex) {
        logger.info("Process Exception....");
        long timeSt = System.currentTimeMillis();
        for (Record record : listRecord) {
            RequestChangeSim bn = (RequestChangeSim) record;
            bn.setResultCode("98");
            bn.setDescription("Exception when process...Ex: " + ex.getLocalizedMessage());
            bn.setDuration(System.currentTimeMillis() - timeSt);
            continue;
        }
        return listRecord;
    }

    @Override
    public boolean startProcessRecord() {
        return true;
    }

    /**
     * Send SMS to customer when use USSD to change SIM
     *
     * @param isdn
     * @param msg
     * @param msgPT
     * @param ussdLoc
     * @param isUssd
     */
    public void sendSmsToCustomer(String isdn, String msg, String msgPT, String ussdLoc, boolean isUssd) {
        if (isUssd) {
            if (msg != null && msg.length() > 0 && isdn != null && isdn.length() > 0) {
                if ("EN".equalsIgnoreCase(ussdLoc)) {
                    db.sendSms(isdn, msg, "86142");
                } else {
                    db.sendSms(isdn, msgPT, "86142");
                }
            }
        }
    }

    /**
     * Rollback Mod IMSI, replace new IMIS with old IMSI
     *
     * @param bn
     * @param timeSt
     * @param newEkiValue
     * @param newImsi
     * @param oldImsi
     * @param k4sNO
     * @param isSim4G
     * @return
     */
    public String rollbackModIMSI(RequestChangeSim bn, Long timeSt, String newEkiValue, String newImsi, String oldImsi, Long k4sNO, boolean isSim4G) {
        logger.info("--->Start rollback process." + bn.getIsdn() + ", newImsi: " + newImsi + ", oldImsi: " + oldImsi + ", Rollback to " + oldImsi);
        String oldEKI = db.getEKI(oldImsi);
        String checkKI = pro.checkKiSim("258" + bn.getIsdn(), oldImsi);
        //try to add KI again
        if (!"0".equals(checkKI)) {
            if ("ERR3048".equals(checkKI)) {
                //KI not load >>> Add KI
                String addKI = pro.addKiSim("258" + bn.getIsdn(), oldEKI, oldImsi, k4sNO + "", isSim4G);
                if (!"0".equals(addKI)) {
                    logger.info("Rollback KI: Add KI SIM 4G failed." + bn.getIsdn() + ", newImsi: " + newImsi + ", oldImsi: " + oldImsi);
                }
            } else {
                logger.info("Rollback KI: Query fail Error isn't KI not load, isdn: " + bn.getIsdn() + ", newImsi: " + newImsi + ", oldImsi: " + oldImsi);
            }
        } else {
            logger.info("Rollback KI: Add KI SIM 4G susseccfully." + bn.getIsdn() + ", newImsi: " + newImsi + ", oldImsi: " + oldImsi);
        }
        //Rollback MOD IMSI
        String resultRollbackModImsi = pro.modImsi("258" + bn.getIsdn(), oldImsi);
        if (!"0".equals(resultRollbackModImsi)) {
            logger.error("Rollback MOD: IMSDI failed " + bn.getIsdn() + ", newImsi: " + newImsi + ", oldImsi: " + oldImsi);
        } else {
            logger.error("Rollback MOD: IMSDI susseccfully " + bn.getIsdn() + ", imsi " + oldImsi);
        }
        logger.info("--->End rollback process." + bn.getIsdn() + ", newImsi: " + newImsi + ", oldImsi: " + oldImsi);
        return resultRollbackModImsi;
    }
}
