/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbRetryConnectKit;
import com.viettel.paybonus.obj.KitRetry;
import com.viettel.paybonus.service.Exchange;
import com.viettel.paybonus.service.Service;
import com.viettel.threadfw.manager.AppManager;
import java.util.List;
import org.apache.log4j.Logger;
import com.viettel.threadfw.process.ProcessRecordAbstract;
import com.viettel.vas.util.ExchangeClientChannel;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

/**
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class RetryConnectKit extends ProcessRecordAbstract {

    Exchange pro;
    Service services;
    DbRetryConnectKit db;

    public RetryConnectKit() {
        super();
        logger = Logger.getLogger(RetryConnectKit.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
        db = new DbRetryConnectKit();

    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        for (Record record : listRecord) {
            KitRetry moRecord = (KitRetry) record;
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
        String imsiOCS;
        String imsiHRL;
        String imsiBCCS;
        for (Record record : listRecord) {
            timeSt = System.currentTimeMillis();
            KitRetry bn = (KitRetry) record;
            imsiOCS = pro.getImsiOnOCSByIsdn(bn.getIsdn());
            imsiHRL = pro.getImsiOnHLRByIsdn(bn.getIsdn());
            imsiBCCS = db.getImsiOnBCCS(bn.getIsdn());
            logger.info(logger.getClass() + ", get info IMSI from OCS:" + imsiOCS + ",HRL:" + imsiHRL + ",BCCS:" + imsiBCCS + " isdn " + bn.getIsdn());

            if(imsiOCS == null){
                bn.setDescription("imsiOCS NULL");
                bn.setCaseProcess("imsiOCS NULL");
                bn.setImsiBCCS(imsiBCCS);
                bn.setImsiHLR(imsiHRL);
                bn.setImsiOCS(imsiOCS);
                bn.setResultCode("E08");
                db.deleteRetryConnectKit(bn);
                db.insertRetryConnectKitHis(bn);
                continue;
            }
//            if (imsiOCS.equals(imsiHRL)) {
            if(imsiOCS == null){
                logger.info(logger.getClass() + " check info in OCS,HLR,BCCS evething OK. Skip!" + " isdn " + bn.getIsdn());
                bn.setDescription("OCS not connected .....SKIP");
                bn.setCaseProcess("OCS not connected .....SKIP");
                bn.setImsiBCCS(imsiBCCS);
                bn.setImsiHLR(imsiHRL);
                bn.setImsiOCS(imsiOCS);
                bn.setResultCode("E08");
                db.deleteRetryConnectKit(bn);
                db.insertRetryConnectKitHis(bn);
                continue;
            }
            if (imsiOCS.equals(imsiHRL) && imsiOCS.equals(imsiBCCS)) {
                logger.info(logger.getClass() + " check info in OCS,HLR,BCCS evething OK. Skip!" + " isdn " + bn.getIsdn());
                bn.setDescription("Evething OK");
                bn.setCaseProcess("Evething OK");
                bn.setImsiBCCS(imsiBCCS);
                bn.setImsiHLR(imsiHRL);
                bn.setImsiOCS(imsiOCS);
                bn.setResultCode("OK");
                db.deleteRetryConnectKit(bn);
                db.insertRetryConnectKitHis(bn);
                continue;
            } else {
               
                if (imsiHRL != null && imsiHRL.trim().length() > 0) {
//                 Case 1:Have imsi on HLR so CHANGE_SIM(HLR_HW_MODI_IMSI)
                    //**add case Exist another number phone on HRL,but The IMIS in use
                    String otherPhoneWithImsi = pro.getIsdnOnHLRByImsi(imsiOCS);
                    if (otherPhoneWithImsi != null && otherPhoneWithImsi.trim().length() > 0) {
                        //Check on OCS
                        String isdnNotUse = pro.checkIsdnExistOnOCS(otherPhoneWithImsi);
                        if ("0".equals(isdnNotUse)) {
                            //Already existed on OCS,please execute by hand
                            logger.error("Fail to Mod IMSI " + bn.getIsdn() + ", newImsi: " + imsiOCS);
                            bn.setResultCode("E07");//Fail to change sim
                            bn.setDescription("Already existed on OCS,please execute by hand");
                            bn.setDuration(System.currentTimeMillis() - timeSt);
                            bn.setImsiBCCS(imsiBCCS);
                            bn.setImsiHLR(imsiHRL);
                            bn.setImsiOCS(imsiOCS);
                            bn.setCaseProcess("CASE4:Exist another number phone on HRL,but The IMIS in use");
                            db.deleteRetryConnectKit(bn);
                            db.insertRetryConnectKitHis(bn);
                            continue;
                        } else {
                            //Romove current number phone on HLR
                            String resultRmSubHrl = pro.removeSubOnHLR(otherPhoneWithImsi);
                            //Remove sub failed
                            if (!"0".equals(resultRmSubHrl)) {
                                logger.error("Remove sub failed " + bn.getIsdn() + ", newImsi: " + imsiOCS);
                                bn.setResultCode("E04");//FRemove sub failed
                                bn.setDescription("Remove sub failed:" + resultRmSubHrl);
                                bn.setDuration(System.currentTimeMillis() - timeSt);
                                bn.setImsiBCCS(imsiBCCS);
                                bn.setImsiHLR(imsiHRL);
                                bn.setImsiOCS(imsiOCS);
                                bn.setCmd2("HLR_HW_REMOVE_SUB");
                                bn.setCmd2_result(resultRmSubHrl);
                                bn.setCaseProcess("CASE4:Exist another number phone on HRL,but The IMIS in use");
                                db.deleteRetryConnectKit(bn);
                                db.insertRetryConnectKitHis(bn);
                                continue;
                            } else {
                                String resultModImsi = pro.modImsi("258" + bn.getIsdn(), imsiOCS);
                                if (!"0".equals(resultModImsi) //&& !"ERR3050".equals(resultModImsi)
                                        ) {//ERR3050: New IMSI in use...
                                    logger.error("Fail to Mod IMSI " + bn.getIsdn() + ", newImsi: " + imsiOCS);
                                    bn.setResultCode("E01");//Fail to change sim
                                    bn.setDescription("Fail to Mod IMSI. ResponseCode: " + resultModImsi);
                                    bn.setCmd2("HLR_HW_MODI_IMSI");
                                    bn.setCmd2_result(resultModImsi);
                                    bn.setCmd1("HLR_HW_REMOVE_SUB");
                                    bn.setCmd1_result(resultRmSubHrl);
                                    bn.setDuration(System.currentTimeMillis() - timeSt);
                                    bn.setImsiBCCS(imsiBCCS);
                                    bn.setImsiHLR(imsiHRL);
                                    bn.setImsiOCS(imsiOCS);
                                    bn.setCaseProcess("CASE4:Exist another number phone on HRL,but The IMIS in use");
                                    db.deleteRetryConnectKit(bn);
                                    db.insertRetryConnectKitHis(bn);
                                } else {
                                    //Change sim successfully
                                    logger.error("Change sim successfully " + bn.getIsdn() + ", newImsi: " + imsiOCS);
                                    bn.setResultCode("0");//Fail to change sim
                                    bn.setDescription("\"Change sim successfully");
                                    bn.setCmd2("HLR_HW_MODI_IMSI");
                                    bn.setCmd2_result(resultModImsi);
                                    bn.setCmd1("HLR_HW_REMOVE_SUB");
                                    bn.setCmd1_result(resultRmSubHrl);
                                    bn.setDuration(System.currentTimeMillis() - timeSt);
                                    bn.setImsiBCCS(imsiBCCS);
                                    bn.setImsiHLR(imsiHRL);
                                    bn.setImsiOCS(imsiOCS);
                                    db.deleteRetryConnectKit(bn);
                                    bn.setCaseProcess("CASE4:Exist another number phone on HRL,but The IMIS in use");
                                    db.insertRetryConnectKitHis(bn);
                                    continue;
                                }
                            }
                        }
                    } else {
                        String resultModImsi = pro.modImsi("258" + bn.getIsdn(), imsiOCS);
                        if (!"0".equals(resultModImsi) //&& !"ERR3050".equals(resultModImsi)
                                ) {//ERR3050: New IMSI in use...
                            logger.error("Fail to Mod IMSI " + bn.getIsdn() + ", newImsi: " + imsiOCS);
                            bn.setResultCode("E01");//Fail to change sim
                            bn.setDescription("Fail to Mod IMSI. ResponseCode: " + resultModImsi);
                            bn.setCmd2("HLR_HW_MODI_IMSI");
                            bn.setCmd2_result(resultModImsi);
                            bn.setDuration(System.currentTimeMillis() - timeSt);
                            bn.setImsiBCCS(imsiBCCS);
                            bn.setImsiHLR(imsiHRL);
                            bn.setImsiOCS(imsiOCS);
                            bn.setCaseProcess("CASE1:Exist another number phone on HRL");
                            db.deleteRetryConnectKit(bn);
                            db.insertRetryConnectKitHis(bn);
                        } else {
                            //Change sim successfully
                            logger.error("Change sim successfully " + bn.getIsdn() + ", newImsi: " + imsiOCS);
                            bn.setResultCode("0");//Fail to change sim
                            bn.setDescription("\"Change sim successfully");
                            bn.setCmd2("HLR_HW_MODI_IMSI");
                            bn.setCmd2_result(resultModImsi);
                            bn.setDuration(System.currentTimeMillis() - timeSt);
                            bn.setImsiBCCS(imsiBCCS);
                            bn.setImsiHLR(imsiHRL);
                            bn.setImsiOCS(imsiOCS);
                            db.deleteRetryConnectKit(bn);
                            bn.setCaseProcess("CASE1:Exist another number phone on HRL");
                            db.insertRetryConnectKitHis(bn);
                            continue;
                        }
                    }

                } else {
                    String otherPhoneWithImsi = pro.getIsdnOnHLRByImsi(imsiOCS);
                    if (otherPhoneWithImsi != null && otherPhoneWithImsi.trim().length() > 0) {
//                    check otherPhoneWithImsi on OCS to make sure not use, if true save to process by hand, other RMV otherPhone and RegisSub Again, before regisSUb must addKi
                        //Check on OCS
                        String isdnNotUse = pro.checkIsdnExistOnOCS(otherPhoneWithImsi);
                        if ("0".equals(isdnNotUse)) {
                            //Already existed on OCS,please execute by hand
                            logger.error("Fail to Mod IMSI " + bn.getIsdn() + ", newImsi: " + imsiOCS);
                            bn.setResultCode("E02");//Fail to change sim
                            bn.setDescription("Already existed on OCS,please execute by hand");
                            bn.setDuration(System.currentTimeMillis() - timeSt);
                            bn.setImsiBCCS(imsiBCCS);
                            bn.setImsiHLR(imsiHRL);
                            bn.setImsiOCS(imsiOCS);
                            bn.setCaseProcess("Exist another number phone on HRL");
                            db.deleteRetryConnectKit(bn);
                            db.insertRetryConnectKitHis(bn);
                            continue;
                        } else {
                            //Add KI for sim
                            String checkKI = pro.checkKiSim("258" + bn.getIsdn(), imsiOCS);
                            //Error when query KI >>> KI not load...etc....
                            long k4sNO = 2;
                            String eKI = db.getEKIbyImsi(imsiOCS);
                            if (!"0".equals(checkKI)) {
                                if ("ERR3048".equals(checkKI)) {
                                    //KI not load >>> Add KI
                                    String addKI = pro.addKiSim("258" + bn.getIsdn(), eKI, imsiOCS, k4sNO + "", false);
                                    //Add KI failed
                                    if (!"0".equals(addKI)) {
                                        logger.error("Add KI failed " + bn.getIsdn() + ", newImsi: " + imsiOCS);
                                        bn.setResultCode("E03");//Fail to change sim
                                        bn.setDescription("Add KI failed:" + addKI);
                                        bn.setDuration(System.currentTimeMillis() - timeSt);
                                        bn.setImsiBCCS(imsiBCCS);
                                        bn.setImsiHLR(imsiHRL);
                                        bn.setImsiOCS(imsiOCS);
                                        bn.setCaseProcess("Exist another number phone on HRL");
                                        db.deleteRetryConnectKit(bn);
                                        db.insertRetryConnectKitHis(bn);
                                        continue;
                                    }
                                } else {
                                    logger.info("Query fail Error isn't KI not load, isdn: " + bn.getIsdn() + ", imsi: " + imsiOCS);
                                    bn.setDescription("Query KI not success, errorCode: " + checkKI);
                                    bn.setResultCode("E06");//Fail to change sim
                                    bn.setDescription("Add KI failed:" + eKI);
                                    bn.setDuration(System.currentTimeMillis() - timeSt);
                                    bn.setImsiBCCS(imsiBCCS);
                                    bn.setImsiHLR(imsiHRL);
                                    bn.setImsiOCS(imsiOCS);
                                    bn.setCaseProcess("Exist another number phone on HRL");
                                    db.deleteRetryConnectKit(bn);
                                    db.insertRetryConnectKitHis(bn);
                                    continue;
                                }
                            }
                            //Romove curent number phone on HLR
                            String resultRmSubHrl = pro.removeSubOnHLR(otherPhoneWithImsi);
                            //Remove sub failed
                            if (!"0".equals(resultRmSubHrl)) {
                                logger.error("Remove sub failed " + bn.getIsdn() + ", newImsi: " + imsiOCS);
                                bn.setResultCode("E04");//FRemove sub failed
                                bn.setDescription("Remove sub failed:" + resultRmSubHrl);
                                bn.setDuration(System.currentTimeMillis() - timeSt);
                                bn.setImsiBCCS(imsiBCCS);
                                bn.setImsiHLR(imsiHRL);
                                bn.setImsiOCS(imsiOCS);
                                bn.setCmd3("HLR_HW_REMOVE_SUB");
                                bn.setCmd3_result(resultRmSubHrl);
                                bn.setCaseProcess("Exist another number phone on HRL");
                                db.deleteRetryConnectKit(bn);
                                db.insertRetryConnectKitHis(bn);
                                continue;
                            } else {
                                logger.info("Register sub startting ..., isdn: " + bn.getIsdn() + ", imsi: " + imsiOCS);
                                String TPLID = db.getTPLID(bn.getIsdn(), false);
                                String regSub = pro.registerSubOnHLR(bn.getIsdn(), imsiOCS, TPLID);
                                if (!"0".equals(regSub)) {
                                    logger.info("Register sub failed, isdn: " + bn.getIsdn() + ", imsi: " + imsiOCS);
                                    bn.setResultCode("E05");
                                    bn.setDescription("Register sub failed");
                                    bn.setDuration(System.currentTimeMillis() - timeSt);
                                    bn.setCmd3("HLR_HW_REGIST_SUB");
                                    bn.setCmd3_result(regSub);
                                    bn.setCmd2("HLR_HW_REMOVE_SUB");
                                    bn.setCmd2_result(resultRmSubHrl);
                                    bn.setImsiBCCS(imsiBCCS);
                                    bn.setImsiHLR(imsiHRL);
                                    bn.setImsiOCS(imsiOCS);
                                    bn.setCaseProcess("Exist another number phone on HRL");
                                    db.deleteRetryConnectKit(bn);
                                    db.insertRetryConnectKitHis(bn);
                                    continue;
                                } else {
                                    logger.info("Register sub successfully, isdn: " + bn.getIsdn() + ", imsi: " + imsiOCS);
                                    bn.setResultCode("0");
                                    bn.setDescription("Register sub successfully");
                                    bn.setDuration(System.currentTimeMillis() - timeSt);
                                    bn.setCmd3("HLR_HW_REGIST_SUB");
                                    bn.setCmd3_result(regSub);
                                    bn.setCmd2("HLR_HW_REMOVE_SUB");
                                    bn.setCmd2_result(resultRmSubHrl);
                                    bn.setImsiBCCS(imsiBCCS);
                                    bn.setImsiHLR(imsiHRL);
                                    bn.setImsiOCS(imsiOCS);
                                    bn.setCaseProcess("Exist another number phone on HRL");
                                    db.deleteRetryConnectKit(bn);
                                    db.insertRetryConnectKitHis(bn);
                                    continue;
                                }
                            }
                        }

                    } else {
//                       RegisSub bn.getIsdn() with imsiOCS before regisSUb must addKi
                        //Add KI for sim
                        String checkKI = pro.checkKiSim("258" + bn.getIsdn(), imsiOCS);
                        //Error when query KI >>> KI not load...etc....
                        long k4sNO = 2;
                        String eKI = db.getEKIbyImsi(imsiOCS);
                        if (!"0".equals(checkKI)) {
                            if ("ERR3048".equals(checkKI)) {
                                //KI not load >>> Add KI
                                String addKI = pro.addKiSim("258" + bn.getIsdn(), eKI, imsiOCS, k4sNO + "", false);
                                //Add KI failed
                                if (!"0".equals(addKI)) {
                                    logger.error("Add KI failed " + bn.getIsdn() + ", newImsi: " + imsiOCS);
                                    bn.setResultCode("E03");//Fail to change sim
                                    bn.setDescription("Add KI failed:" + addKI);
                                    bn.setDuration(System.currentTimeMillis() - timeSt);
                                    bn.setImsiBCCS(imsiBCCS);
                                    bn.setImsiHLR(imsiHRL);
                                    bn.setImsiOCS(imsiOCS);
                                    bn.setCaseProcess("NOT Exist another number phone on HRL");
                                    db.deleteRetryConnectKit(bn);
                                    db.insertRetryConnectKitHis(bn);
                                    continue;
                                }
                            }
                        }
                        //Register sub on HLR
                        logger.info("Register sub startting ..., isdn: " + bn.getIsdn() + ", imsi: " + imsiOCS);
                        String TPLID = db.getTPLID(bn.getIsdn(), false);
                        String regSub = pro.registerSubOnHLR(bn.getIsdn(), imsiOCS, TPLID);
                        if (!"0".equals(regSub)) {
                            logger.info("Register sub failed, isdn: " + bn.getIsdn() + ", imsi: " + imsiOCS);
                            bn.setResultCode("E05");
                            bn.setDescription("Register sub failed");
                            bn.setDuration(System.currentTimeMillis() - timeSt);
                            bn.setCmd2("HLR_HW_REGIST_SUB");
                            bn.setCmd2_result(regSub);
                            bn.setImsiBCCS(imsiBCCS);
                            bn.setImsiHLR(imsiHRL);
                            bn.setImsiOCS(imsiOCS);
                            bn.setCaseProcess("NOT Exist number phone on HRL");
                            db.deleteRetryConnectKit(bn);
                            db.insertRetryConnectKitHis(bn);
                            continue;
                        } else {
                            logger.info("Register sub successfully, isdn: " + bn.getIsdn() + ", imsi: " + imsiOCS);
                            bn.setResultCode("0");
                            bn.setDescription("Register sub successfully");
                            bn.setDuration(System.currentTimeMillis() - timeSt);
                            bn.setCmd2("HLR_HW_REGIST_SUB");
                            bn.setCmd2_result(regSub);
                            bn.setImsiBCCS(imsiBCCS);
                            bn.setImsiHLR(imsiHRL);
                            bn.setImsiOCS(imsiOCS);
                            bn.setCaseProcess("NOT Exist number phone on HRL");
                            db.deleteRetryConnectKit(bn);
                            db.insertRetryConnectKitHis(bn);
                            continue;
                        }
                    }
                }
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
                append("|\ttACTION_AUDIT_ID|").
                append("|\tSIM_SERIAL|").
                append("|\tDATE_PROCESS|");
        for (Record record : listRecord) {
            KitRetry bn = (KitRetry) record;
            br.append("\r\n").
                    append("||\t").
                    append(bn.getActionAuditId()).
                    append("||\t").
                    append(bn.getIsdn()).
                    append("||\t").
                    append(bn.getSerial()).
                    append("||\t").
                    append(bn.getProcessDate());

        }
        logger.info(br);
    }

    @Override
    public List<Record> processException(List<Record> listRecord, Exception ex) {
        return listRecord;
    }

    @Override
    public boolean startProcessRecord() {
        return true;
    }
}
