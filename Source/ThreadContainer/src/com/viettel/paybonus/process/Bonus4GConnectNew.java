/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbBonus4GConnectNew;
import com.viettel.paybonus.obj.BonusSim4G;
import com.viettel.paybonus.obj.Offer;
import com.viettel.paybonus.service.Exchange;
import com.viettel.threadfw.manager.AppManager;
import java.util.List;
import org.apache.log4j.Logger;
import com.viettel.threadfw.process.ProcessRecordAbstract;
import com.viettel.vas.util.ExchangeClientChannel;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.ResourceBundle;
import javax.xml.parsers.ParserConfigurationException;
import org.xml.sax.SAXException;

/**
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class Bonus4GConnectNew extends ProcessRecordAbstract {

    Exchange pro;
    DbBonus4GConnectNew db;
    String bonusMBDataWhenChangeTo4G;
    long bonusBytes;
    Calendar cal;
    String balanceType;
    int dataValidDate;
    String bonusValidDateWhenChangeTo4G;
    String bonusDataOnPCRF;
    String[] pckDataOnPCRF;
    String bonusMsgToCusSuccess;
    String bonusMsgToCusWarning;
    String bonusCheckPeriod;
    String bonus4GMsgConnNew;
    String vOCSIdDataUnlimited;
    String[] arrvOCSDataUnlimited;

    public Bonus4GConnectNew() {
        super();
        logger = Logger.getLogger(Bonus4GConnectNew.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
        db = new DbBonus4GConnectNew();
        bonusMBDataWhenChangeTo4G = ResourceBundle.getBundle("configPayBonus").getString("bonus4GData");
        bonusValidDateWhenChangeTo4G = ResourceBundle.getBundle("configPayBonus").getString("bonus4GValidDate");
        bonusBytes = getKBBonusData(bonusMBDataWhenChangeTo4G);
        dataValidDate = getValidDate(bonusValidDateWhenChangeTo4G);
        bonusDataOnPCRF = ResourceBundle.getBundle("configPayBonus").getString("bonus4GDataOnPCRFPackage");
        pckDataOnPCRF = bonusDataOnPCRF.split("\\|");
        bonusMsgToCusSuccess = ResourceBundle.getBundle("configPayBonus").getString("bonus4GMsgSuccess");
        bonusMsgToCusWarning = ResourceBundle.getBundle("configPayBonus").getString("bonus4GMsgWarning");
        bonusCheckPeriod = ResourceBundle.getBundle("configPayBonus").getString("bonus4GCheckPeriod");
        bonus4GMsgConnNew = ResourceBundle.getBundle("configPayBonus").getString("bonus4GMsgConnNew");
        balanceType = "5002";
        vOCSIdDataUnlimited = ResourceBundle.getBundle("configPayBonus").getString("vOCSIdDataUnlimited");
        arrvOCSDataUnlimited = vOCSIdDataUnlimited.split("\\|");
    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        return listRecord;
    }

    @Override
    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
        List<Record> listResult = new ArrayList<Record>();
        for (Record record : listRecord) {

            cal = Calendar.getInstance();
            cal.setTime(new Date());
            cal.add(Calendar.DATE, dataValidDate);
            BonusSim4G bn = (BonusSim4G) record;
            listResult.add(bn);
            boolean isNewSim = true;
            BonusSim4G bnUpt = db.getInforBonusSim4G(bn.getIsdn());
            if (bnUpt != null && bnUpt.getActionAuditId() != null) {
                isNewSim = false;
                bn = bnUpt;
            }
            if (isNewSim) {
                boolean checkProfile = db.checkCorrectProfile(bn.getIsdn());
                if (!checkProfile) {
                    logger.warn("Invalid  profile:" + bn.toString());
                    bn.setAddDataDesc("Invalid  profile");
                    bn.setStatus(2);
                    bn.setResultCode("E10");
                    db.insertInforBonusSim4G(bn);
                    continue;
                }
            }
            if (bn.getStatus() == 2) {//update profile
                boolean checkProfile = db.checkCorrectProfile(bn.getIsdn());
                if (!checkProfile) {
                    logger.warn("Invalid  profile:" + bn.toString());
                    bn.setAddDataDesc("Invalid  profile");
                    bn.setStatus(2);
                    bn.setTimeCheck(bn.getTimeCheck() + 1);
                    bn.setResultCode("E10");
                    db.updateInforBonusSim4G(bn);
                    continue;
                }
            }
            if (bonusCheckPeriod == null || bonusCheckPeriod.length() == 0) {
                logger.warn("Invalid  period time " + bn.getIsdn());
                bn.setAddDataDesc("Invalid  period time");
                bn.setResultCode("E8");
                if (isNewSim) {
                    logger.warn("Insert insertInforBonusSim4G:" + bn.toString());
                    db.insertInforBonusSim4G(bn);
                } else {
                    logger.warn("Update updateInforBonusSim4G:" + bn.toString());
                    db.updateInforBonusSim4G(bn);
                }
                continue;
            }

            int priodDays = Integer.parseInt(bonusCheckPeriod);
            if (bn.getTimeCheck() >= priodDays) {
                logger.warn("Over priod times. Remove in queue " + bn.getIsdn() + " priodDays " + priodDays);
                bn.setAddDataDesc("Over priod times " + priodDays);
                bn.setResultCode("E5");
                bn.setStatus(0);
                if (isNewSim) {
                    logger.warn("Insert insertInforBonusSim4G:" + bn.toString());
                    db.insertInforBonusSim4G(bn);
                } else {
                    logger.warn("Update updateInforBonusSim4G:" + bn.toString());
                    db.updateInforBonusSim4G(bn);
                }
                continue;
            }
            //start add bonus data
            if (bonusBytes != 0) {//check config for bonus data
                String imei = db.getDataVolume4G(bn.getIsdn());
                if (imei != null && imei.length() >= 14) {//check handset must be 4G
                    if (!db.checkReceivedPromotion(bn.getIsdn().trim())) {//not reveived promotion before
                        if (db.checkIMEIReceived(imei)) {
                            logger.warn("The phone already received promotion before " + bn.getIsdn());
                            bn.setAddDataDesc("The phone already received promotion");
                            bn.setResultCode("E7");
                            bn.setTimeCheck(bn.getTimeCheck() + 1);
                        } else {
                            String productCode = db.getProductCode(bn.getIsdn());
                            boolean checkAddOnPCRF = checkAddOnPCRF(pckDataOnPCRF, productCode);
                            String resultChargeData = addBonusData(checkAddOnPCRF, bonusBytes, dataValidDate, bn.getIsdn());

                            if (!"0".equals(resultChargeData)) {
                                logger.warn("Fail to add data resultChargeData " + bn.getIsdn() + " errcode resultChargeData " + resultChargeData);
                                bn.setAddDataDesc("Fail to add data");
                                bn.setResultCode("E1");
                                bn.setTimeCheck(bn.getTimeCheck() + 1);
                            } else {
                                db.sendSms(bn.getIsdn(), bonusMsgToCusSuccess, "86904");
                                logger.warn("Add data successfully " + bn.getIsdn() + " errcode resultChargeData " + resultChargeData);
                                bn.setAddDataDesc("Add data successfully , value : " + (bonusBytes / 1024 / 1024));
                                bn.setDataValuesAdded(bonusBytes / 1024 / 1024);
                                bn.setResultCode(resultChargeData);
                                bn.setTimeCheck(bn.getTimeCheck() + 1);
                                bn.setStatus(0);
                                bn.setImeiHS(imei);
                            }
                        }

                    } else {
                        logger.warn("The number already received promotion" + bn.getIsdn());
                        bn.setAddDataDesc("Already received promotion");
                        bn.setResultCode("E2");
                        bn.setTimeCheck(bn.getTimeCheck() + 1);
                        bn.setStatus(0);
                    }
                } else {
                    logger.warn("The isdn not gennerate data on 4G infrastructure or the IMEI invalid " + bn.getIsdn());
                    bn.setAddDataDesc("The isdn not gennerate data on 4G infrastructure or the IMEI invalid " + imei);
                    bn.setResultCode("E6");
                    bn.setTimeCheck(bn.getTimeCheck() + 1);
                    if (isNewSim) {
                        db.sendSms(bn.getIsdn(), bonus4GMsgConnNew, "86904");
                    }
                }
            } else {
                logger.warn("Configuration is invalid" + bn.getIsdn() + " bonusBytes " + bonusBytes);
                bn.setAddDataDesc("Configuration is invalid" + bonusBytes);
                bn.setResultCode("E4");
            }
            //end add bonus data
            if (isNewSim) {
                logger.warn("Insert insertInforBonusSim4G:" + bn.toString());
                db.insertInforBonusSim4G(bn);
            } else {
                logger.warn("Update updateInforBonusSim4G:" + bn.toString());
                db.updateInforBonusSim4G(bn);
            }
        }
        listRecord.clear();
        return listResult;
    }

    public long getKBBonusData(String mbDataStr) {
        long mbData = 0L;
        try {
            if (mbDataStr != null && mbDataStr.trim().length() > 0) {
                mbData = Long.valueOf(mbDataStr.trim()) * 1024 * 1024;//--Bytes ->> MB
            }
        } catch (Exception ex) {
            logger.error("BonusChangeSim4G --> getMBBonusData --> " + ex.getMessage());
            return 0L;
        }
        return mbData;
    }

    public int getValidDate(String mbDateStr) {
        int valDate = 0;
        try {
            if (mbDateStr != null && mbDateStr.trim().length() > 0) {
                valDate = Integer.valueOf(mbDateStr.trim());
                return valDate;
            }
        } catch (Exception ex) {
            logger.error("BonusChangeSim4G --> getValidDate --> " + ex.getMessage());
            return 0;
        }
        return valDate;
    }

    public List<HashMap> getDataMapConfiguration(String dataMap) {
        String[] arrDataMap;
        List<HashMap> lstDataMap;
        try {
            lstDataMap = new ArrayList<HashMap>();
            arrDataMap = dataMap.split("\\|");
            for (String item : arrDataMap) {
                String[] arrConn = item.split("\\:");
                HashMap map = new HashMap();
                map.put(arrConn[0].trim().toUpperCase(), arrConn[1].trim());
                lstDataMap.add(map);
            }
        } catch (Exception ex) {
            logger.error("BonusChangeSim4G --> getDataMapConfiguration --> " + ex.getMessage());
            return new ArrayList<HashMap>();
        }
        return lstDataMap;
    }

    /**
     * Calculator bonus money for channel, the values get from configuration
     * file
     *
     * @param bonusChannelValue
     * @return
     */
    public long getBonusForChanel(String bonusChannelValue) {
        try {
            if (bonusChannelValue != null || bonusChannelValue.length() != 0) {
                return Long.valueOf(bonusChannelValue);
            }
            return 0;
        } catch (Exception ex) {
            logger.error("getBonusForChanel --> Error --> " + ex.getMessage());
            return 0;
        }
    }

    @Override
    public void printListRecord(List<Record> listRecord) throws Exception {
        StringBuilder br = new StringBuilder();
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        br.setLength(0);
        br.append("\r\n").
                append("|\tACTION_AUDIT_ID|").
                append("|\tisdn\t|").
                append("|\tid_no\t|").
                append("|\tcreate_time\t|").
                append("|\tstaff_code\t|").
                append("|\tchannel_type\t|").
                append("|\tvas_code\t|").
                append("|\tbonus_status\t|");
        for (Record record : listRecord) {
            BonusSim4G bn = (BonusSim4G) record;
            br.append("\r\n").
                    append("|\t").
                    append(bn.getActionAuditId()).
                    append("||\t").
                    append(bn.getIsdn()).
                    append("||\t").
                    append(bn.getID()).
                    append("||\t").
                    append((bn.getCreateTime() != null ? sdf.format(bn.getCreateTime()) : null)).
                    append("||\t").
                    append(bn.getChannelType()).
                    append("||\t").
                    append(bn.getStaffCode()).
                    append("||\t").
                    append(bn.getVas_code()).
                    append("||\t").
                    append(bn.getBonusStatus());
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

    public String addBonusData(boolean addOnPCRF, long dataValueMB, int validDay, String isdn) throws ParserConfigurationException, SAXException, IOException {
        String msisdn = isdn;
        if (!msisdn.startsWith("258")) {
            msisdn = "258" + msisdn;
        }
        Date sysdate = new Date();
        Calendar calExpireTime = Calendar.getInstance();
        calExpireTime.setTime(sysdate);
        calExpireTime.add(Calendar.DATE, validDay);
        SimpleDateFormat sdfEx = new SimpleDateFormat("yyyyMMddHHmmss");
        if (addOnPCRF) {
            logger.info("Start Add data " + msisdn + " value data(MB)  " + dataValueMB);
            HashMap<String, String> lstParams = new HashMap<String, String>();
            lstParams.put("MSISDN", msisdn);
            String original = pro.getOriginalOfCommand(msisdn, "OCSHW_INTEGRATIONENQUIRY", lstParams);
            if (!original.isEmpty()) {
                List<Offer> listOffer = pro.parseListOffer(original);
                if (listOffer == null || listOffer.isEmpty()) {
                    logger.warn("Not exist add on PCRF " + msisdn);
                    return "Not exist add on PCRF";
                }
                boolean isExistDataUnlimited = false;
                for (Offer offer : listOffer) {
                    for (String tmpId : arrvOCSDataUnlimited) {
                        if (offer.getId() != null && tmpId.equals(offer.getId()) && "1".equals(offer.getState())) {
                            isExistDataUnlimited = true;
                            break;
                        }
                    }
                    if (isExistDataUnlimited) {
                        break;
                    }
                }
                if (isExistDataUnlimited) {
                    //Convert dataValue to byte...(Config dataValue = MB)
                    logger.info("dataValue: " + dataValueMB + " byte, isdn: " + msisdn);
                    String rsAddData = pro.addSmsDataVoice(msisdn, String.valueOf(dataValueMB), "500", "");
                    if (!"0".equals(rsAddData)) {
                        logger.warn("Fail to add data " + msisdn + " errcode " + rsAddData);
                        return "Fail to add data";
                    }
                } else {
                    logger.warn("Not exist add on PCRF " + msisdn);
                    return "Not exist add on PCRF";
                }


            } else {
                logger.warn("Cannot get original of subscriber " + msisdn);
                return "Cannot get original of subscriber";
            }

        } else {
            logger.info("Start Add data " + msisdn + " value data(MB)  " + dataValueMB);
            String resultAddData = pro.addSmsDataVoice(msisdn, (dataValueMB) + "", balanceType, sdfEx.format(calExpireTime.getTime()));
            if (!"0".equals(resultAddData)) {
                logger.info("Add data un-successfull result " + resultAddData + " " + msisdn + " value data(MB)  " + dataValueMB);
                return "Add data un-successfull";
            }
        }
        return "0";
    }

    public boolean checkAddOnPCRF(String[] products, String currentPP) {
        if (products == null || products.length == 0) {
            return false;
        } else {
            for (String p : products) {
                if (p.equals(currentPP)) {
                    return true;
                }
            }
            return false;
        }
    }
}
