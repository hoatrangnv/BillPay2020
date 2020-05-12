/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbBonus4GChangeSim;
import com.viettel.paybonus.obj.Agent;
import com.viettel.paybonus.obj.BonusSim4G;
import com.viettel.paybonus.obj.Customer;
import com.viettel.paybonus.obj.Offer;
import com.viettel.paybonus.service.Exchange;
//import com.viettel.paybonus.service.XmlUtil;
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
public class Bonus4GChangeSim extends ProcessRecordAbstract {

    Exchange pro;
    DbBonus4GChangeSim db;
    String bonusMBDataWhenChangeTo4G;
    String bonusForChannelEmola;
    List<HashMap> mapDataConfig;
    long bonusBytes;
    Calendar cal;
    String balanceType;
    int dataValidDate;
    String bonusValidDateWhenChangeTo4G;
    String bonusDataOnPCRF;
    String[] pckDataOnPCRF;
    long bonusChannelValue;
    String bonusMsgToCusSuccess;
    String bonusMsgToCusWarning;
    String bonusMsgToChannelChangeTo4G;
    String bonus4GMsgForCustSuccess;
    String bonus4GForCustEmola;
    long valueBonusForCust;
    long valuesBonusForStaff;
    String vOCSIdDataUnlimited;
    String[] arrvOCSDataUnlimited;

    public Bonus4GChangeSim() {
        super();
        logger = Logger.getLogger(Bonus4GChangeSim.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
        db = new DbBonus4GChangeSim();
        bonusMBDataWhenChangeTo4G = ResourceBundle.getBundle("configPayBonus").getString("bonus4GData");
        bonusForChannelEmola = ResourceBundle.getBundle("configPayBonus").getString("bonus4GChannelEmola");
        bonusValidDateWhenChangeTo4G = ResourceBundle.getBundle("configPayBonus").getString("bonus4GValidDate");
        bonusBytes = getKBBonusData(bonusMBDataWhenChangeTo4G);
        dataValidDate = getValidDate(bonusValidDateWhenChangeTo4G);
        bonusDataOnPCRF = ResourceBundle.getBundle("configPayBonus").getString("bonus4GDataOnPCRFPackage");
        pckDataOnPCRF = bonusDataOnPCRF.split("\\|");
        bonusMsgToCusSuccess = ResourceBundle.getBundle("configPayBonus").getString("bonus4GChangeSimMsgSuccess");
        bonusMsgToCusWarning = ResourceBundle.getBundle("configPayBonus").getString("bonus4GMsgWarning");
        bonusMsgToChannelChangeTo4G = ResourceBundle.getBundle("configPayBonus").getString("bonus4GMsgChannel");
        bonus4GMsgForCustSuccess = ResourceBundle.getBundle("configPayBonus").getString("bonus4GMsgForCustSuccess");
        bonus4GForCustEmola = ResourceBundle.getBundle("configPayBonus").getString("bonus4GForCustEmola");
        balanceType = "5002";
        valuesBonusForStaff = 0;
        valueBonusForCust = 0;
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
        long timeSt;
        SimpleDateFormat sdf = new SimpleDateFormat("ddMMyyyyHHmmssSSS");
        String actionCodeBonusEmola = "615";
        boolean newSimIs4G;
        boolean oldSimIs4G;
        boolean addEmolaForStaff = true;
        boolean addData = true;
        boolean addEmolaForCust = true;

        for (Record record : listRecord) {
            timeSt = System.currentTimeMillis();
            cal = Calendar.getInstance();
            cal.setTime(new Date());
            cal.add(Calendar.DATE, dataValidDate);
            BonusSim4G bn = (BonusSim4G) record;
            listResult.add(bn);
            if (bn.getNewSerial() == null || bn.getNewSerial().trim().length() == 0
                    || bn.getOldSerial() == null || bn.getOldSerial().trim().length() == 0) {
                logger.debug("Invalid input "
                        + "MISDN: " + bn.getIsdn() + ", new serial " + bn.getNewSerial() + ", staff_code " + bn.getStaffCode() + " old serial " + bn.getOldSerial());
                bn.setAddDataDesc("Invalid input serial must be not null");
                bn.setAddDataStatus("1");
                bn.setBonusDesc("Invalid input serial must be not null");
                bn.setResultCode("E13");
                continue;
            }
            newSimIs4G = db.isSim4G(bn.getNewSerial(), 207607L);
            oldSimIs4G = db.isSim4G(bn.getOldSerial(), 207607L);
            if (!oldSimIs4G && newSimIs4G) {//apply only for change from 3G to 4G
                //<editor-fold defaultstate="collapsed" desc="bonus data for customer">
                if (bonusBytes != 0) {//check config for bonus data
                    String imeiHS = db.checkUsedHandSet4G(bn.getIsdn());
                    if (imeiHS != null && imeiHS.length() >= 14) {//check handset must be 4G
                        boolean checkimeiHS = db.checkIMEIReceived(imeiHS);
                        if (checkimeiHS) {
                            logger.warn("he mobile already received promotion " + bn.getIsdn() + "  IMEI " + imeiHS);
                            bn.setAddDataDesc("The mobile already received promotion");
                            bn.setAddDataStatus("1");
                            db.sendSms(bn.getIsdn(), bonusMsgToCusWarning, "86904");
                            db.insertWaitingBonusQueue(bn);
                        } else {
                            if (!db.checkReceivedPromotion(bn.getIsdn().trim())) {//not reveived promotion before
                                boolean checkUsedDataInMonth = db.checkUseData3G(bn.getIsdn(), 0);//curent month
                                if (!checkUsedDataInMonth) {
                                    //check in last month N-1
                                    boolean checkUsedDataInMonthN1 = db.checkUseData3G(bn.getIsdn(), -1);
                                    if (!checkUsedDataInMonthN1) {
                                        //check in last month N-2
                                        boolean checkUsedDataInMonthN2 = db.checkUseData3G(bn.getIsdn(), -2);
                                        if (!checkUsedDataInMonthN2) {
                                            addData = false;
                                            db.sendSms(bn.getIsdn(), bonusMsgToCusWarning, "86904");
                                            db.insertWaitingBonusQueue(bn);
                                        }
                                    }
                                }
                                if (addData) {
                                    String productCode = db.getProductCode(bn.getIsdn());
                                    boolean checkAddOnPCRF = checkAddOnPCRF(pckDataOnPCRF, productCode);
                                    String resultChargeData = addBonusData(checkAddOnPCRF, bonusBytes, dataValidDate, bn.getIsdn());

                                    if (!"0".equals(resultChargeData)) {
                                        logger.warn("Fail to add data resultChargeData " + bn.getIsdn() + " errcode resultChargeData " + resultChargeData);
                                        bn.setAddDataDesc(resultChargeData);
                                        bn.setAddDataStatus("1");
                                    } else {
                                        db.sendSms(bn.getIsdn(), bonusMsgToCusSuccess, "86904");
                                        logger.warn("Add data successfully " + bn.getIsdn() + " errcode resultChargeData " + resultChargeData);
                                        bn.setAddDataDesc("Add data successfully , value : " + (bonusBytes / 1024 / 1024));
                                        bn.setDataValuesAdded(bonusBytes / 1024 / 1024);
                                        bn.setAddDataStatus("0");
                                        bn.setImeiHS(imeiHS);
                                    }
                                } else {
                                    logger.warn("The isdn does not generate data on 3G site in 3 last months " + bn.getIsdn());
                                    bn.setAddDataDesc("The isdn does not generate data on 3G site in 3 last months");
                                    bn.setAddDataStatus("1");
                                }
                            } else {
                                logger.warn("The number already received promotion " + bn.getIsdn());
                                bn.setAddDataDesc("The number already received promotion");
                                bn.setAddDataStatus("0");
                                addEmolaForCust = false;
                                addEmolaForStaff = false;
                            }
                        }

                    } else {
                        db.sendSms(bn.getIsdn(), bonusMsgToCusWarning, "86904");
                        db.insertWaitingBonusQueue(bn);
                        logger.warn("The handset isn't 4G or IMEI invalid so add to queue waiting for 30 days " + bn.getIsdn());
                        bn.setAddDataDesc("The handset wasn't online on 4G network or the IMEI invalid.");
                        bn.setAddDataStatus("1");
                    }
                } else {
                    logger.warn("Don't add data. Cannot find the config for data promotion or change by USSD " + bn.getIsdn());
                    bn.setAddDataDesc("Cannot find the configuration of data promotion");
                    bn.setAddDataStatus("1");
                }
                //</editor-fold>
                if ("USSD".equals(bn.getChannelType())) {
                    if (addEmolaForCust) {
                        //<editor-fold defaultstate="collapsed" desc="bonus emola for customer">
                        logger.warn("Change sim using USSD so bonus emola for customer isdn: " + bn.getIsdn() + " channel type " + bn.getChannelType());
                        Customer customer = db.getCustomerByIsdn(bn.getIsdn());
                        if (customer != null) {
                            boolean checkEwalletAccount = pro.createEmolaForCustomerV2(customer, bn.getIsdn(), db);
                            if (checkEwalletAccount) {
                                valueBonusForCust = getBonusValues(bonus4GForCustEmola);
                                if (valueBonusForCust > 0) {
                                    String eWalletResponse = pro.callEwallet(bn.getActionAuditId(), 0, bn.getIsdn(), valueBonusForCust, actionCodeBonusEmola, "CUSTOMER", sdf.format(new Date()), db);
                                    bn.seteWalletErrCode(eWalletResponse);
                                    if ("01".equals(eWalletResponse)) {
                                        logger.info("Pay Bonus for customer success for actionId " + bn.getActionAuditId() + " isdnEmola "
                                                + bn.getIsdn() + " amount " + valueBonusForCust + " channel type " + bn.getChannelType());
                                        bn.setBonusDesc("[CUSTOMER]Bonus eMola Successfully, amount " + valueBonusForCust);
                                        bn.setResultCode(eWalletResponse);
                                        if (bonus4GMsgForCustSuccess != null && bonus4GMsgForCustSuccess.length() > 0) {
                                            String tempMss = bonus4GMsgForCustSuccess.replace("%SIM_SERIAL_ NUMBER%", bn.getNewSerial());
                                            db.sendSms(bn.getIsdn(), tempMss, "86904");
                                        }
                                    } else {
                                        logger.error("Bonus for customer failed , actionId " + bn.getActionAuditId()
                                                + " isdnEmola " + bn.getIsdn() + " amount " + valueBonusForCust);
                                        bn.setBonusDesc("[CUSTOMER]Bonus eMola falied, amount " + valueBonusForCust);
                                        bn.setResultCode(eWalletResponse);
                                    }
                                } else {
                                    logger.error("Not bonus, the bonus value config is invalid, actionId " + bn.getActionAuditId()
                                            + " isdnEmola " + bn.getIsdn() + " amount " + valueBonusForCust);
                                    bn.setBonusDesc("[CUSTOMER]Not bonus, the bonus value config is invalid, amount " + valueBonusForCust);
                                    bn.setResultCode("E10");
                                }
                            } else {
                                logger.info("Falid to check account emola, try to create failed." + bn.getActionAuditId() + " isdnEmola "
                                        + bn.getIsdn() + " amount " + valueBonusForCust + " channel type " + bn.getChannelType());
                                bn.setBonusDesc("[CUSTOMER]Falid to check account emola, try to create failed");
                                bn.setResultCode("E11");
                            }
                        } else {
                            logger.info("Cannot find customer information." + bn.getActionAuditId() + " isdnEmola "
                                    + bn.getIsdn() + " amount " + valueBonusForCust + " channel type " + bn.getChannelType());
                            bn.setBonusDesc("[CUSTOMER]Cannot find customer information");
                            bn.setResultCode("E12");
                        }
                        //</editor-fold>
                    } else {
                        logger.info("Customer already received promotion." + bn.getActionAuditId() + " isdn "
                                + bn.getIsdn() + " channel type " + bn.getChannelType());
                        bn.setBonusDesc("[CUSTOMER]The isdn already received promotion");
                        bn.setResultCode("E14");
                    }

                } else {
                    //<editor-fold defaultstate="collapsed" desc="bonus emola for staff ">
                    if (addEmolaForStaff) {
                        valuesBonusForStaff = getBonusValues(bonusForChannelEmola);
                        if (valuesBonusForStaff > 0) {
                            Agent staffInfo = db.getAgentInfoByUser(bn.getStaffCode());
                            if (staffInfo.getIsdnWallet() != null && staffInfo.getIsdnWallet().length() > 0 && valuesBonusForStaff > 0) {
                                String eWalletResponse = pro.callEwallet(bn.getActionAuditId(), staffInfo.getChannelTypeId(), staffInfo.getIsdnWallet(), valuesBonusForStaff, actionCodeBonusEmola, bn.getStaffCode(), sdf.format(new Date()), db);
                                bn.seteWalletErrCode(eWalletResponse);
                                if ("01".equals(eWalletResponse)) {
                                    logger.info("Pay Bonus for Channel success for actionId " + bn.getActionAuditId() + " isdnEmola "
                                            + staffInfo.getIsdnWallet() + " amount " + valuesBonusForStaff + " isdn " + bn.getIsdn());
                                    bn.setBonusDesc("Bonus eMola Successfully, amount " + valuesBonusForStaff);
                                    bn.setResultCode(eWalletResponse);
                                    db.sendSms(staffInfo.getIsdnWallet(), bonusMsgToChannelChangeTo4G, "86904");
                                } else {
                                    logger.error("Pay Bonus fail for channel actionId " + bn.getActionAuditId()
                                            + " isdnEmola " + staffInfo.getIsdnWallet() + " amount " + valuesBonusForStaff);
                                    bn.setResultCode("E3");
                                    bn.setBonusDesc("Pay Emola fail for channel isdnEmola " + staffInfo.getIsdnWallet() + " amount " + valuesBonusForStaff + ", time :" + (System.currentTimeMillis() - timeSt));
                                }
                            } else {//Bonus failed. Don't have isdn wallet, cannot pay comission
                                logger.warn(bn.getStaffCode() + "Bonus failed. Don't have isdn wallet, cannot pay comission " + bn.getActionAuditId() + ", duration: " + (System.currentTimeMillis() - timeSt));
                                bn.setBonusDesc("Don't have isdn wallet " + bn.getStaffCode());
                                bn.setResultCode("E5");
                            }

                        } else {//Bonus failed. Cannot get bonus value
                            logger.warn(bn.getStaffCode() + "Not support bonus emola " + bn.getActionAuditId() + ", duration: " + (System.currentTimeMillis() - timeSt));
                            bn.setBonusDesc("Not bonus, the bonus value config is invalid");
                            bn.setResultCode("E8");
                        }
                    } else {
                        logger.warn(bn.getStaffCode() + "The satff already received promotion " + bn.getActionAuditId() + ", duration: " + (System.currentTimeMillis() - timeSt));
                        bn.setBonusDesc("The satff already received promotion");
                        bn.setResultCode("E15");
                    }
                    //</editor-fold>
                }

            } else {
                logger.debug("Function not supported "
                        + "MISDN: " + bn.getIsdn() + ", new serial " + bn.getNewSerial() + ", staff_code " + bn.getStaffCode()
                        + " the data configuration is " + bonusBytes + ". Dont add data.");
                bn.setAddDataDesc("The function not supported. Not change from 3G to 4G");
                bn.setAddDataStatus("1");
                bn.setBonusDesc("The function not supported. Not change from 3G to 4G");
                bn.setResultCode("E12");
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
     * @param vasCode
     * @param lstDatamap
     * @param bonusChannelStr
     * @return
     */
    public long getBonusValues(String bonusValues) {
        try {
            if (bonusValues != null || bonusValues.length() != 0) {
                return Long.valueOf(bonusValues);
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
