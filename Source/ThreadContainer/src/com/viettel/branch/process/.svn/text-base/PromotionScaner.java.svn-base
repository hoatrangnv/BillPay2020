/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.branch.process;

import com.viettel.paybonus.process.*;
import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbPromotionScaner;
import com.viettel.paybonus.obj.BranchPromotionConfig;
import com.viettel.paybonus.obj.BranchPromotionSub;
import com.viettel.paybonus.service.Exchange;
import com.viettel.threadfw.manager.AppManager;
import java.util.List;
import org.apache.log4j.Logger;
import com.viettel.threadfw.process.ProcessRecordAbstract;
import com.viettel.vas.util.ExchangeClientChannel;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.MissingResourceException;
import java.util.ResourceBundle;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class PromotionScaner extends ProcessRecordAbstract {

    Exchange pro;
    DbPromotionScaner db;
    ArrayList<String> listErrNotEnoughBalance;
    String hcErrCodeExistPricePlan;
    SimpleDateFormat sdf2;
    String olaSystemError;
    String olaNotEnoughMoney;
    String olaMsgCommssionStaff;
    String olaMsgCommssionChannel;
    String olaMsgNotInZone;
    String olaServiceNamePCRF;
    String olaQuotaNamePCRF;
    List<BranchPromotionConfig> lstConfig;
    BranchPromotionConfig branchConfig;
    String prefixRoutingvOCS;
    String[] arrPrefixRoutingvOCS;

    public PromotionScaner() {
        super();
        logger = Logger.getLogger(PayBonusConnectKit.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
        db = new DbPromotionScaner("dbvas", logger);
        olaMsgNotInZone = ResourceBundle.getBundle("configPayBonus").getString("olaMsgNotInZone");
        olaServiceNamePCRF = ResourceBundle.getBundle("configPayBonus").getString("olaServiceNamePCRF");
        olaQuotaNamePCRF = ResourceBundle.getBundle("configPayBonus").getString("olaQuotaNamePCRF");
        olaMsgCommssionStaff = ResourceBundle.getBundle("configPayBonus").getString("olaMsgCommssionStaff");
        olaMsgCommssionChannel = ResourceBundle.getBundle("configPayBonus").getString("olaMsgCommssionChannel");
        olaNotEnoughMoney = ResourceBundle.getBundle("configPayBonus").getString("olaNotEnoughMoney");
        olaSystemError = ResourceBundle.getBundle("configPayBonus").getString("olaSystemError");
        hcErrCodeExistPricePlan = ResourceBundle.getBundle("configPayBonus").getString("hcErrCodeExistPricePlan");
        listErrNotEnoughBalance = new ArrayList<String>(Arrays.asList(ResourceBundle.getBundle("configPayBonus").getString("ERR_BALANCE_NOT_ENOUGH").split("\\|")));
        sdf2 = new SimpleDateFormat("yyyyMMddHHmmss");
        lstConfig = db.getConfig();

        try {
            prefixRoutingvOCS = ResourceBundle.getBundle("configPayBonus").getString("prefixRoutingvOCS");
            arrPrefixRoutingvOCS = prefixRoutingvOCS.split("\\|");
        } catch (MissingResourceException ex) {
            ex.printStackTrace();
            prefixRoutingvOCS = null;
        }
    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        for (Record record : listRecord) {
            BranchPromotionSub moRecord = (BranchPromotionSub) record;
            moRecord.setNodeName(holder.getNodeName());
            moRecord.setClusterName(holder.getClusterName());
        }
        return listRecord;
    }

    @Override
    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
        List<Record> listResult = new ArrayList<Record>();
        List<BranchPromotionSub> listUpdate = new ArrayList<BranchPromotionSub>();
        long days;
        String smsValue;
        String voiceValue;
        String dataValue;
        String moneyFee;
        String ppSmsVoice;
        String ppVoiceInternational;
        String tmpCommission;
        String commStaff;
        String commChannel;
        String mscNumCus;
        String cellIdCus;
        String cellCodeCus;
        String resultAddVoiceInternational;
        String resultAddSmsOffNet;
        String resultAddVoiceOffNet;
        String resultAddPrice;
        String resultCheckSubPCRF;
        String resultChargeMoney;
        String resultAddSubPCRF;
        String resultAddService;
        String resultUpdateQuota;
        String resultCheckServicePCRF;
        String resultUpdateServicePCRF;
        String resultRechargeQuota;
        String msgBuyOlaSuccess;
        String resultAddData;
        boolean isRoutingvOCS;

        for (Record record : listRecord) {
            msgBuyOlaSuccess = "";
            moneyFee = "";
            dataValue = "";
            voiceValue = "";
            smsValue = "";
            ppSmsVoice = "";
            ppVoiceInternational = "";
            mscNumCus = "";
            cellIdCus = "";
            cellCodeCus = "";
            tmpCommission = "";
            commStaff = "";
            commChannel = "";
            resultAddVoiceInternational = "";
            resultAddSmsOffNet = "";
            resultAddVoiceOffNet = "";
            resultAddPrice = "";
            resultChargeMoney = "";
            resultCheckSubPCRF = "";
            resultAddSubPCRF = "";
            resultAddService = "";
            resultUpdateQuota = "";
            resultCheckServicePCRF = "";
            resultUpdateServicePCRF = "";
            resultRechargeQuota = "";
            resultAddData = "";
            isRoutingvOCS = false;

            BranchPromotionSub bn = (BranchPromotionSub) record;

            String tmpIsdn = bn.getIsdn();
            if (!tmpIsdn.startsWith("258")) {
                tmpIsdn = "258" + bn.getIsdn();
            }
            for (String tmpPrefix : arrPrefixRoutingvOCS) {
                if (tmpIsdn.startsWith(tmpPrefix)) {
                    logger.info("Routing number to vOCS, prefix: " + tmpPrefix + ", isdn: " + tmpIsdn);
                    isRoutingvOCS = true;
                    break;
                }
            }
            if (!isRoutingvOCS && prefixRoutingvOCS != null && prefixRoutingvOCS.isEmpty()) {
                logger.info("Don't have config for prefix routing isdn to ocs, default is routing to vOCS, isdn: " + tmpIsdn);
                isRoutingvOCS = true;
            }

            listResult.add(bn);
            if ("0".equals(bn.getResultCode())) {
//                Check over 30 day from create time
                long diff = new Date().getTime() - bn.getCreateTime().getTime();
                days = TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);
                days++; //add one more day
                if (days > 7) {
                    logger.warn("Over 7 days sub " + bn.getIsdn() + " not yet online so ignore now id " + bn.getActionAuditId());
                    bn.setResultCode("E1");
                    bn.setDescription("Over 7 days sub not yet online");
                    db.deletePromotionQueue(bn);
                    db.insertMoHis(bn);
                    continue;
                }
                if (lstConfig.isEmpty()) {
                    logger.warn("Can not get list of config for branch with ISDN= " + bn.getIsdn());
                    bn.setResultCode("E2");
                    bn.setDescription("Can not get list of config for branch");
                    db.deletePromotionQueue(bn);
                    db.insertMoHis(bn);
                    continue;
                }
                for (BranchPromotionConfig obj : lstConfig) {
                    if (obj.getPromotionCode().equalsIgnoreCase(bn.getProductCode())) {
                        branchConfig = obj;
                        break;
                    }
                }

                if (branchConfig == null) {
                    logger.warn("Can not get config for branch with ISDN= " + bn.getIsdn());
                    bn.setResultCode("E3");
                    bn.setDescription("Can not get config for branch");
                    db.deletePromotionQueue(bn);
                    db.insertMoHis(bn);
                    continue;
                }
////            Step 1: Check sub online GAZA/INH or not
                mscNumCus = pro.getMSCInfor(bn.getIsdn(), "");
                if (mscNumCus.trim().length() <= 0) {
                    logger.warn("Can not get mscNumCus for customer with ISDN= " + bn.getIsdn());
                    bn.setCountProcess(bn.getCountProcess() + 1);
                    listUpdate.add(bn);
                    continue;
                }
//            Step 2: 
                cellIdCus = pro.getCellIdRsString(bn.getIsdn(), mscNumCus, "");
                if (cellIdCus.trim().length() <= 0) {
                    logger.warn("Can not get cellIdChannel with mscNumChannel " + mscNumCus);
                    bn.setCountProcess(bn.getCountProcess() + 1);
                    listUpdate.add(bn);
                    continue;
                }
                String[] arrCellId = cellIdCus.split("\\|");
                if ((arrCellId != null) && (arrCellId.length == 2)) {
                    cellCodeCus = db.getCell("", arrCellId[0], arrCellId[1]);
                    if (cellCodeCus.trim().length() <= 0) {
                        logger.warn("Can not get cellIdChannel " + bn.getIsdn() + ", cellId: " + arrCellId[0] + "|" + arrCellId[1]);
                        bn.setCountProcess(bn.getCountProcess() + 1);
                        listUpdate.add(bn);
                        continue;
                    }
                } else {
                    logger.warn("Invalid cellIdChannel " + bn.getIsdn());
                    bn.setCountProcess(bn.getCountProcess() + 1);
                    listUpdate.add(bn);
                    continue;
                }
                if (!db.checkZone(cellCodeCus, bn.getProductCode())) {
                    logger.warn("Phone number of customer not in zone with staff_code " + bn.getCreateUser()
                            + " and isdn= " + bn.getIsdn() + " promotioncode " + bn.getProductCode());
                    bn.setResultCode("E4");
                    bn.setDescription("Phone number of customer not in zone " + cellCodeCus);
                    db.deletePromotionQueue(bn);
                    db.insertMoHis(bn);
                    db.sendSms(bn.getIsdn(), olaMsgNotInZone.replace("%PACKAGE%", bn.getProductCode()), "155");
                    continue;
                }
                smsValue = branchConfig.getSmsOutNetValue();
                logger.warn("Sms value will be add " + smsValue + ", isdn: " + bn.getIsdn());
                voiceValue = branchConfig.getCallOutNetValue();
                logger.warn("Voice value will be add " + voiceValue + ", isdn: " + bn.getIsdn());
                dataValue = branchConfig.getDataValue();
                long convertDataHighSpeed = 0;
                long convertDataNormal = 0;
                if (dataValue != null && dataValue.trim().length() > 0) {
                    convertDataHighSpeed = 1024 * Long.valueOf(dataValue);
                    convertDataNormal = 1024 * 1024 * Long.valueOf(dataValue);
                }
                logger.warn("Data value will be add " + dataValue + ", isdn: " + bn.getIsdn()
                        + " convertDataHighSpeed " + convertDataHighSpeed + " convertDataNormal " + convertDataNormal);
                moneyFee = branchConfig.getMoneyFee() + "";
                logger.warn("Money value will be charge " + moneyFee + ", isdn: " + bn.getIsdn());
                ppSmsVoice = branchConfig.getCallSmsOnNetPlan();
                logger.warn("Price plan free voice, sms value will be add " + ppSmsVoice + ", isdn: " + bn.getIsdn());
                ppVoiceInternational = branchConfig.getInterCallPlan();
                logger.warn("Price plan value will be add call international " + ppVoiceInternational + ", isdn: " + bn.getIsdn());
                tmpCommission = branchConfig.getCommision();
                logger.warn("Commission config: " + tmpCommission + ", isdn: " + bn.getIsdn());
                if (tmpCommission.isEmpty()) {
                    logger.warn("Config commission not yet config isdn= " + bn.getIsdn());
                    tmpCommission = "0-0^0-0";
                }
                if (!isRoutingvOCS) {
                    resultChargeMoney = pro.addMoney(bn.getIsdn(), "-" + moneyFee, "2000");
                } else {
                    resultChargeMoney = pro.addMoney(bn.getIsdn(), "-" + moneyFee, "1");
                }
                bn.setMoneyFee(Long.valueOf(moneyFee));
                if (listErrNotEnoughBalance.contains(resultChargeMoney)) {
                    logger.warn("Not enough money buy " + bn.getIsdn() + " money " + moneyFee
                            + " errcode chargemoney " + resultChargeMoney);
                    bn.setResultCode("E5");
                    bn.setDescription("Not enough money buy");
                    db.deletePromotionQueue(bn);
                    db.insertMoHis(bn);
                    db.sendSms(bn.getIsdn(), olaNotEnoughMoney, "155");
                    continue;
                } else if (!"0".equals(resultChargeMoney)) {
                    logger.warn("Fail to charge money " + bn.getIsdn() + " money " + moneyFee
                            + " errcode chargemoney " + resultChargeMoney);
                    bn.setResultCode("E6");
                    bn.setDescription("Fail to charge money");
                    db.deletePromotionQueue(bn);
                    db.insertMoHis(bn);
                    db.sendSms(bn.getIsdn(), olaSystemError, "155");
                    continue;
                }
//            Step 3: Add free call onnet,sms
//            Step 3.1: Free sms, voice onnet... Add priceplan: 10195033
                if (ppSmsVoice != null && ppSmsVoice.trim().length() > 0) {
                    resultAddPrice = pro.addPrice(bn.getIsdn(), ppSmsVoice, "", "30");
                    if (!"0".equals(resultAddPrice) && !hcErrCodeExistPricePlan.equals(resultAddPrice)) {
                        logger.warn("Fail to add priceplan " + bn.getIsdn() + " errcode " + resultAddPrice + " now rollback money");
                        bn.setResultCode("E8");
                        bn.setDescription("Fail to add priceplan, must be rollback money");
                        db.deletePromotionQueue(bn);
                        db.insertMoHis(bn);
                        db.sendSms(bn.getIsdn(), olaSystemError, "155");
                        if (!isRoutingvOCS) {
                            pro.addMoney(bn.getIsdn(), moneyFee, "2000");
                        } else {
                            pro.addMoney(bn.getIsdn(), moneyFee, "1");
                        }

                        continue;
                    }
                }
                Date sysdate = new Date();
                Calendar calExpireTime = Calendar.getInstance();
                calExpireTime.setTime(sysdate);
                calExpireTime.add(Calendar.DATE, 30);
//            Step 3.2: Add Sms offnet...
                if (smsValue != null && smsValue.trim().length() > 0) {
                    resultAddSmsOffNet = pro.addSmsDataVoice(bn.getIsdn(), smsValue, "4200", sdf2.format(calExpireTime.getTime()));
                    if (!"0".equals(resultAddSmsOffNet)) {
                        logger.warn("Fail to add sms offnet " + bn.getIsdn() + " errcode " + resultAddSmsOffNet + " now rollback money");
                        bn.setResultCode("E9");
                        bn.setDescription("Fail to add free sms offnet, must be rollback money");
                        db.deletePromotionQueue(bn);
                        db.insertMoHis(bn);
                        db.sendSms(bn.getIsdn(), olaSystemError, "155");
                        pro.removePrice(bn.getIsdn(), ppSmsVoice);
                        if (!isRoutingvOCS) {
                            pro.addMoney(bn.getIsdn(), moneyFee, "2000");
                        } else {
                            pro.addMoney(bn.getIsdn(), moneyFee, "1");
                        }
                        continue;
                    }
                }
                if (voiceValue != null && voiceValue.trim().length() > 0) {
                    resultAddVoiceOffNet = pro.addSmsDataVoice(bn.getIsdn(), voiceValue, "5005", sdf2.format(calExpireTime.getTime()));
                    if (!"0".equals(resultAddVoiceOffNet)) {
                        logger.warn("Fail to add voice offnet " + bn.getIsdn() + " errcode " + resultAddVoiceOffNet + " now rollback money");
                        bn.setResultCode("E10");
                        bn.setDescription("Fail to add voice offnet, must be rollback money");
                        db.deletePromotionQueue(bn);
                        db.insertMoHis(bn);
                        db.sendSms(bn.getIsdn(), olaSystemError, "155");
                        pro.addSmsDataVoice(bn.getIsdn(), "-" + smsValue, "4200", sdf2.format(calExpireTime.getTime()));
                        pro.removePrice(bn.getIsdn(), ppSmsVoice);
                        if (!isRoutingvOCS) {
                            pro.addMoney(bn.getIsdn(), moneyFee, "2000");
                        } else {
                            pro.addMoney(bn.getIsdn(), moneyFee, "1");
                        }
                        continue;
                    }
                }
//                Step 3.3: Add voice international...
                if (ppVoiceInternational != null && ppVoiceInternational.trim().length() > 0) {
                    resultAddVoiceInternational = pro.addPrice(bn.getIsdn(), ppVoiceInternational, "", "30");
                    if (!"0".equals(resultAddVoiceInternational) && !"102010228".equals(resultAddVoiceInternational)) {//102010228: The offer 12705037 cannot be subscribed to repeatedly.
                        logger.warn("Fail to add voice international " + bn.getIsdn() + " errcode " + resultAddVoiceInternational + " now rollback money");
                        bn.setResultCode("E11");
                        bn.setDescription("Fail to add voice international, must be rollback money");
                        db.deletePromotionQueue(bn);
                        db.insertMoHis(bn);
                        db.sendSms(bn.getIsdn(), olaSystemError, "155");
                        pro.removePrice(bn.getIsdn(), ppSmsVoice);
                        pro.addSmsDataVoice(bn.getIsdn(), "-" + smsValue, "4200", sdf2.format(calExpireTime.getTime()));
                        pro.addSmsDataVoice(bn.getIsdn(), "-" + voiceValue, "5005", sdf2.format(calExpireTime.getTime()));
                        if (!isRoutingvOCS) {
                            pro.addMoney(bn.getIsdn(), moneyFee, "2000");
                        } else {
                            pro.addMoney(bn.getIsdn(), moneyFee, "1");
                        }
                        continue;
                    }
                }
                msgBuyOlaSuccess = branchConfig.getMessage();
                logger.info("Sms will be send " + msgBuyOlaSuccess + ", isdn: " + bn.getIsdn());
//            LinhNBV 20190104: Buy more for DIAMOND_UNLIMITED, add Bonus on PCRF...
                if (branchConfig.getDataId() != null && "pcrf".equalsIgnoreCase(branchConfig.getDataId())) {
                    if (!isRoutingvOCS) {
                        logger.info("Add high data for OLA500, msisdn: " + bn.getIsdn());
//                Step 3.4: Add high data for OLA500
                    Date startDate = new Date();
//                Step 1: Query PCRF_INFO
                    resultCheckSubPCRF = pro.querySubPCRF(bn.getIsdn());
//                Step 2: Check PCRF_INFO
//                Step 2.1: Not Exist Sub Profile >>> Add Sub PCRF >> ADD SERVICE >>> UPDATE QUOTA 10GB
                        if (!resultCheckSubPCRF.isEmpty() && !"0".equals(resultCheckSubPCRF)) {
                            resultAddSubPCRF = pro.addSubViaPCRF(bn.getIsdn());
                            if (!"0".equals(resultAddSubPCRF)) {
                                logger.warn("Fail to add subPCRF " + bn.getIsdn() + " errcode " + resultAddSubPCRF + " now rollback money, remove price_plan");
                                bn.setResultCode("E12");
                                bn.setDescription("Fail to add subPCRF, must be rollback money");
                                db.deletePromotionQueue(bn);
                                db.insertMoHis(bn);
                                db.sendSms(bn.getIsdn(), olaSystemError, "155");
                                pro.removePrice(bn.getIsdn(), ppSmsVoice);
                                pro.removePrice(bn.getIsdn(), ppVoiceInternational);
                                pro.addSmsDataVoice(bn.getIsdn(), "-" + smsValue, "4200", sdf2.format(calExpireTime.getTime()));
                                pro.addSmsDataVoice(bn.getIsdn(), "-" + voiceValue, "5005", sdf2.format(calExpireTime.getTime()));
                                pro.addMoney(bn.getIsdn(), moneyFee, "2000");
                                continue;
                            }
                            resultAddService = pro.addSubServiceViaPCRF(bn.getIsdn(), olaServiceNamePCRF, sdf2.format(calExpireTime.getTime()));
                            if (!"0".equals(resultAddService)) {
                                logger.warn("Fail to add Service " + bn.getIsdn() + " errcode " + resultAddService + " now rollback money, remove price_plan");
                                bn.setResultCode("E13");
                                bn.setDescription("Fail to add Service on Pcrf, must be rollback money");
                                db.deletePromotionQueue(bn);
                                db.insertMoHis(bn);
                                db.sendSms(bn.getIsdn(), olaSystemError, "155");
                                pro.removePrice(bn.getIsdn(), ppSmsVoice);
                                pro.removePrice(bn.getIsdn(), ppVoiceInternational);
                                pro.addSmsDataVoice(bn.getIsdn(), "-" + smsValue, "4200", sdf2.format(calExpireTime.getTime()));
                                pro.addSmsDataVoice(bn.getIsdn(), "-" + voiceValue, "5005", sdf2.format(calExpireTime.getTime()));
                                pro.addMoney(bn.getIsdn(), moneyFee, "2000");
                                continue;
                            }
                            resultUpdateQuota = pro.updateQuotaPCRF(bn.getIsdn(), olaQuotaNamePCRF, convertDataHighSpeed + "");
                            if (!"0".equals(resultUpdateQuota)) {
                                logger.warn("Fail to update quota " + bn.getIsdn() + " errcode " + resultUpdateQuota + " now rollback money, remove price_plan");
                                bn.setResultCode("E14");
                                bn.setDescription("Fail to update quota, must be rollback money");
                                db.deletePromotionQueue(bn);
                                db.insertMoHis(bn);
                                db.sendSms(bn.getIsdn(), olaSystemError, "155");
                                pro.removePrice(bn.getIsdn(), ppSmsVoice);
                                pro.removePrice(bn.getIsdn(), ppVoiceInternational);
                                pro.addSmsDataVoice(bn.getIsdn(), "-" + smsValue, "4200", sdf2.format(calExpireTime.getTime()));
                                pro.addSmsDataVoice(bn.getIsdn(), "-" + voiceValue, "5005", sdf2.format(calExpireTime.getTime()));
                                pro.addMoney(bn.getIsdn(), moneyFee, "2000");
                                continue;
                            }
                        } else {
//                Step 2.2: Exist Sub Profile: 258864843540
//                Step 2.2.1: Exist SERVICE_NAME (Diamond_Unlimited) >> UPDATE SERVICE (sysdate + 30) >>> RECHARGE QUOTA (10GB)
                            resultCheckServicePCRF = pro.getServiceOfSubPCRFV2(bn.getIsdn(), olaServiceNamePCRF);
                            if (resultCheckServicePCRF != null && resultCheckServicePCRF.contains(olaServiceNamePCRF)) {
                                resultUpdateServicePCRF = pro.updateServicesPCRF(bn.getIsdn(), olaServiceNamePCRF, sdf2.format(calExpireTime.getTime()), sdf2.format(startDate));
                                if (!"0".equals(resultUpdateServicePCRF)) {
                                    logger.warn("Fail to update Service " + bn.getIsdn() + " errcode " + resultUpdateServicePCRF + " now rollback money, remove price_plan");
                                    bn.setResultCode("E15");
                                    bn.setDescription("Fail to update Service on Pcrf, must be rollback money");
                                    db.deletePromotionQueue(bn);
                                    db.insertMoHis(bn);
                                    db.sendSms(bn.getIsdn(), olaSystemError, "155");
                                    pro.removePrice(bn.getIsdn(), ppSmsVoice);
                                    pro.removePrice(bn.getIsdn(), ppVoiceInternational);
                                    pro.addSmsDataVoice(bn.getIsdn(), "-" + smsValue, "4200", sdf2.format(calExpireTime.getTime()));
                                    pro.addSmsDataVoice(bn.getIsdn(), "-" + voiceValue, "5005", sdf2.format(calExpireTime.getTime()));
                                    pro.addMoney(bn.getIsdn(), moneyFee, "2000");
                                    continue;
                                }
                                resultRechargeQuota = pro.rechargeSubQuotaPCRF(bn.getIsdn(), convertDataHighSpeed + "", olaQuotaNamePCRF);
                                if (!"0".equals(resultRechargeQuota)) {
                                    logger.warn("Fail to add Service " + bn.getIsdn() + " errcode " + resultRechargeQuota + " now rollback money, remove price_plan");
                                    bn.setResultCode("E16");
                                    bn.setDescription("Fail to add Service on Pcrf, must be rollback money");
                                    db.deletePromotionQueue(bn);
                                    db.insertMoHis(bn);
                                    db.sendSms(bn.getIsdn(), olaSystemError, "155");
                                    pro.removePrice(bn.getIsdn(), ppSmsVoice);
                                    pro.removePrice(bn.getIsdn(), ppVoiceInternational);
                                    pro.addSmsDataVoice(bn.getIsdn(), "-" + smsValue, "4200", sdf2.format(calExpireTime.getTime()));
                                    pro.addSmsDataVoice(bn.getIsdn(), "-" + voiceValue, "5005", sdf2.format(calExpireTime.getTime()));
                                    pro.addMoney(bn.getIsdn(), moneyFee, "2000");
                                    continue;
                                }
                            } else {
//                Step 2.2.2: Not exist SERVICE_NAME >> ADD SERVICE (sysdate + 30) >>> UPDATE QUOTA
                                resultAddService = pro.addSubServiceViaPCRF(bn.getIsdn(), olaServiceNamePCRF, sdf2.format(calExpireTime.getTime()));
                                if (!"0".equals(resultAddService)) {
                                    logger.warn("Fail to add Service " + bn.getIsdn() + " errcode " + resultAddService + " now rollback money, remove price_plan");
                                    bn.setResultCode("E17");
                                    bn.setDescription("Fail to add Service on Pcrf, must be rollback money");
                                    db.deletePromotionQueue(bn);
                                    db.insertMoHis(bn);
                                    db.sendSms(bn.getIsdn(), olaSystemError, "155");
                                    pro.removePrice(bn.getIsdn(), ppSmsVoice);
                                    pro.removePrice(bn.getIsdn(), ppVoiceInternational);
                                    pro.addSmsDataVoice(bn.getIsdn(), "-" + smsValue, "4200", sdf2.format(calExpireTime.getTime()));
                                    pro.addSmsDataVoice(bn.getIsdn(), "-" + voiceValue, "5005", sdf2.format(calExpireTime.getTime()));
                                    pro.addMoney(bn.getIsdn(), moneyFee, "2000");
                                    continue;
                                }
                                resultUpdateQuota = pro.updateQuotaPCRF(bn.getIsdn(), olaQuotaNamePCRF, convertDataHighSpeed + "");
                                if (!"0".equals(resultUpdateQuota)) {
                                    logger.warn("Fail to update quota " + bn.getIsdn() + " errcode " + resultUpdateQuota + " now rollback money, remove price_plan");
                                    bn.setResultCode("E18");
                                    bn.setDescription("Fail to update quota must be rollback money");
                                    db.deletePromotionQueue(bn);
                                    db.insertMoHis(bn);
                                    db.sendSms(bn.getIsdn(), olaSystemError, "155");
                                    pro.removePrice(bn.getIsdn(), ppSmsVoice);
                                    pro.removePrice(bn.getIsdn(), ppVoiceInternational);
                                    pro.addSmsDataVoice(bn.getIsdn(), "-" + smsValue, "4200", sdf2.format(calExpireTime.getTime()));
                                    pro.addSmsDataVoice(bn.getIsdn(), "-" + voiceValue, "5005", sdf2.format(calExpireTime.getTime()));
                                    pro.addMoney(bn.getIsdn(), moneyFee, "2000");
                                    continue;
                                }
                            }
                        }
                    } else {
                        String result = pro.addPrice(bn.getIsdn(), "6000", "", "30");
                        if (!"0".equals(result)) {
                            logger.warn("Fail to price plan data unlimited for ola 500" + bn.getIsdn() + " errcode " + result + " now rollback money, remove price_plan");
                            bn.setResultCode("E20");
                            bn.setDescription("Fail to price plan data unlimited");
                            db.deletePromotionQueue(bn);
                            db.insertMoHis(bn);
                            db.sendSms(bn.getIsdn(), olaSystemError, "155");
                            pro.removePrice(bn.getIsdn(), ppSmsVoice);
                            pro.removePrice(bn.getIsdn(), ppVoiceInternational);
                            pro.addSmsDataVoice(bn.getIsdn(), "-" + smsValue, "4200", sdf2.format(calExpireTime.getTime()));
                            pro.addSmsDataVoice(bn.getIsdn(), "-" + voiceValue, "5005", sdf2.format(calExpireTime.getTime()));
                            pro.addMoney(bn.getIsdn(), moneyFee, "1");
                            continue;
                        }
                    }

                } else if (branchConfig.getDataId() != null && !"pcrf".equalsIgnoreCase(branchConfig.getDataId())) {
//                Add data normal
                    logger.warn("Add data normal, NOT add data on PCRF for sub " + bn.getIsdn());
                    resultAddData = pro.addSmsDataVoice(bn.getIsdn(), convertDataNormal + "",
                            branchConfig.getDataId(), sdf2.format(calExpireTime.getTime()));
                    if (!"0".equals(resultAddData)) {
                        logger.warn("Fail to add data " + bn.getIsdn() + " errcode " + resultAddData + " now rollback money, remove price_plan");
                        bn.setResultCode("E19");
                        bn.setDescription("Fail to add data, must be rollback money");
                        db.deletePromotionQueue(bn);
                        db.insertMoHis(bn);
                        db.sendSms(bn.getIsdn(), olaSystemError, "155");
                        pro.removePrice(bn.getIsdn(), ppSmsVoice);
                        pro.removePrice(bn.getIsdn(), ppVoiceInternational);
                        pro.addSmsDataVoice(bn.getIsdn(), "-" + smsValue, "4200", sdf2.format(calExpireTime.getTime()));
                        pro.addSmsDataVoice(bn.getIsdn(), "-" + voiceValue, "5005", sdf2.format(calExpireTime.getTime()));
                        pro.addSmsDataVoice(bn.getIsdn(), "-" + convertDataNormal, branchConfig.getDataId(), sdf2.format(calExpireTime.getTime()));
                        if (!isRoutingvOCS) {
                            pro.addMoney(bn.getIsdn(), moneyFee, "2000");
                        } else {
                            pro.addMoney(bn.getIsdn(), moneyFee, "1");
                        }
                        continue;
                    }
                } else {
//                Don't have config add data...
                    logger.warn("Don't have config add data for sub " + bn.getIsdn());
                }
//            Step 4: Insert Gaza Promotion Sub
                db.insertBranchPromotionSub(bn.getActionAuditId(), bn.getIsdn(), bn.getCreateUser(),
                        bn.getProductCode(), sdf2.format(calExpireTime.getTime()));
                db.insertSubRelProduct(bn.getIsdn(), bn.getProductCode());
//            Step 4: Pay bonus base on Param...
//            Step 4.1: Check staff or channel
                //                    gazaCommission=OLA500:200-0^180-20|OLA200:60-0^54-6
                logger.warn("Start pay commission for sub " + bn.getIsdn());
                String[] arrCommission = tmpCommission.split("\\^");
                if (db.checkMovitelStaff(bn.getCreateUser())) {
                    commStaff = arrCommission[0].split("\\-")[0];
                    logger.warn("Commission staff: " + commStaff + ", isdn: " + bn.getIsdn());
                    String isdnWallet = db.getIsdnWallet(bn.getCreateUser());
                    if (isdnWallet.length() > 0) {
                        String eWalletResponse = pro.callEwallet(bn.getActionAuditId(), 14,
                                isdnWallet, Long.valueOf(commStaff), "615", bn.getCreateUser(), sdf2.format(new Date()), db);
                        if ("01".equals(eWalletResponse)) {
                            logger.info("Pay Bonus success for mo_id " + bn.getActionAuditId() + " isdnEmola " + isdnWallet
                                    + " amount " + commStaff
                                    + " staff " + bn.getCreateUser()
                                    + " isdn customer" + bn.getIsdn());
//                        String msgBonusStaff = comissionConnectSuccessfully.replace("%PCKG%", productCode).replace("%SERIAL%", bn.getSimSerial()).replace("%PHONE%",bn.getIsdnCustomer()).replace("%BONUS%", bonusStaff + "");
                            String telStaff = db.getTelByStaffCode(bn.getCreateUser());
                            String msgBonusStaff = olaMsgCommssionStaff.replace("%MONEY%", commStaff).replace("%ISDN%", bn.getIsdn());
                            msgBonusStaff = msgBonusStaff.replace("%PACKAGE%", bn.getProductCode());
                            db.sendSms(telStaff, msgBonusStaff, "86142");
                            bn.setResultCode("0");
                            bn.setDescription("Add policy success, paycommission successfully");
                            db.deletePromotionQueue(bn);
                            db.insertMoHis(bn);
                            db.sendSms(bn.getIsdn(), msgBuyOlaSuccess, "155");
                            logger.info("Bonus to staff success: " + msgBonusStaff + " isdnCustomer " + bn.getIsdn());
                            continue;
                        } else {
                            logger.info("Add policy success, but fail to bonus to staff " + bn.getCreateUser()
                                    + " isdn " + bn.getIsdn());
                            bn.setResultCode("0");
                            bn.setDescription("Add policy success, but fail to bonus to staff");
                            db.deletePromotionQueue(bn);
                            db.insertMoHis(bn);
                            db.sendSms(bn.getIsdn(), msgBuyOlaSuccess, "155");
                            continue;
                        }
                    } else {
                        logger.info("Don't have isdnWallet staff " + bn.getCreateUser()
                                + " isdn " + bn.getIsdn());
                        continue;
                    }
                } else {
//                Step 4.2: Get owner of channel and paybonus first
                    commStaff = arrCommission[1].split("\\-")[0];
                    commChannel = arrCommission[1].split("\\-")[1];
                    logger.warn("Commission staff: " + commStaff + "commission channel: " + commChannel + ", isdn: " + bn.getIsdn());
                    //                        Step 4.2.1: Paycommission for channel...
                    String isdnWalletChannel = db.getIsdnWallet(bn.getCreateUser());
                    if (isdnWalletChannel.length() > 0) {
                        String eWalletResponse = pro.callEwallet(bn.getActionAuditId(), 14,
                                isdnWalletChannel, Long.valueOf(commChannel), "615", bn.getCreateUser(), sdf2.format(new Date()), db);
                        if ("01".equals(eWalletResponse)) {
                            logger.info("Pay Bonus success for mo_id " + bn.getActionAuditId() + " isdnEmola " + isdnWalletChannel
                                    + " amount " + commStaff
                                    + " staff " + bn.getCreateUser()
                                    + " isdn customer" + bn.getIsdn());
//                        String msgBonusStaff = comissionConnectSuccessfully.replace("%PCKG%", productCode).replace("%SERIAL%", bn.getSimSerial()).replace("%PHONE%",bn.getIsdnCustomer()).replace("%BONUS%", bonusStaff + "");
                            String telChannel = db.getTelByStaffCode(bn.getCreateUser());
                            String msgBonusChannel = olaMsgCommssionChannel.replace("%MONEY%", commChannel).replace("%CHANNEL%", bn.getCreateUser())
                                    .replace("%ISDN%", bn.getIsdn());
                            msgBonusChannel = msgBonusChannel.replace("%PACKAGE%", bn.getProductCode());
                            db.sendSms(telChannel, msgBonusChannel, "86142");
                            String ownerCode = db.getOwnerStaffOfChannel(bn.getCreateUser());
                            String isdnWalletOwner = db.getIsdnWallet(ownerCode);
                            if (isdnWalletOwner.length() > 0) {
                                eWalletResponse = pro.callEwallet(bn.getActionAuditId(), 14,
                                        isdnWalletOwner, Long.valueOf(commStaff), "615", bn.getCreateUser(), sdf2.format(new Date()), db);
                                if ("01".equals(eWalletResponse)) {
                                    String telStaff = db.getTelByStaffCode(ownerCode);
                                    String msgBonusStaff = olaMsgCommssionStaff.replace("%MONEY%", commStaff).replace("%ISDN%", bn.getIsdn());
                                    msgBonusStaff = msgBonusStaff.replace("%PACKAGE%", bn.getProductCode());
                                    db.sendSms(telStaff, msgBonusStaff, "86142");
                                    bn.setResultCode("0");
                                    bn.setDescription("Add policy success, paycommission successfully");
                                    db.deletePromotionQueue(bn);
                                    db.insertMoHis(bn);
                                    db.sendSms(bn.getIsdn(), msgBuyOlaSuccess, "155");
                                    logger.info("Bonus to staff success: " + msgBonusStaff + " isdnCustomer " + bn.getIsdn());
                                    continue;
                                } else {
                                    logger.info("Add policy success but Fail to bonus to staff " + bn.getCreateUser()
                                            + " isdn " + bn.getIsdn());
                                    bn.setResultCode("0");
                                    bn.setDescription("Add policy success but Fail to bonus to staff");
                                    db.deletePromotionQueue(bn);
                                    db.insertMoHis(bn);
                                    db.sendSms(bn.getIsdn(), msgBuyOlaSuccess, "155");
                                    continue;
                                }
                            } else {
                                logger.info("Don't have isdnWallet of owner " + ownerCode
                                        + " isdn " + bn.getIsdn());
                                bn.setResultCode("0");
                                bn.setDescription("Add policy success but Don't have isdnWallet of owner");
                                db.deletePromotionQueue(bn);
                                db.insertMoHis(bn);
                                db.sendSms(bn.getIsdn(), msgBuyOlaSuccess, "155");
                                continue;
                            }

                        } else {
                            logger.info("Bonus success staff, fail to bonus to channel " + bn.getCreateUser()
                                    + " isdn " + bn.getIsdn());
                            bn.setResultCode("0");
                            bn.setDescription("Add policy success, bonus success staff, fail to bonus to channel");
                            db.deletePromotionQueue(bn);
                            db.insertMoHis(bn);
                            db.sendSms(bn.getIsdn(), msgBuyOlaSuccess, "155");
                            continue;
                        }
                    } else {
                        logger.info("Don't have isdnWallet channel " + bn.getCreateUser()
                                + " isdn " + bn.getIsdn());
                        bn.setResultCode("0");
                        bn.setDescription("Add policy success, Don't have isdnWallet of channel");
                        db.deletePromotionQueue(bn);
                        db.insertMoHis(bn);
                        db.sendSms(bn.getIsdn(), msgBuyOlaSuccess, "155");
                        continue;
                    }
                }
            } else {
                logger.warn("After validate respone code is fail actionId " + bn.getActionAuditId()
                        + " id " + bn.getActionAuditId() + " isdn: " + bn.getIsdn()
                        + " so continue with other transaction");
                bn.setResultCode("0");
                bn.setDescription("After validate respone code is fail");
                db.deletePromotionQueue(bn);
                db.insertMoHis(bn);
                continue;
            }
        }
        db.updatePromotionQueue(listUpdate);
        listRecord.clear();
        Thread.sleep(1000 * 60 * 1);
        return listResult;
    }

    @Override
    public void printListRecord(List<Record> listRecord) throws Exception {
        StringBuilder br = new StringBuilder();
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        br.setLength(0);
        br.append("\r\n").
                append("|\tACTION_AUDIT_ID|").
                append("|\tisdn\t|").
                append("|\tcreate_time\t|").
                append("|\tcreate_staff\t|").
                append("|\tproductcode\t|").
                append("|\tcount_process\t|").
                append("|\tlast_process\t|");
        for (Record record : listRecord) {
            BranchPromotionSub bn = (BranchPromotionSub) record;
            br.append("\r\n").
                    append("|\t").
                    append(bn.getActionAuditId()).
                    append("||\t").
                    append(bn.getIsdn()).
                    append("||\t").
                    append((bn.getCreateTime() != null ? sdf.format(bn.getCreateTime()) : null)).
                    append("||\t").
                    append(bn.getCreateUser()).
                    append("||\t").
                    append(bn.getProductCode()).
                    append("||\t").
                    append(bn.getCountProcess()).
                    append("||\t").
                    append((bn.getLastProcess() != null ? sdf.format(bn.getLastProcess()) : null));
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
