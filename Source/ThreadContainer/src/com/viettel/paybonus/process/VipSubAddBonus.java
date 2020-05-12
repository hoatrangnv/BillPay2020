/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbVipSub;
import com.viettel.paybonus.obj.VipSubDetail;
import com.viettel.paybonus.service.Exchange;
import com.viettel.threadfw.manager.AppManager;
import java.util.List;
import org.apache.log4j.Logger;
import com.viettel.threadfw.process.ProcessRecordAbstract;
import com.viettel.vas.util.ExchangeClientChannel;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.ResourceBundle;

/**
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class VipSubAddBonus extends ProcessRecordAbstract {

    Exchange pro;
    DbVipSub db;
    String mapConfig;
    ArrayList<HashMap> lstMapPriceGroup;

    public VipSubAddBonus() {
        super();
        logger = Logger.getLogger(VipSubAddBonus.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
        db = new DbVipSub();
        mapConfig = ResourceBundle.getBundle("configPayBonus").getString("vip_sub_price_group");
        String[] tmpMap = mapConfig.split("\\|");
        lstMapPriceGroup = new ArrayList<HashMap>();
        for (String reason : tmpMap) {
            String[] tmp = reason.split("\\:");
            HashMap map = new HashMap();
            map.put(tmp[0].trim().toUpperCase(), tmp[1].trim());
            lstMapPriceGroup.add(map);
        }
    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        return listRecord;
    }

    @Override
    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
        List<Record> listResult = new ArrayList<Record>();
        String rsMoney;
        String rsSms;
        String rsSmsOut;
        String rsData;
        String rsVoice;
        String rsVoiceOut;
        String rsVoiceInOut;
        String rsMoneySpecial;
        String rsAddPrice;
        StringBuilder sbDes;
        String errCode;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        boolean isPreSub;
        boolean isPosSub;
        String msgCustomer;
        String groupType;
        String priceForGroup;
        for (Record record : listRecord) {
            msgCustomer = ResourceBundle.getBundle("configPayBonus").getString("vip_sub_message_customer");
            sbDes = new StringBuilder();
            sbDes.setLength(0);
            errCode = "";
            rsMoney = "";
            rsSms = "";
            rsSmsOut = "";
            rsData = "";
            rsVoice = "";
            rsVoiceOut = "";
            rsVoiceInOut = "";
            rsMoneySpecial = "";
            rsAddPrice = "";
            isPreSub = false;
            isPosSub = false;
            groupType = "";
            priceForGroup = "";
            VipSubDetail bn = (VipSubDetail) record;
            listResult.add(bn);
            if ("0".equals(bn.getResultCode())) {
//                Step 00: modify group
                if (bn.getGroupId() != null && bn.getGroupId() != 0) {
//                    create group
                    String[] response = pro.createGroupCUG(bn.getGroupId().toString());
                    if (response != null && ("0".equals(response[0]) || "WS_VPN_CLUSTER_EXIST".equals(response[1]))) {
                        logger.info("Create group with groupId " + bn.getGroupId().toString() + " successfully isdn ");
//                        add member to group
                        String[] rsAddMemberTmp = pro.addMemberGroupCUG(bn.getGroupId().toString(), bn.getIsdn());
                        if (rsAddMemberTmp != null && ("0".equals(rsAddMemberTmp[0]) || "WS_VPN_MEMBER_EXIST".equals(rsAddMemberTmp[1]))) {
                            logger.info("Add member " + bn.getIsdn() + " successfully with policyId " + bn.getPolicyId());
//                            find group by group_id
                            groupType = db.getGroupType(bn.getGroupId());
                            if (groupType != null) {
                                for (int i = 0; i < lstMapPriceGroup.size(); i++) {
                                    if (lstMapPriceGroup.get(i).containsKey(groupType)) {
                                        priceForGroup = String.valueOf(lstMapPriceGroup.get(i).get(groupType).toString());
                                        break;
                                    }
                                }
//                            add price for member in group                                  
                                rsAddPrice = "";
                                rsAddPrice = pro.addPrice(bn.getIsdn(), priceForGroup, "", bn.getCycleType());
                                if ("0".equals(rsAddPrice)) {
                                    logger.info("Successfully add Price " + priceForGroup + " listPrice " + bn.getPricePlan()
                                            + " isdn " + bn.getIsdn());
                                    sbDes.append(priceForGroup).append("|").append("addPrice").append("ok").append("|");
                                } else {
                                    logger.error("fail to add Price " + priceForGroup + " listPrice " + bn.getPricePlan()
                                            + " isdn " + bn.getIsdn());
                                    sbDes.append(priceForGroup).append("|").append("addPrice").append("fail").append("|");
                                }
                            }
                        } else {
                            logger.warn("Failt to add member " + bn.getIsdn() + " with policyId " + bn.getPolicyId()
                                    + " and result from OCS= " + rsAddMemberTmp);
                            bn.setResultCode("E01");
                            bn.setDescription("Failt to add member " + bn.getIsdn() + " with policyId " + bn.getPolicyId()
                                    + " and result from OCS= " + rsAddMemberTmp);
                            continue;
                        }
                    } else {
                        String rsCreateGroup = "NA";
                        if (response != null) {
                            rsCreateGroup = response[0];
                        }
                        logger.error("Fail to create group with group_id=" + bn.getGroupId().toString()
                                + " and group_name=" + bn.getPolicyId() + " and result from OCS=" + rsCreateGroup);
                        bn.setResultCode("E02");
                        bn.setDescription("Fail to create group with group_id=" + bn.getGroupId().toString()
                                + " and group_name=" + bn.getPolicyId() + " and result from OCS=" + rsCreateGroup);
                        continue;
                    }

                }
//                Step 0: calculate next_process_time
                logger.info("Start calculate next process time for sub " + bn.getIsdn()
                        + " current processtime " + sdf.format(bn.getHisNextProcessTime()));
                Calendar cal = Calendar.getInstance();
                cal.setTime(bn.getHisNextProcessTime());
                if (bn.getCycleType() != null && bn.getCycleType().trim().length() > 0) {
                    cal.add(Calendar.DATE, Integer.valueOf(bn.getCycleType().trim()));
                    bn.setNextProcessTime(new java.sql.Timestamp(cal.getTimeInMillis()));
                    logger.info("Next process time for sub " + bn.getIsdn()
                            + " " + sdf.format(bn.getNextProcessTime()));
                } else {
                    logger.warn("Do not have next process time for sub " + bn.getIsdn()
                            + " " + sdf.format(bn.getNextProcessTime()));
                }
//                if (bn.getCycleType() != null && "D".equals(bn.getCycleType().trim().toUpperCase())) {
//                    cal.add(Calendar.DATE, 1);
//                    bn.setNextProcessTime(new java.sql.Timestamp(cal.getTimeInMillis()));
//                    logger.info("Next process time for sub " + bn.getIsdn()
//                            + " " + sdf.format(bn.getNextProcessTime()));
//                } else if ("W".equals(bn.getCycleType().trim().toUpperCase())) {
//                    cal.add(Calendar.DATE, 7);
//                    bn.setNextProcessTime(new java.sql.Timestamp(cal.getTimeInMillis()));
//                    logger.info("Next process time for sub " + bn.getIsdn()
//                            + " " + sdf.format(bn.getNextProcessTime()));
//                } else if ("M".equals(bn.getCycleType().trim().toUpperCase())) {
//                    int daysInMonth = cal.getActualMaximum(Calendar.DAY_OF_MONTH);
//                    if (daysInMonth < 30) {
//                        cal.add(Calendar.DATE, 30 - (30 - daysInMonth));
//                    } else {
//                        cal.add(Calendar.DATE, 30 + (daysInMonth - 30));
//                    }
//                    bn.setNextProcessTime(new java.sql.Timestamp(cal.getTimeInMillis()));
//                    logger.info("Next process time for sub " + bn.getIsdn()
//                            + " " + sdf.format(bn.getNextProcessTime()));
//                } else {
//                    logger.warn("Do not have next process time for sub " + bn.getIsdn()
//                            + " " + sdf.format(bn.getNextProcessTime()));
//                }

                if (bn.getPrepaidMonth() != null && bn.getPrepaidMonth() > 0 && bn.getPrepaidMonth().toString().trim().length() > 0
                        && bn.getApprovalTime() != null) {
                    if (db.checkFirstMonth(bn.getVipSubDetailId())) {
                        logger.info(bn.getVipSubDetailId()
                                + " this first time so set status = 1 for next time, and set remain prepaid month = original prepaid month - 1");
                        bn.setStatus("1");
                        bn.setRemainPrepaidMonth(bn.getPrepaidMonth() - 1);
                    } else if (bn.getRemainPrepaidMonth() > 0) {
                        logger.info(bn.getVipSubDetailId()
                                + " is still in prepaid month, so set status = 1 for next time, and set approvaluser to null"
                                + " for not making sale trans more, remain prepaid month " + (bn.getRemainPrepaidMonth() - 1));
                        bn.setStatus("1");
//                        bn.setApprovalUser(""); // will be used to check making new sale trans or not
                        bn.setRemainPrepaidMonth(bn.getRemainPrepaidMonth() - 1);
                        bn.setMakeSaleTrans(1l); // will be used to check making new sale trans or not
                    } else {
                        logger.warn("Over prepaid month so change status to wait renew status = 3 for next turn " + bn.getIsdn());
                        bn.setStatus("3"); //Huynq13 20180425 chage to status 3 to wait SaleDept make request renew afterthat status change to 2 and FinalDept can approve
                        bn.setApprovalTime(null);
                        bn.setMakeSaleTrans(1l);
                        bn.setRemainPrepaidMonth(0l);
                    }
//                    Calendar cal1 = Calendar.getInstance();
//                    cal1.setTime(bn.getApprovalTime());
//                    Integer addDay = Integer.valueOf(bn.getCycleType().trim()) * Integer.valueOf(bn.getPrepaidMonth().toString());
//                    cal1.add(Calendar.DATE, addDay);
//                    Date currentTime = new Date();
//                    if (currentTime.before(cal1.getTime())) {
//                        logger.info(bn.getVipSubDetailId()
//                                + " is still in last approvaltime + cycleday*prepaidpay, so set approvaluser to null"
//                                + " for not making sale trans more");
//                        bn.setStatus("1");
//                        bn.setApprovalUser(""); // will be used to check making new sale trans or not
//                    } else {
////              Change status to waite approving for next turn    
//                        logger.warn("Over prepaid month so change status to waite approving for next turn " + bn.getIsdn());
//                        bn.setStatus("2");
//                        bn.setApprovalTime(null);
//                    }
                } else {
//              Change status to waite approving for next turn    
                    logger.warn("Not have prepaid month so change status to waite renew status = 3 for next turn " + bn.getIsdn());
                    bn.setStatus("3"); //Huynq13 20180425 chage to status 3 to wait SaleDept make request renew afterthat status change to 2 and FinalDept can approve
                    bn.setApprovalTime(null);
                }
//                Step -1 check isdn is Movitel sub
//                isPreSub = db.checkPreSub(bn.getIsdn().trim());
//                if (!isPreSub) {
//                    isPosSub = db.checkPosSub(bn.getIsdn().trim());
//                    if (!isPosSub) {
//                        logger.warn("Not Movitel subscriber " + bn.getIsdn());
//                        bn.setResultCode("E98");
//                        bn.setDescription("Not Movitel subscriber");
//                        continue;
//                    }
//                }
                boolean resultCheck = pro.checkActiveIsdn(bn.getIsdn().trim());
                if (!resultCheck) {
                    logger.warn("Not Active at Movitel OR Not Movitel subscriber " + bn.getIsdn());
                    bn.setResultCode("E98");
                    bn.setDescription("Not Active at Movitel OR Not Movitel subscriber");
                    continue;
                }
//                Step 1: Check to make sure not yet processing this record, not duplicate process for each record
                logger.info("Start check to make sure not duplicate process id " + bn.getVipSubDetailId() + " isdn " + bn.getIsdn());
                if (bn.getHisNextProcessTime() != null
                        && db.checkAlreadyProcessRecord(bn.getIsdn().trim(), sdf.format(bn.getHisNextProcessTime()))) {
                    logger.warn("Already process record actionId " + bn.getVipSubDetailId());
                    bn.setResultCode("E01");
                    bn.setDescription("Already process record");
                    continue;
                }
//                Step 2: check and add money
                if (bn.getMoneyAcc() != null && !"".equals(bn.getMoneyAcc().trim())
                        && bn.getMoneyValue() != null && !"".equals(bn.getMoneyValue().trim())) {
                    logger.info("start add money accountid " + bn.getMoneyAcc() + " value " + bn.getMoneyValue()
                            + " isdn " + bn.getIsdn());
                    rsMoney = pro.addMoney(bn.getIsdn().trim(), bn.getMoneyValue(), bn.getMoneyAcc());
                    if ("0".equals(rsMoney)) {
                        logger.info("successfully add money accountid " + bn.getMoneyAcc() + " value " + bn.getMoneyValue()
                                + " isdn " + bn.getIsdn().trim());
                        sbDes.append(bn.getMoneyAcc()).append("|").append(bn.getMoneyValue()).append("ok").append("|");
                    } else {
                        logger.error("fail to add money accountid " + bn.getMoneyAcc() + " value " + bn.getMoneyValue()
                                + " isdn " + bn.getIsdn());
                        sbDes.append(bn.getMoneyAcc()).append("|").append(bn.getMoneyValue()).append("fail").append("|");
                        errCode = "E02";
                    }
                }
//                Step 3: Check and add sms
                if (bn.getSmsAcc() != null && !"".equals(bn.getSmsAcc().trim())
                        && bn.getSmsValue() != null && !"".equals(bn.getSmsValue().trim())) {
                    logger.info("start add sms accountid " + bn.getSmsAcc() + " value " + bn.getSmsValue()
                            + " isdn " + bn.getIsdn());
                    rsSms = pro.addSmsDataVoice(bn.getIsdn().trim(), bn.getSmsValue(), bn.getSmsAcc(), sdf.format(bn.getNextProcessTime()));
                    if ("0".equals(rsSms)) {
                        logger.info("successfully add sms accountid " + bn.getSmsAcc() + " value " + bn.getSmsValue()
                                + " isdn " + bn.getIsdn());
                        sbDes.append(bn.getSmsAcc()).append("|").append(bn.getSmsValue()).append("ok").append("|");
                    } else {
                        logger.error("fail to add sms accountid " + bn.getSmsAcc() + " value " + bn.getSmsValue()
                                + " isdn " + bn.getIsdn());
                        sbDes.append(bn.getSmsAcc()).append("|").append(bn.getSmsValue()).append("fail").append("|");
                        if (!"E02".equals(errCode)) {
                            errCode = "E03";
                        } else {
                            errCode = "E04";
                        }
                    }
                }
//                Step 4: Check and add sms out
                if (bn.getSmsOutAcc() != null && !"".equals(bn.getSmsOutAcc().trim())
                        && bn.getSmsOutValue() != null && !"".equals(bn.getSmsOutValue().trim())) {
                    logger.info("start add smsout accountid " + bn.getSmsOutAcc() + " value " + bn.getSmsOutValue()
                            + " isdn " + bn.getIsdn());
                    rsSmsOut = pro.addSmsDataVoice(bn.getIsdn().trim(), bn.getSmsOutValue(), bn.getSmsOutAcc(), sdf.format(bn.getNextProcessTime()));
                    if ("0".equals(rsSmsOut)) {
                        logger.info("successfully add smsout accountid " + bn.getSmsOutAcc() + " value " + bn.getSmsOutValue()
                                + " isdn " + bn.getIsdn());
                        sbDes.append(bn.getSmsOutAcc()).append("|").append(bn.getSmsOutValue()).append("ok").append("|");
                    } else {
                        logger.error("fail to add smsout accountid " + bn.getSmsOutAcc() + " value " + bn.getSmsOutValue()
                                + " isdn " + bn.getIsdn());
                        sbDes.append(bn.getSmsOutAcc()).append("|").append(bn.getSmsOutValue()).append("fail").append("|");
                        if (!"E02".equals(errCode) && !"E03".equals(errCode) && !"E04".equals(errCode)) {
                            errCode = "E05";
                        } else if ("E04".equals(errCode)) {
                            errCode = "E06";
                        } else if ("E02".equals(errCode)) {
                            errCode = "E07";
                        } else if ("E03".equals(errCode)) {
                            errCode = "E08";
                        }
                    }
                }
//                Step 5: Check and add data
                if (bn.getDataAcc() != null && !"".equals(bn.getDataAcc().trim())
                        && bn.getDataValue() != null && !"".equals(bn.getDataValue().trim())) {
                    logger.info("start add data accountid " + bn.getDataAcc() + " value " + bn.getDataValue()
                            + " isdn " + bn.getIsdn());
                    BigInteger dataValue = new BigInteger(bn.getDataValue());
                    BigInteger dataHeigh = new BigInteger("1048576");
                    dataValue = dataValue.multiply(dataHeigh);
                    logger.info("Convert value from megabyte to byte for sub " + bn.getIsdn() + " byteValue " + String.valueOf(dataValue));
                    rsData = pro.addSmsDataVoice(bn.getIsdn(), String.valueOf(dataValue), bn.getDataAcc(), sdf.format(bn.getNextProcessTime()));
                    if ("0".equals(rsData)) {
                        logger.info("successfully add data accountid " + bn.getDataAcc() + " value " + bn.getDataValue()
                                + " isdn " + bn.getIsdn());
                        sbDes.append(bn.getDataAcc()).append("|").append(bn.getDataValue()).append("ok").append("|");
                    } else {
                        logger.error("fail to add data accountid " + bn.getDataAcc() + " value " + bn.getDataValue()
                                + " isdn " + bn.getIsdn());
                        sbDes.append(bn.getDataAcc()).append("|").append(bn.getDataValue()).append("fail").append("|");
                        if (!"E02".equals(errCode) && !"E03".equals(errCode) && !"E04".equals(errCode) && !"E05".equals(errCode)) {
                            errCode = "E09";
                        } else if ("E06".equals(errCode)) {
                            errCode = "E10";
                        } else if ("E07".equals(errCode)) {
                            errCode = "E11";
                        } else if ("E08".equals(errCode)) {
                            errCode = "E12";
                        } else if ("E02".equals(errCode)) {
                            errCode = "E13";
                        } else if ("E03".equals(errCode)) {
                            errCode = "E14";
                        } else if ("E04".equals(errCode)) {
                            errCode = "E15";
                        } else if ("E05".equals(errCode)) {
                            errCode = "E16";
                        }
                    }
                }
//                Step 6: Check and add voice
                if (bn.getVoiceAcc() != null && !"".equals(bn.getVoiceAcc().trim())
                        && bn.getVoiceValue() != null && !"".equals(bn.getVoiceValue().trim())) {
                    logger.info("start add Voice accountid " + bn.getVoiceAcc() + " value " + bn.getVoiceValue()
                            + " isdn " + bn.getIsdn());
                    rsVoice = pro.addSmsDataVoice(bn.getIsdn(), bn.getVoiceValue(), bn.getVoiceAcc(), sdf.format(bn.getNextProcessTime()));
                    if ("0".equals(rsVoice)) {
                        logger.info("successfully add Voice accountid " + bn.getVoiceAcc() + " value " + bn.getVoiceValue()
                                + " isdn " + bn.getIsdn());
                        sbDes.append(bn.getVoiceAcc()).append("|").append(bn.getVoiceValue()).append("ok").append("|");
                    } else {
                        logger.error("fail to add Voice accountid " + bn.getVoiceAcc() + " value " + bn.getVoiceValue()
                                + " isdn " + bn.getIsdn());
                        sbDes.append(bn.getVoiceAcc()).append("|").append(bn.getVoiceValue()).append("fail").append("|");
                        if (!"E02".equals(errCode) && !"E03".equals(errCode) && !"E04".equals(errCode) && !"E05".equals(errCode)
                                && !"E09".equals(errCode)) {
                            errCode = "E17";
                        } else if ("E06".equals(errCode)) {
                            errCode = "E18";
                        } else if ("E07".equals(errCode)) {
                            errCode = "E19";
                        } else if ("E08".equals(errCode)) {
                            errCode = "E20";
                        } else if ("E02".equals(errCode)) {
                            errCode = "E21";
                        } else if ("E03".equals(errCode)) {
                            errCode = "E22";
                        } else if ("E04".equals(errCode)) {
                            errCode = "E22";
                        } else if ("E05".equals(errCode)) {
                            errCode = "E23";
                        } else if ("E10".equals(errCode)) {
                            errCode = "E24";
                        } else if ("E11".equals(errCode)) {
                            errCode = "E25";
                        } else if ("E12".equals(errCode)) {
                            errCode = "E26";
                        } else if ("E13".equals(errCode)) {
                            errCode = "E27";
                        } else if ("E14".equals(errCode)) {
                            errCode = "E28";
                        } else if ("E15".equals(errCode)) {
                            errCode = "E29";
                        } else if ("E16".equals(errCode)) {
                            errCode = "E30";
                        } else if ("E09".equals(errCode)) {
                            errCode = "E31";
                        }
                    }
                }
//                Step 7: Check and add voice out
                if (bn.getVoiceOutAcc() != null && !"".equals(bn.getVoiceOutAcc().trim())
                        && bn.getVoiceOutValue() != null && !"".equals(bn.getVoiceOutValue().trim())) {
                    logger.info("start add VoiceOut accountid " + bn.getVoiceOutAcc() + " value " + bn.getVoiceOutValue()
                            + " isdn " + bn.getIsdn());
                    rsVoiceOut = pro.addSmsDataVoice(bn.getIsdn(), bn.getVoiceOutValue(), bn.getVoiceOutAcc(), sdf.format(bn.getNextProcessTime()));
                    if ("0".equals(rsVoiceOut)) {
                        logger.info("successfully add VoiceOut accountid " + bn.getVoiceOutAcc() + " value " + bn.getVoiceOutValue()
                                + " isdn " + bn.getIsdn());
                        sbDes.append(bn.getVoiceOutAcc()).append("|").append(bn.getVoiceOutValue()).append("ok").append("|");
                    } else {
                        logger.error("fail to add VoiceOut accountid " + bn.getVoiceOutAcc() + " value " + bn.getVoiceOutValue()
                                + " isdn " + bn.getIsdn());
                        sbDes.append(bn.getVoiceOutAcc()).append("|").append(bn.getVoiceOutValue()).append("fail").append("|");
                    }
                }
//                Step 8: Add Price
                if (bn.getPricePlan() != null && bn.getPricePlan().trim().length() > 0) {
//                    20181001 start change to support list of priceplan
                    String[] lstPrice = bn.getPricePlan().split("\\|");
                    for (String price : lstPrice) {
                        logger.info("start add priceplan " + price + " listPrice " + bn.getPricePlan()
                                + " isdn " + bn.getIsdn());
                        rsAddPrice = "";
                        rsAddPrice = pro.addPrice(bn.getIsdn(), price, "", bn.getCycleType());
                        if ("0".equals(rsAddPrice)) {
                            logger.info("successfully add Price " + price + " listPrice " + bn.getPricePlan()
                                    + " isdn " + bn.getIsdn());
                            sbDes.append(price).append("|").append("addPrice").append("ok").append("|");
                        } else {
                            logger.error("fail to add Price " + price + " listPrice " + bn.getPricePlan()
                                    + " isdn " + bn.getIsdn());
                            sbDes.append(price).append("|").append("addPrice").append("fail").append("|");
                        }
                    }
                }
//                Huynq13 20180521 add to support new accounts
//                Step 9: Check and add voice in out
                if (bn.getVoiceInOutAcc() != null && !"".equals(bn.getVoiceInOutAcc().trim())
                        && bn.getVoiceInOutValue() != null && !"".equals(bn.getVoiceInOutValue().trim())) {
                    logger.info("start add VoiceInOut accountid " + bn.getVoiceInOutAcc() + " value " + bn.getVoiceInOutValue()
                            + " isdn " + bn.getIsdn());
                    rsVoiceInOut = pro.addSmsDataVoice(bn.getIsdn(), bn.getVoiceInOutValue(), bn.getVoiceInOutAcc(), sdf.format(bn.getNextProcessTime()));
                    if ("0".equals(rsVoiceInOut)) {
                        logger.info("successfully add VoiceInOut accountid " + bn.getVoiceInOutAcc() + " value " + bn.getVoiceInOutValue()
                                + " isdn " + bn.getIsdn());
                        sbDes.append(bn.getVoiceInOutAcc()).append("|").append(bn.getVoiceInOutValue()).append("ok").append("|");
                    } else {
                        logger.error("fail to add VoiceInOut accountid " + bn.getVoiceInOutAcc() + " value " + bn.getVoiceInOutValue()
                                + " isdn " + bn.getIsdn());
                        sbDes.append(bn.getVoiceInOutAcc()).append("|").append(bn.getVoiceInOutValue()).append("fail").append("|");
                    }
                }
//                Step 9: Check and add money special for UTT partner
                if (bn.getMoneySpecialAcc() != null && !"".equals(bn.getMoneySpecialAcc().trim())
                        && bn.getMoneySpecialValue() != null && !"".equals(bn.getMoneySpecialValue().trim())) {
                    logger.info("start add MoneySpecial accountid " + bn.getMoneySpecialValue() + " value " + bn.getMoneySpecialValue()
                            + " isdn " + bn.getIsdn());
                    rsMoneySpecial = pro.addMoney(bn.getIsdn().trim(), bn.getMoneySpecialValue(), bn.getMoneySpecialAcc());
                    if ("0".equals(rsMoneySpecial)) {
                        logger.info("successfully add MoneySpecial accountid " + bn.getMoneySpecialAcc() + " value " + bn.getMoneySpecialValue()
                                + " isdn " + bn.getIsdn());
                        sbDes.append(bn.getMoneySpecialAcc()).append("|").append(bn.getMoneySpecialValue()).append("ok").append("|");
                    } else {
                        logger.error("fail to add MoneySpecial accountid " + bn.getMoneySpecialAcc() + " value " + bn.getMoneySpecialValue()
                                + " isdn " + bn.getIsdn());
                        sbDes.append(bn.getMoneySpecialAcc()).append("|").append(bn.getMoneySpecialValue()).append("fail").append("|");
                    }
                }
//                Step 9: Check all results                
                if (("".equals(rsMoney) || "0".equals(rsMoney))
                        && ("".equals(rsData) || "0".equals(rsData))
                        && ("".equals(rsSms) || "0".equals(rsSms))
                        && ("".equals(rsSmsOut) || "0".equals(rsSmsOut))
                        && ("".equals(rsVoice) || "0".equals(rsVoice))
                        && ("".equals(rsVoiceOut) || "0".equals(rsVoiceOut))
                        && ("".equals(rsVoiceInOut) || "0".equals(rsVoiceInOut))
                        && ("".equals(rsMoneySpecial) || "0".equals(rsMoneySpecial))) {
                    logger.info("All steps success " + bn.getIsdn());
                    bn.setResultCode("0");
                    bn.setDescription("All benefits added");
//                    Send sms to customer where finish success
                    if (msgCustomer != null && msgCustomer.trim().length() > 0) {
                        db.sendSms("258" + bn.getIsdn(), msgCustomer, "86904");
                    }
                    continue;
                } else {
                    if (!"".equals(errCode)) {
                        logger.warn("Some step fail " + bn.getIsdn());
                        bn.setResultCode(errCode);
                        bn.setDescription(sbDes.toString());
                        continue;
                    } else {
                        logger.warn("Have step fail but not define, let see description in DB " + bn.getIsdn());
                        bn.setResultCode("E99");
                        bn.setDescription(sbDes.toString());
                        continue;
                    }
                }
            } else {
                logger.warn("After validate respone code is fail id " + bn.getVipSubDetailId()
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
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        br.setLength(0);
        br.append("\r\n").
                append("|\tVIP_SUB_DETAIL_ID|").
                append("|\tVIP_SUB_INFO_ID\t|").
                append("|\tISDN\t|").
                append("|\tCYCLE_TYPE\t|").
                append("|\tPREPAID_MONTH\t|").
                append("|\tREMAIN_MONTH\t|").
                append("|\tCREATE_TIME\t|").
                append("|\tAPPROVE_TIME\t|").
                append("|\tPOLICY_ID\t|").
                append("|\tTIME_TO_RUN\t|");
        for (Record record : listRecord) {
            VipSubDetail bn = (VipSubDetail) record;
            br.append("\r\n").
                    append("|\t").
                    append(bn.getVipSubDetailId()).
                    append("||\t").
                    append(bn.getVipSubInfoId()).
                    append("||\t").
                    append(bn.getIsdn()).
                    append("||\t").
                    append(bn.getCycleType()).
                    append("||\t").
                    append(bn.getPrepaidMonth()).
                    append("||\t").
                    append(bn.getRemainPrepaidMonth()).
                    append("||\t").
                    append(bn.getCreateTime()).
                    append("||\t").
                    append((bn.getApprovalTime() != null ? sdf.format(bn.getApprovalTime()) : null)).
                    append("||\t").
                    append(bn.getPolicyId()).
                    append("||\t").
                    append((bn.getHisNextProcessTime() != null ? sdf.format(bn.getHisNextProcessTime()) : null));
        }
        logger.info(br);
    }

    @Override
    public List<Record> processException(List<Record> listRecord, Exception ex) {
//        logger.warn("TEMPLATE process exception record: " + ex.toString());
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

    public static void main(String[] args) {
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        cal.add(Calendar.DATE, -1);
        System.out.println("Ok " + sdf.format(cal.getTime()));
        BigInteger dataValue = new BigInteger("10240");
        BigInteger dataHeigh = new BigInteger("1048576");
        dataValue = dataValue.multiply(dataHeigh);
        System.out.println(String.valueOf(dataValue));
        System.out.println("doan dau" + "  thoi test  ".trim());
    }
}
