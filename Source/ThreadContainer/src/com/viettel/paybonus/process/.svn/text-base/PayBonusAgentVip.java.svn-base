/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbPayBonusAgentVipProcessor;
import com.viettel.paybonus.obj.Agent;
import com.viettel.paybonus.obj.BonusAgentVip;
import com.viettel.paybonus.obj.RateExchange;
import com.viettel.paybonus.service.Exchange;
import com.viettel.threadfw.manager.AppManager;
import java.util.List;
import org.apache.log4j.Logger;
import com.viettel.threadfw.process.ProcessRecordAbstract;
import com.viettel.vas.util.ExchangeClientChannel;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.ResourceBundle;

/**
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class PayBonusAgentVip extends ProcessRecordAbstract {

    Exchange pro;
    DbPayBonusAgentVipProcessor db;
    String payBonusBranchDirector;
    String payBonusAgentVip;
    Double bonusBrDirector, bonusBrAgentVip, bonusBrChannel;
    Double bonusAgentVip, bonusChannel;

    public PayBonusAgentVip() {
        super();
        logger = Logger.getLogger(PayBonusAgentVip.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
        db = new DbPayBonusAgentVipProcessor();
        payBonusBranchDirector = ResourceBundle.getBundle("configPayBonus").getString("payBonusBranchDirector");
        payBonusAgentVip = ResourceBundle.getBundle("configPayBonus").getString("payBonusAgentVip");
        String[] tmpBonusBrDirector = payBonusBranchDirector.split("\\|");
        for (String tmpBonus : tmpBonusBrDirector) {
            String[] arrBonus = tmpBonus.split("\\:");
            if ("LV1".equals(arrBonus[0])) {
                bonusBrDirector = Double.valueOf(arrBonus[1]);
            } else if ("LV2".equals(arrBonus[0])) {
                bonusBrAgentVip = Double.valueOf(arrBonus[1]);
            } else {
                bonusBrChannel = Double.valueOf(arrBonus[1]);
            }
        }

        String[] tmpBonusAgent = payBonusAgentVip.split("\\|");
        for (String tmpBonus : tmpBonusAgent) {
            String[] arrBonus = tmpBonus.split("\\:");
            if ("LV2".equals(arrBonus[0])) {
                bonusAgentVip = Double.valueOf(arrBonus[1]);
            } else {
                bonusChannel = Double.valueOf(arrBonus[1]);
            }
        }
    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        return listRecord;
    }

    @Override
    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
        List<Record> listResult = new ArrayList<Record>();
        Agent staffInfo;
        String staffCode;
        long timeSt;
        double totalSubMb;
        double totalSubCUG;
        double totalSubFtth;
        double targetBranch;
        double soldLv1, soldLv2, soldLv3;
        long bonusValue, bonusAgent;
        SimpleDateFormat sdf = new SimpleDateFormat("ddMMyyyyHHmmssSSS");
        for (Record record : listRecord) {
            timeSt = System.currentTimeMillis();
            staffInfo = null;
            staffCode = "";
            totalSubMb = 0;
            totalSubCUG = 0;
            totalSubFtth = 0;
            targetBranch = 0;
            soldLv1 = 0;
            soldLv2 = 0;
            soldLv3 = 0;
//            overTargetLv2 = 0;
//            overTargetLv3 = 0;
            bonusValue = 0;
            bonusAgent = 0;

            BonusAgentVip bn = (BonusAgentVip) record;
            listResult.add(bn);
            if ("0".equals(bn.getResultCode())) {
//                Check account ewallet on SM to get isdn ewallet, staff_code
                staffCode = bn.getStaffCode();
                if ("".equals(staffCode)) {
                    logger.warn("staffCode in bonus_agent_vip is null or empty, id " + bn.getId());
                    bn.setResultCode("01");
                    bn.setDescription("Staff code in bonus_agent_vip is null or empty");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    continue;
                } else {
                    bn.setStaffCode(staffCode);
                }

                staffInfo = db.getAgentInfoByUser(staffCode);
                if (staffInfo == null) {
                    logger.warn("staffInfo is null, staffCode " + staffCode);
                    bn.setResultCode("01");
                    bn.setDescription("Staff is null");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    continue;
                }
//                Sum target of BOD branches.
                targetBranch = bn.getTarget() + db.sumTargetStaffLevel2(staffCode);
                logger.info("Target of branch is: " + targetBranch + ", staffCode: " + staffCode);
//                count total sub already connect in system
//                List product for group 1. Mobile
                List<RateExchange> listRateExchange = db.getListRateExchange(1);
                if (listRateExchange.size() > 0) {
                    logger.info("Begin count total sub_mb, staffCode: " + staffCode);
                    for (RateExchange rateExchange : listRateExchange) {
                        double tmpSubMb = db.getTotalSubMb(rateExchange.getProductCode(), staffCode);
                        totalSubMb += tmpSubMb;
                    }
                }
                logger.info("End count total sub_mb, staffCode: " + staffCode + ", totalSubElite: " + totalSubMb);
//                List product for group 2. CUG
                listRateExchange.clear();
                listRateExchange = db.getListRateExchange(2);
                if (listRateExchange.size() > 0) {
                    logger.info("Begin count total sub CUG, staffCode: " + staffCode);
                    for (RateExchange rateExchange : listRateExchange) {
                        double tmpSubCUG = db.getTotalSubCUG(staffCode, rateExchange.getProductCode());
                        totalSubCUG += tmpSubCUG;
                    }
                }
                logger.info("End count totalSubCUG, staffCode: " + staffCode + ", totalSubCUG: " + totalSubCUG);
//                List product for group 3. FTTH
                listRateExchange.clear();
                listRateExchange = db.getListRateExchange(3);
                if (listRateExchange.size() > 0) {
                    logger.info("Begin count total sub FTTH, staffCode: " + staffCode);
                    for (RateExchange rateExchange : listRateExchange) {
                        double tmpSubAdsl = db.getTotalSubAdslLl(staffInfo.getStaffId(), rateExchange.getProductCode());
                        totalSubFtth += tmpSubAdsl;
                    }
                }
                listRateExchange.clear();
                logger.info("End count totalSubFtth, staffCode: " + staffCode + ", totalSubFtth: " + totalSubFtth);
                soldLv1 = totalSubMb + totalSubCUG + totalSubFtth;
                logger.info("Total sub converted to premeum staffCode: " + staffCode + " soldLv1: " + soldLv1);
                bn.setSoldLv1(soldLv1);
                if (bn.getSoldLv1() > targetBranch) {
                    logger.info("soldLv1 of staff " + staffCode + " over targetBranch: " + targetBranch);
                    bn.setOverTargetLv1Br(bn.getSoldLv1() - targetBranch);
                }
                if (bn.getSoldLv1() > bn.getTarget()) {
                    bn.setOverTargetLv1(bn.getSoldLv1() - bn.getTarget());
                }
                Long bonusAgentVipProcessId = db.getSequence("BONUS_AGENT_VIP_PROCESS_SEQ", "dbapp1");
                bn.setBonusAgentVipProcessId(bonusAgentVipProcessId);
//                Get list staff Level 2
                List<BonusAgentVip> listStaffLevel2 = db.getListStaffLevel2(staffCode);
                if (listStaffLevel2.size() > 0) {
                    for (BonusAgentVip staffLevel2 : listStaffLevel2) {
                        double tmpSubMbLv2 = 0, tmpSubCUGLv2 = 0, tmpSubAdslLv2 = 0, tmpSubMbLv3 = 0;
                        bonusAgent = 0;
                        Long bonusAgentVipProcessDetailId = db.getSequence("BONUS_AGENT_VIP_PROCESS_DT_SEQ", "dbapp1");
                        Agent tmpStaff = db.getAgentInfoByUser(staffLevel2.getStaffCode());
                        listRateExchange = db.getListRateExchange(1);
                        if (listRateExchange.size() > 0) {
                            logger.info("Begin count total sub_mb, staffLevel2: " + staffLevel2.getStaffCode());
                            for (RateExchange rateExchange : listRateExchange) {
                                double tmpSubMb = db.getTotalSubMb(rateExchange.getProductCode(), staffLevel2.getStaffCode());
                                tmpSubMbLv2 += tmpSubMb;
                                soldLv2 += tmpSubMb;
                            }
                        }
                        logger.info("End count total sub_mb, staffLevel2: " + staffLevel2.getStaffCode() + ", totalSubEliteLv2: " + tmpSubMbLv2);
//                       List product for group 2. CUG
                        listRateExchange.clear();
                        listRateExchange = db.getListRateExchange(2);
                        if (listRateExchange.size() > 0) {
                            logger.info("Begin count total sub CUG, staffLevel2: " + staffLevel2.getStaffCode());
                            for (RateExchange rateExchange : listRateExchange) {
                                double tmpSubCUG = db.getTotalSubCUG(staffLevel2.getStaffCode(), rateExchange.getProductCode());
                                tmpSubCUGLv2 += tmpSubCUG;
                                soldLv2 += tmpSubCUG;
                            }
                        }
                        logger.info("End count totalSubCUG, staffLevel2: " + staffLevel2.getStaffCode() + ", totalSubCUGLv2: " + tmpSubCUGLv2);
//                       List product for group 3. FTTH
                        listRateExchange.clear();
                        listRateExchange = db.getListRateExchange(3);
                        if (listRateExchange.size() > 0) {
                            logger.info("Begin count total sub FTTH, staffLevel2: " + staffLevel2.getStaffCode());
                            for (RateExchange rateExchange : listRateExchange) {
                                double tmpSubAdsl = db.getTotalSubAdslLl(tmpStaff.getStaffId(), rateExchange.getProductCode());
                                tmpSubAdslLv2 += tmpSubAdsl;
                                soldLv2 += tmpSubAdsl;
                            }
                        }
                        logger.info("End count totalSubFtth, staffLevel2: " + staffLevel2.getStaffCode() + ", totalSubAdslLv2: " + tmpSubAdslLv2);
//                        if ((tmpSubMbLv2 + tmpSubCUGLv2 + tmpSubAdslLv2) > staffLevel2.getTarget()) {
//                            overTargetLv2 += ((tmpSubMbLv2 + tmpSubCUGLv2 + tmpSubAdslLv2) - staffLevel2.getTarget());
//                        }
                        listRateExchange.clear();
//                        Get list staff level 3 (channel), only connect KIT 
                        List<String> lstStaffLv3 = db.getListStaffLevel3(staffLevel2.getStaffCode());
                        if (lstStaffLv3.size() > 0) {
                            logger.info("Begin count total sub mobile of channel belong staffCode: " + staffLevel2.getStaffCode());
                            for (String tmpStaffLv3 : lstStaffLv3) {
                                listRateExchange = db.getListRateExchange(1);
                                if (listRateExchange.size() > 0) {
                                    for (RateExchange rateExchange : listRateExchange) {
                                        double tmpSubMb = db.getTotalSubMb(rateExchange.getProductCode(), tmpStaffLv3);
                                        tmpSubMbLv3 += tmpSubMb;
                                        soldLv3 += tmpSubMb;
                                    }
                                }
                            }
                            logger.info("End count total sub mobile connect by channel belong staff: " + staffLevel2.getStaffCode()
                                    + ", totalSubEliteLv3: " + tmpSubMbLv3);
                        }
//                        if (tmpSubMbLv3 > targetBranch) {
//                            overTargetLv3 += (tmpSubMbLv3 - targetBranch);
//                        }
                        listRateExchange.clear();
//                        Compare total and target of staff level 2
                        if ((tmpSubMbLv2 + tmpSubCUGLv2 + tmpSubAdslLv2 + tmpSubMbLv3) > staffLevel2.getTarget()) {
                            logger.info("Reach to target of staffLevel2 " + staffLevel2.getStaffCode());
//                            Pay comission for staff level 2
                            if (tmpSubMbLv3 > staffLevel2.getTarget()) {
                                logger.info("tmpSubMbLv3 of staff " + staffLevel2.getStaffCode() + " over staffLevel2.getTarget : " + staffLevel2.getTarget());
                                bonusAgent = Math.round((tmpSubMbLv2 + tmpSubCUGLv2 + tmpSubAdslLv2) * bonusAgentVip + (tmpSubMbLv3 - staffLevel2.getTarget()) * bonusChannel);
                            } else {
                                bonusAgent = Math.round((tmpSubMbLv2 + tmpSubCUGLv2 + tmpSubAdslLv2 + tmpSubMbLv3 - staffLevel2.getTarget()) * bonusAgentVip);
                            }
                            if (bonusAgent > 0) {
//                            Get bonus last time system pay in month.
//                            real bonus = bonus(calculated) - sum (bonus in month)
                                long lastBonus = db.sumBonusValueAgentVipInMonth(staffLevel2.getStaffCode());
                                bonusAgent = bonusAgent - lastBonus;
                                if (bonusAgent > 0) {
                                    if (tmpStaff.getIsdnWallet() != null && tmpStaff.getIsdnWallet().length() > 0) {
                                        String eWalletResponse = pro.callEwallet(bonusAgentVipProcessDetailId, 0,
                                                tmpStaff.getIsdnWallet(), bonusAgent,
                                                "615", staffLevel2.getStaffCode(), sdf.format(new Date()), db);
                                        bn.seteWalletErrCode(eWalletResponse);
                                        if ("01".equals(eWalletResponse)) {
                                            bn.setResultCode("0");
                                            logger.info("Succeed to pay for staffLevel2: " + staffLevel2.getStaffCode() + ", value: " + bonusAgent);
                                            bn.setDescription("Succeed to pay for staffLevel2 value " + bonusAgent);
                                        } else {
                                            bn.setResultCode("E06");
                                            logger.info("Failt to pay bonus  staffLevel2 " + staffLevel2.getStaffCode() + ", value: " + bonusAgent);
                                            bn.setDescription("Failt to pay bonus  staffLevel2 eWalletResponse " + eWalletResponse + " value " + bonusAgent);
                                        }
                                    } else {
                                        bn.setResultCode("E05");
                                        logger.info("Can not pay : " + staffLevel2.getStaffCode() + "don't have isdn wallet, bonus value: " + bonusAgent);
                                        bn.setDescription("Can not pay, don't have isdn wallet on system bonus value " + bonusAgent);
                                    }
                                } else {
                                    bn.setResultCode("E04");
                                    logger.info("Target is the same last time, not paid comission for staff: " + staffLevel2.getStaffCode()
                                            + ", value: " + bonusAgent + " lastBonus " + lastBonus);
                                    bn.setDescription("Target is the same last time, not paid comission for staffLevel2 lastBonus " + lastBonus);
                                }
                            } else {
                                bn.setResultCode("E03");
                                logger.info("Bonus less than 0, no need to pay comission for staffLevel2 " + staffLevel2.getStaffCode());
                                bn.setDuration(System.currentTimeMillis() - timeSt);
                                bn.setDescription("Bonus less than 0, no need to pay comission for staffLevel2");
                            }
                        } else {
                            bn.setResultCode("E02");
                            logger.info("Not yet complete target, no need to pay comission, staffCode: " + staffLevel2.getStaffCode());
                            bn.setDuration(System.currentTimeMillis() - timeSt);
                            bn.setDescription("Not yet complete target, no need to pay comission for staff.");
                        }
                        double tmpSoldLv2 = tmpSubMbLv2 + tmpSubCUGLv2 + tmpSubAdslLv2;
                        double tmpOverTargetLv2 = tmpSoldLv2 - staffLevel2.getTarget();
                        double tmpOverTargetLv3Lv2 = tmpSubMbLv3 - staffLevel2.getTarget();
                        db.insertBonusAgentVipProcessDetail(bonusAgentVipProcessDetailId, bn.getBonusAgentVipProcessId(), staffLevel2.getStaffCode(), bn.getResultCode(),
                                bn.getDescription(), bonusAgent, tmpSoldLv2, tmpOverTargetLv2, tmpOverTargetLv3Lv2, tmpSubMbLv3);
                    }
                }
                logger.info("Total sub converted to premium all staffLevel2 of staff " + staffCode + " soldLv2: " + soldLv2);
                bn.setSoldLv2(soldLv2);
                if (bn.getSoldLv2() > targetBranch) {
                    logger.info("soldLv2 of staff " + staffCode + " over targetBranch: " + targetBranch);
                    bn.setOverTargetLv2Br(bn.getSoldLv2() - targetBranch);
                }
                logger.info("Total sub converted to premium all channels of all staffLevel2 of staff " + staffCode + " soldLv3: " + soldLv3);
                bn.setSoldLv3(soldLv3);
                if (bn.getSoldLv3() > targetBranch) {
                    logger.info("soldLv3 of staff " + staffCode + " over targetBranch: " + targetBranch);
                    bn.setOverTargetLv3Br(bn.getSoldLv3() - targetBranch);
                }
//                    Check target BOD of branch
                if ((soldLv1 + soldLv2 + soldLv3) > targetBranch) {
//                    Pay comission for BOD of branch
                    if (soldLv3 > targetBranch) {
                        bonusValue = Math.round(soldLv1 * bonusBrDirector + soldLv2 * bonusBrAgentVip + (soldLv3 - targetBranch) * bonusBrChannel);
                    } else if ((soldLv2 + soldLv3) > targetBranch) {
                        logger.info("soldLv2 + soldLv3 of staff " + staffCode + " over targetBranch: " + targetBranch);
                        bonusValue = Math.round(soldLv1 * bonusBrDirector + (soldLv2 + soldLv3 - targetBranch) * bonusBrAgentVip);
                    } else {
                        logger.info("soldLv1 + soldLv2 + soldLv3 of staff " + staffCode + " over targetBranch: " + targetBranch);
                        bonusValue = Math.round((soldLv1 + soldLv2 + soldLv3 - targetBranch) * bonusBrDirector);
                    }
                    if (bonusValue > 0) {
                        long lastBonus = db.sumBonusValueBrDiriectorInMonth(staffCode, "0");
                        bonusValue = bonusValue - lastBonus;
                        if (bonusValue > 0) {
                            if (staffInfo.getIsdnWallet() != null && staffInfo.getIsdnWallet().length() > 0) {
                                String eWalletResponse = pro.callEwallet(bonusAgentVipProcessId, 0,
                                        staffInfo.getIsdnWallet(), bonusValue,
                                        "615", staffCode, sdf.format(new Date()), db);
                                bn.seteWalletErrCode(eWalletResponse);
                                if ("01".equals(eWalletResponse)) {
                                    logger.info("Succeed to pay for br director: " + staffCode + ", value: " + bonusValue);
                                    bn.setDescription("Succeed to pay for br director " + bonusValue);
                                    bn.setResultCode("0");
                                } else {
                                    bn.setResultCode("E06");
                                    logger.info("Fail to pay for director: " + staffCode + ", error eWallet: " + eWalletResponse
                                            + " bonusValue " + bonusValue);
                                    bn.setDescription("Fail to pay for director eWalletResponse " + eWalletResponse + " bonusValue " + bonusValue);
                                }
                            } else {
                                bn.setResultCode("E05");
                                logger.info("Can not pay : " + staffCode + "don't have isdn wallet, bonus value: " + bonusAgent);
                                bn.setDescription("Can not pay, don't have isdn wallet on system bonus value " + bonusAgent);
                            }
                        } else {
                            logger.info("Target is the same last time, no need to pay bonus for director: " + staffCode
                                    + ", value: " + bonusValue + " lastBonus " + lastBonus);
                            bn.setDescription("Target is the same last time, no need to pay bonus for director " + lastBonus);
                            bn.setResultCode("E04");
                        }
                        bn.setBonusValue(bonusValue);
                        bn.setDuration(System.currentTimeMillis() - timeSt);
                    } else {
                        bn.setResultCode("E03");
                        logger.info("Bonus less than 0, no need to pay comission for BR Director, staffCode: " + staffCode);
                        bn.setDuration(System.currentTimeMillis() - timeSt);
                        bn.setDescription("Bonus less than 0, no need to pay comission for BR Director.");
                    }
                } else {
//                    no bonus
                    bn.setResultCode("E02");
                    logger.info("Not yet complete target, no need to pay comission for BR Director, staffCode: " + staffCode);
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    bn.setDescription("Not yet complete target, no need to pay comission for BR Director.");
                }
            } else {
                logger.warn("After validate respone code is fail bonusAgentVipId " + bn.getBonusAgentVipId()
                        + " staffCode: " + bn.getStaffCode()
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
                append("|\tBONUS_AGENT_VIP_ID|").
                append("|\tSTAFF_CODE|").
                append("|\tSTAFF_OWNER_ID|").
                append("|\tLAST_MODIFY_TIME\t|").
                append("|\tTARGET\t|");
        for (Record record : listRecord) {
            BonusAgentVip bn = (BonusAgentVip) record;
            br.append("\r\n").
                    append("|\t").
                    append(bn.getBonusAgentVipId()).
                    append("||\t").
                    append(bn.getStaffCode()).
                    append("||\t").
                    append(bn.getStaffOwnerId()).
                    append("||\t").
                    append(bn.getLastModifyTime()).
                    append("||\t").
                    append(bn.getTarget());
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
}
