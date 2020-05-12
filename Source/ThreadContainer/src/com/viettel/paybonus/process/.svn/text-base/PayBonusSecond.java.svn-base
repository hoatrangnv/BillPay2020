/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbPayBonusSecond;
import com.viettel.paybonus.obj.BonusSecond;
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
import java.util.concurrent.TimeUnit;

/**
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class PayBonusSecond extends ProcessRecordAbstract {

    Exchange pro;
    DbPayBonusSecond db;
    String shopIdAvailablePolicy;
    String[] arrShopIdAvailablePolicy;

    public PayBonusSecond() {
        super();
        logger = Logger.getLogger(PayBonusSecond.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
        db = new DbPayBonusSecond();
        shopIdAvailablePolicy = ResourceBundle.getBundle("configPayBonus").getString("shopIdAvailablePolicy");
        arrShopIdAvailablePolicy = shopIdAvailablePolicy.split("\\|");
    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        for (Record record : listRecord) {
            BonusSecond moRecord = (BonusSecond) record;
            moRecord.setNodeName(holder.getNodeName());
            moRecord.setClusterName(holder.getClusterName());
        }
        return listRecord;
    }

    @Override
    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
        List<Record> listResult = new ArrayList<Record>();
        List<BonusSecond> listUpdate = new ArrayList<BonusSecond>();
        long timeSt;
        SimpleDateFormat sdf = new SimpleDateFormat("ddMMyyyyHHmmssSSS");
        SimpleDateFormat sdfs = new SimpleDateFormat("yyyyMMddHHmmss");
        long days;
        String[] cardInfo;
        boolean isPay;
        boolean isSpecialCommission;
        for (Record record : listRecord) {
            timeSt = System.currentTimeMillis();
            BonusSecond bn = (BonusSecond) record;
            cardInfo = null;
            isPay = false;
            isSpecialCommission = true;//always pay comission 1 times...
            listResult.add(bn);
            if ("0".equals(bn.getResultCode())) {
//                Check over 30 day from create time
                long diff = new Date().getTime() - bn.getCreateTime().getTime();
                days = TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);
                days++; //add one more day
                if (days > 7) {
                    logger.warn("Over 7 days sub " + bn.getIsdn()
                            + " not yet topup or scraf card so ignore now id " + bn.getId());
                    bn.setPayType(0);
                    bn.setResultCode("E1");
                    bn.setDescription("Over 7 days sub not yet topup or scraf card");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    db.deleteBonusSecond(bn);
                    db.insertBonusSecondHis(bn);
//                    String activeStatus = pro.checkActiveStatusOnOCS(bn.getIsdn());
//                    if (activeStatus == null || activeStatus.trim().equals("1") || activeStatus.trim().equals("5")) {
//                        logger.warn("Not yet active so now update to syn status of sub " + bn.getActionAuditId()
//                                + " id " + bn.getId() + " isdn: " + bn.getIsdn());
//                        db.updateSubMbBackIdle(bn.getIsdn());
//                    }
                    continue;
                }
//                Check user belong Cabo branch or not
                if (arrShopIdAvailablePolicy.length > 0) {
                    for (String tmpShopId : arrShopIdAvailablePolicy) {
                        if (db.checkUserCodeOfBranch(bn.getCreateStaff(), Long.valueOf(tmpShopId))) {
                            isSpecialCommission = true;
                            logger.warn("User belong Cabo branch. Begin check and only payCommission 1 time, staffCode: " + bn.getCreateStaff() + " actionId " + bn.getActionAuditId()
                                    + " id " + bn.getId() + " isdn: " + bn.getIsdn());
                            break;
                        }
                    }
                } else {
                    logger.warn("Don't have config for branch. Pay commission normal (3MT + 10MT), isdn: " + bn.getIsdn());
                }
//                Check to make sure not yet processing this record, not duplicate process for each record (base on action_audit_id)
//                logger.info("Start check duplicate process actionId " + bn.getActionAuditId()
//                        + " id " + bn.getId() + " isdn: " + bn.getIsdn());
                if (db.checkAlreadyProcessRecord(bn.getActionAuditId())) {
                    logger.warn("Already process record actionId " + bn.getActionAuditId());
                    bn.setPayType(0);
                    bn.setResultCode("E2");
                    bn.setDescription("Already process record");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    db.deleteBonusSecond(bn);
                    db.insertBonusSecondHis(bn);
                    continue;
                }
//                Check active and not pay for first time
                if (!db.checkAlreadyPayFirstTime(bn.getActionAuditId())) {
                    String activeStatus = pro.checkActiveStatusOnOCS(bn.getIsdn());
                    if (activeStatus == null || activeStatus.trim().equals("1") || activeStatus.trim().equals("5")) {
                        logger.warn("Not active so not pay first for actionId " + bn.getActionAuditId()
                                + " id " + bn.getId() + " isdn: " + bn.getIsdn());
                        bn.setCountProcess(bn.getCountProcess() + 1);
                        listUpdate.add(bn);
//                        db.updateBonusSecond(bn);
                        continue;
                    } else {
                        logger.warn("Already active so now pay first for actionId " + bn.getActionAuditId()
                                + " id " + bn.getId() + " isdn: " + bn.getIsdn());
//                        db.updateSubMbWhenActive(bn.getIsdn()); //active on BCCS to support register VAS assp
                        //              Call eWallet to add bonus
                        if (isSpecialCommission) {
                            bn.setFirstAmount(0L);
                            logger.info("User belong Cabo branch, no need to pay commission. Only pay after topup, actionId " + bn.getActionAuditId() + " isdnEmola "
                                    + bn.geteMolaIsdn() + " amount " + bn.getFirstAmount() + " isdn " + bn.getIsdn());
                        }
                        if (bn.getFirstAmount() > 0) {
                            String eWalletResponse = pro.callEwallet(bn.getActionAuditId(), bn.getChannelTypeId(), bn.geteMolaIsdn(),
                                    bn.getFirstAmount(), bn.getActionCode(), bn.getCreateStaff(), sdf.format(new Date()), db);
                            bn.seteWalletErrCode(eWalletResponse);
                            bn.setPayType(1);
                            if ("01".equals(eWalletResponse)) {
                                logger.info("Pay Bonus first success for actionId " + bn.getActionAuditId() + " isdnEmola "
                                        + bn.geteMolaIsdn() + " amount " + bn.getFirstAmount() + " isdn " + bn.getIsdn());
                                bn.setResultCode("0");
                                bn.setDescription("Pay Bonus first success for isdnEmola " + bn.geteMolaIsdn()
                                        + " amount " + bn.getFirstAmount());
                                if (!isSpecialCommission) {
                                    String msg = ResourceBundle.getBundle("configPayBonus").getString("ms_pay_bonus_emoney_realtime");
                                    String serial = db.getSerialByIsdnCustomer(bn.getIsdn());
                                    msg = msg.replace("%MONEY%", "" + bn.getFirstAmount());
                                    msg = msg.replace("%SERIAL%", serial);
                                    db.sendSms(bn.geteMolaIsdn(), msg, "86142");
                                }
                                bn.setDuration(System.currentTimeMillis() - timeSt);
                                db.insertBonusSecondHis(bn);
                            } else {
                                logger.error("Fail to pay bonus first for actionId " + bn.getActionAuditId()
                                        + " isdnEmola " + bn.geteMolaIsdn() + " amount " + bn.getSecondAmount());
                                bn.setResultCode("E3");
                                bn.setDescription("Fail to pay bonus first for isdn " + bn.geteMolaIsdn() + " amount " + bn.getSecondAmount());
                                bn.setDuration(System.currentTimeMillis() - timeSt);
                                db.deleteBonusSecond(bn);
                                db.insertBonusSecondHis(bn);
                            }
                        } else {
                            logger.info("Amount first is: " + bn.getFirstAmount() + ", no need pay commission for first time, isdn: " + bn.getIsdn());
                        }
                    }
                }
//                Check today have topup or scraftcard
                cardInfo = db.checkTopupToday(bn);
                if (cardInfo != null && cardInfo[0] != null && cardInfo[0].trim().length() > 0) {
                    logger.info("Today isdn " + bn.getIsdn() + " have topup via " + cardInfo[0]
                            + " amount " + cardInfo[1] + " time " + cardInfo[2] + " id " + bn.getId());
                    isPay = true;
                } else {
                    cardInfo = db.checkTopupUttmToday(bn);
                    if (cardInfo != null && cardInfo[0] != null && cardInfo[0].trim().length() > 0) {
                        logger.info("Today isdn " + bn.getIsdn() + " have topup via " + cardInfo[0]
                                + " amount " + cardInfo[1] + " time " + cardInfo[2] + " id " + bn.getId());
                        isPay = true;
                    } else {
                        cardInfo = db.checkTopupRecargAkiToday(bn);
                        if (cardInfo != null && cardInfo[0] != null && cardInfo[0].trim().length() > 0) {
                            logger.info("Today isdn " + bn.getIsdn() + " have topup via " + cardInfo[0]
                                    + " amount " + cardInfo[1] + " time " + cardInfo[2] + " id " + bn.getId());
                            isPay = true;
                        } else {
                            cardInfo = db.checkScraftCardToDay(bn);
                            if (cardInfo != null && cardInfo[0] != null && cardInfo[0].trim().length() > 0) {
                                logger.info("Today isdn " + bn.getIsdn() + " have scraft card serial " + cardInfo[0]
                                        + " amount " + cardInfo[1] + " time " + cardInfo[2] + " id " + bn.getId());
                                isPay = true;
                            }
//                            else {
//                                for (int i = 0; i < days; i++) {
//                                    cardInfo = db.checkTopup(bn, String.valueOf(i + 1));
//                                    if (cardInfo != null && cardInfo[0] != null && cardInfo[0].trim().length() > 0) {
//                                        logger.info("isdn " + bn.getIsdn() + " have topup via " + cardInfo[0]
//                                                + " amount " + cardInfo[1] + " time " + cardInfo[2] + " id " + bn.getId());
//                                        isPay = true;
//                                        break;
//                                    } else {
//                                        cardInfo = db.checkTopupUttm(bn, String.valueOf(i + 1));
//                                        if (cardInfo != null && cardInfo[0] != null && cardInfo[0].trim().length() > 0) {
//                                            logger.info("isdn " + bn.getIsdn() + " have topup via " + cardInfo[0]
//                                                    + " amount " + cardInfo[1] + " time " + cardInfo[2] + " id " + bn.getId());
//                                            isPay = true;
//                                            break;
//                                        } else {
//                                            cardInfo = db.checkTopupRecargAki(bn, String.valueOf(i + 1));
//                                            if (cardInfo != null && cardInfo[0] != null && cardInfo[0].trim().length() > 0) {
//                                                logger.info("isdn " + bn.getIsdn() + " have topup via " + cardInfo[0]
//                                                        + " amount " + cardInfo[1] + " time " + cardInfo[2] + " id " + bn.getId());
//                                                isPay = true;
//                                                break;
//                                            } else {
//                                                cardInfo = db.checkScraftCard(bn, String.valueOf(i + 1));
//                                                if (cardInfo != null && cardInfo[0] != null && cardInfo[0].trim().length() > 0) {
//                                                    logger.info("isdn " + bn.getIsdn() + " have scraft card serial " + cardInfo[0]
//                                                            + " amount " + cardInfo[1] + " time " + cardInfo[2] + " id " + bn.getId());
//                                                    isPay = true;
//                                                    break;
//                                                }
//                                            }
//                                        }
//                                    }
//                                }
//                            }
                        }
                    }
                }
                if (isPay) {
                    bn.setSecondPayCard(cardInfo[0]);
                    bn.setSecondPayTime(sdfs.parse(cardInfo[2]));
                    //              Call eWallet to add bonus
                    if (isSpecialCommission) {
                        int tmpRefillAmount = Integer.parseInt(cardInfo[1]);
//                        if (bn.getSecondAmount() <= 0) { // Huynq13 20190904 add to support register vas after connecting kit by Staff (not Channel)
//                            bn.setSecondAmount(0L);
//                        } else 
                        if (tmpRefillAmount >= 10 && tmpRefillAmount < 15) {
                            bn.setSecondAmount(6L);
                        } else if (tmpRefillAmount >= 15) {
                            bn.setSecondAmount(8L);
                        } else {
                            bn.setSecondAmount(0L);
                        }
                        logger.info("User belong Cabo branch, amount will be pay bonus second time actionId " + bn.getActionAuditId() + " isdnEmola "
                                + bn.geteMolaIsdn() + " amount " + bn.getSecondAmount() + " isdn " + bn.getIsdn());
                    }
                    String eWalletResponse = pro.callEwallet(bn.getActionAuditId(), bn.getChannelTypeId(), bn.geteMolaIsdn(),
                            bn.getSecondAmount(), bn.getActionCode(), bn.getCreateStaff(), sdf.format(new Date()), db);
                    bn.seteWalletErrCode(eWalletResponse);
                    bn.setPayType(2);
                    if ("01".equals(eWalletResponse)) {
                        logger.info("Pay Bonus success for actionId " + bn.getActionAuditId() + " isdnEmola "
                                + bn.geteMolaIsdn() + " amount " + bn.getSecondAmount() + " isdn " + bn.getIsdn());
                        bn.setResultCode("0");
                        bn.setDescription("Pay Bonus second success for isdnEmola " + bn.geteMolaIsdn()
                                + " amount " + bn.getSecondAmount());
                        String msg = ResourceBundle.getBundle("configPayBonus").getString("ms_pay_bonus_emoney_second");
                        msg = msg.replace("%MONEY%", "" + bn.getSecondAmount());
                        String serial = db.getSerialByIsdnCustomer(bn.getIsdn());
                        msg = msg.replace("%SERIAL%", serial);
                        bn.setDuration(System.currentTimeMillis() - timeSt);
                        db.sendSms(bn.geteMolaIsdn(), msg, "86142");
                        db.deleteBonusSecond(bn);
                        db.insertBonusSecondHis(bn);
                        continue;
                    } else {
                        logger.error("Fail to pay bonus second for actionId " + bn.getActionAuditId()
                                + " isdnEmola " + bn.geteMolaIsdn() + " amount " + bn.getSecondAmount());
                        bn.setResultCode("E4");
                        bn.setDescription("Fail to pay bonus second for isdn " + bn.geteMolaIsdn() + " amount " + bn.getSecondAmount());
                        bn.setDuration(System.currentTimeMillis() - timeSt);
                        db.deleteBonusSecond(bn);
                        db.insertBonusSecondHis(bn);
                        continue;
                    }
                } else {
                    logger.info("Not yet topup or scraftcard, so now update process count process id "
                            + bn.getId() + " isdn " + bn.getIsdn() + " staff " + bn.getCreateStaff());
                    bn.setCountProcess(bn.getCountProcess() + 1);
                    listUpdate.add(bn);
//                    db.updateBonusSecond(bn);
                    continue;
                }
            } else {
                logger.warn("After validate respone code is fail actionId " + bn.getActionAuditId()
                        + " id " + bn.getId() + " isdn: " + bn.getIsdn()
                        + " so continue with other transaction");
                continue;
            }
        }
        db.updateBonusSecond(listUpdate);
        listRecord.clear();
        Thread.sleep(1000 * 60 * 5);
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
                append("|\temola_isdnF\t|").
                append("|\tfirst_amount\t|").
                append("|\tsecond_amount\t|").
                append("|\tcount_process\t|").
                append("|\tlast_process\t|");
        for (Record record : listRecord) {
            BonusSecond bn = (BonusSecond) record;
            br.append("\r\n").
                    append("|\t").
                    append(bn.getId()).
                    append("||\t").
                    append(bn.getIsdn()).
                    append("||\t").
                    append((bn.getCreateTime() != null ? sdf.format(bn.getCreateTime()) : null)).
                    append("||\t").
                    append(bn.getCreateStaff()).
                    append("||\t").
                    append(bn.geteMolaIsdn()).
                    append("||\t").
                    append(bn.getFirstAmount()).
                    append("||\t").
                    append(bn.getSecondAmount()).
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
