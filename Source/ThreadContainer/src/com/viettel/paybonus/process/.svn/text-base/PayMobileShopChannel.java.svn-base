/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbMobileShopChannel;
import com.viettel.paybonus.obj.MobileShopChannel;
import com.viettel.paybonus.service.Exchange;
import com.viettel.threadfw.manager.AppManager;
import java.util.List;
import org.apache.log4j.Logger;
import com.viettel.threadfw.process.ProcessRecordAbstract;
import com.viettel.vas.util.ExchangeClientChannel;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.ResourceBundle;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class PayMobileShopChannel extends ProcessRecordAbstract {

    Exchange pro;
    DbMobileShopChannel db;
    SimpleDateFormat sdf = new SimpleDateFormat("ddMMyyyyHHmmssSSS");
    SimpleDateFormat sdfs = new SimpleDateFormat("yyyyMMddHHmmss");
    private SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddHHmmss");
    String smsBonusEmola;
    String smsReceiveFisrt;

    public PayMobileShopChannel() {
        super();
        logger = Logger.getLogger(MobileShopChannel.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        db = new DbMobileShopChannel();
//        pro = new Exchange(ExchangeClientChannel.getInstance("D:\\STUDY\\Project\\Movitel\\mBCCS_MOZ_FULL\\PayBonus\\etc\\exchange_client.cfg").getInstanceChannel(), logger);
        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
        smsBonusEmola = ResourceBundle.getBundle("configPayBonus").getString("smsBonusEmola");
        smsReceiveFisrt = ResourceBundle.getBundle("configPayBonus").getString("smsReceiveFisrt");
    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        for (Record record : listRecord) {
            MobileShopChannel moRecord = (MobileShopChannel) record;
            moRecord.setNodeName(holder.getNodeName());
            moRecord.setClusterName(holder.getClusterName());
        }
        return listRecord;
    }

    @Override
    public List<Record> processListRecord(List<Record> listRecord) throws Exception {

        List<Record> listResult = new ArrayList<Record>();
        long timeSt;
        long days;
        String[] cardInfo;
        String mscId;
        boolean isPay;
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        cal.add(Calendar.DATE, 1);
        for (Record record : listRecord) {
            timeSt = System.currentTimeMillis();
            MobileShopChannel bn = (MobileShopChannel) record;
            cardInfo = null;
            isPay = false;
            mscId = "";
            listResult.add(bn);
//          B1 : Check over 30 day from create time
            long diff = new Date().getTime() - bn.getCreateTime().getTime();
            days = TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);
            days++; //add one more day
            if (days > 30) {
                logger.warn("Over 30 days sub " + bn.getIsdn()
                        + " not yet topup or scraf card so ignore now id " + bn.getId());
                bn.setResultCode("E1");
                bn.setDescription("Over 30 days sub not yet topup or scraf card");
                bn.setDuration(System.currentTimeMillis() - timeSt);
                db.deleteMobileShopChannel(bn);
                db.insertMobileShopChannelHis(bn, "");
                continue;
            }
//          B2 : Check to make sure not yet processing this record, not duplicate process for each record (base on action_audit_id)
            logger.info("Start check to make sure not duplicate process actionId " + bn.getActionAuditId()
                    + " id " + bn.getId() + " isdn: " + bn.getIsdn());
            if (db.checkAlreadyProcessRecord(bn.getActionAuditId())) {
                logger.warn("Already process record actionId " + bn.getActionAuditId());
                bn.setResultCode("E2");
                bn.setDescription("Already process record");
                bn.setDuration(System.currentTimeMillis() - timeSt);
                db.deleteMobileShopChannel(bn);
                db.insertMobileShopChannelHis(bn, "");
                continue;
            }

            //B3: Kiem tra xem co phai la may moi hay khong
            // GetMscId
            mscId = pro.getMSCInfor("258" + bn.getIsdn(), "");
            if (mscId.trim().length() <= 0) {
                logger.warn("Can not get mscId " + bn.getIsdn());
                bn.setResultCode("E4");
                bn.setDescription("Can not get mscId " + bn.getIsdn());
                bn.setDuration(System.currentTimeMillis() - timeSt);
                bn.setCountProcess(bn.getCountProcess() + 1);
                db.updateMobileShopChannel(bn);
                continue;
            }

            String toIMEI = pro.getIMEIByGetCellId("258" + bn.getIsdn(), mscId, "");
            if (toIMEI.equalsIgnoreCase("")) {
                logger.warn("Can not get toIMEI " + bn.getIsdn());
                bn.setResultCode("E6");
                bn.setDescription("Can not get toIMEI " + bn.getIsdn());
                bn.setDuration(System.currentTimeMillis() - timeSt);
                bn.setCountProcess(bn.getCountProcess() + 1);
                db.updateMobileShopChannel(bn);
                continue;
            }

            String newHandset = db.checkNewHandset(bn, "258" + bn.getIsdn(), toIMEI);
            if (newHandset != null && newHandset.trim().length() > 0) {
                logger.info("It is not new handset with id :" + bn.getId() + " isdn " + bn.getIsdn() + " staff " + bn.getCreateStaff());
                bn.setResultCode("E7");
                bn.setDescription("It is not new handset with id :" + bn.getId() + " isdn " + bn.getIsdn() + " staff " + bn.getCreateStaff());
                bn.setDuration(System.currentTimeMillis() - timeSt);
                db.deleteMobileShopChannel(bn);
                db.insertMobileShopChannelHis(bn, "");
                continue;
            }

//          B4 : Check today have topup or scraftcard
            cardInfo = db.checkTopupToday(bn);
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
                } else {
                    // check cardInfo voi 2 doi tac
                    cardInfo = db.checkTopupTodayPartner1(bn);
                    if (cardInfo != null && cardInfo[0] != null && cardInfo[0].trim().length() > 0) {
                        logger.info("Today isdn " + bn.getIsdn() + " have topup via " + cardInfo[0]
                                + " amount " + cardInfo[1] + " time " + cardInfo[2] + " id " + bn.getId());
                        isPay = true;
                    } else {
                        cardInfo = db.checkTopupTodayPartner2(bn);
                        if (cardInfo != null && cardInfo[0] != null && cardInfo[0].trim().length() > 0) {
                            logger.info("Today isdn " + bn.getIsdn() + " have scraft card serial " + cardInfo[0]
                                    + " amount " + cardInfo[1] + " time " + cardInfo[2] + " id " + bn.getId());
                            isPay = true;
                        }
                    }
                }
            }
            if (!isPay) {
                logger.warn(" not yet recharge 20MT with isdn : " + bn.getIsdn());
                bn.setResultCode("E3");
                bn.setDescription(" Not yet recharge 20MT with isdn : " + bn.getIsdn());
                bn.setDuration(System.currentTimeMillis() - timeSt);
                bn.setCountProcess(bn.getCountProcess() + 1);
                db.updateMobileShopChannel(bn);
                continue;
            }

//          B5: tannh Kiem tra day co phai may moi va dung 3g khong tren system MDM
            // GetCellId
            boolean rscellId = pro.getCellId("258" + bn.getIsdn(), mscId, "");
            if (!rscellId) {
                logger.warn("Can not get cellId " + bn.getIsdn());
                bn.setResultCode("E5");
                bn.setDescription("Can not get cellId " + bn.getIsdn());
                bn.setDuration(System.currentTimeMillis() - timeSt);
                bn.setCountProcess(bn.getCountProcess() + 1);
                db.updateMobileShopChannel(bn);
                continue;
            }

            //B6: Kiem tra neu dung va du dieu kien thi tra thuong
            String description = "";
            String rsCode = "";
            // Cong data cho KH 300MB
            String resultChargeMoney = pro.addSmsDataVoice(bn.getIsdn(), "314572800", "5300", sdf2.format(cal.getTime()));
            if (!"0".equals(resultChargeMoney)) {
                logger.warn("Fail to charge money " + bn.getIsdn() + " errcode chargemoney " + resultChargeMoney);
                rsCode = "E8";
                description = "Fail to charge money " + bn.getIsdn() + " errcode chargemoney " + resultChargeMoney;
            }
            // cong sms
            String rsAddSMS = pro.addSmsDataVoice(bn.getIsdn(), "30", "5011", sdf2.format(cal.getTime()));
            if (!"0".equals(rsAddSMS)) {
                logger.warn("Fail to charge money " + bn.getIsdn() + " errcode rsAddSMS " + rsAddSMS);
                rsCode = rsCode + "||E9";
                description = description + "|| Fail to charge money " + bn.getIsdn() + " errcode rsAddSMS " + rsAddSMS;
            }
            // Cong 33 ngay con lai
            int rsInser = db.insertBounesMonth(bn, toIMEI);
            if (rsInser <= 0) {
                logger.warn("Fail to insertBounesMonth " + bn.getIsdn() + " errcode rsInser " + rsInser);
                rsCode = rsCode + "||E10";
                description = description + "|| Fail to insertBounesMonth " + bn.getIsdn() + " errcode rsInser " + rsInser;
            }
            db.sendSms(bn.getIsdn(), smsReceiveFisrt, "86142");

            bn.setSecondPayCard(cardInfo[0]);
            bn.setSecondPayTime(sdfs.parse(cardInfo[2]));
            // Call eWallet to add bonus
            String eWalletResponse = pro.callEwallet(bn.getActionAuditId(), bn.getChannelTypeId(), bn.geteMolaIsdn(),
                    20, "615", bn.getCreateStaff(), sdf.format(new Date()), db);
            bn.seteWalletErrCode(eWalletResponse);
            if ("01".equals(eWalletResponse)) {
                logger.info("Pay Bonus success for actionId " + bn.getActionAuditId() + " isdnEmola "
                        + bn.geteMolaIsdn() + " amount " + bn.getSecondAmount() + " isdn " + bn.getIsdn());
                bn.setResultCode(rsCode + "0");
                bn.setDescription(description + "|| Pay Bonus second success for isdnEmola " + bn.geteMolaIsdn()
                        + " amount " + bn.getSecondAmount());
                bn.setDuration(System.currentTimeMillis() - timeSt);
                db.sendSms(bn.geteMolaIsdn(), smsBonusEmola, "86142");
                db.deleteMobileShopChannel(bn);
                db.insertMobileShopChannelHis(bn, toIMEI);
            } else {
                logger.error("Fail to pay bonus second for actionId " + bn.getActionAuditId()
                        + " isdnEmola " + bn.geteMolaIsdn() + " amount " + bn.getSecondAmount());
                bn.setResultCode(rsCode + "||E11");
                bn.setDescription(description + "|| Fail to pay bonus second for isdn " + bn.geteMolaIsdn() + " amount " + bn.getSecondAmount());
                bn.setDuration(System.currentTimeMillis() - timeSt);
                db.deleteMobileShopChannel(bn);
                db.insertMobileShopChannelHis(bn, toIMEI);
            }

        }
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
                append("|\temola_isdnF\t|").
                append("|\tfirst_amount\t|").
                append("|\tsecond_amount\t|").
                append("|\tcount_process\t|").
                append("|\tlast_process\t|");
        for (Record record : listRecord) {
            MobileShopChannel bn = (MobileShopChannel) record;
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
