/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.capital;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbBounesMonthly;
import com.viettel.paybonus.database.DbPayFirstMobilePhone;
import com.viettel.paybonus.database.DbTrparu300;
import com.viettel.paybonus.obj.BounesMonthly;
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
import com.viettel.paybonus.service.Exchange;
import java.math.BigInteger;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class CapitalBonusMonthly extends ProcessRecordAbstract {

    Exchange pro;
    DbBounesMonthly db;
    DbPayFirstMobilePhone dbPayFirstMobilePhone;
    DbTrparu300 dbTrparu300;
    private SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddHHmmss");
    SimpleDateFormat sdf = new SimpleDateFormat("ddMMyyyyHHmmssSSS");
    String smsReceiveSecond;
    String smsReceiveInday;
    Calendar cal = Calendar.getInstance();
    Calendar calNow = Calendar.getInstance();
    Calendar calOneDay = Calendar.getInstance();
    Calendar calFiveDays = Calendar.getInstance();
    Calendar calmonth = Calendar.getInstance();
    String smsBonusEmola;

    public CapitalBonusMonthly() {
        super();
        logger = Logger.getLogger(BounesMonthly.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        db = new DbBounesMonthly();
        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
        smsReceiveSecond = ResourceBundle.getBundle("configPayBonus").getString("smsReceiveSecond");
        smsReceiveInday = ResourceBundle.getBundle("configPayBonus").getString("smsReceiveInday");
        smsBonusEmola = ResourceBundle.getBundle("configPayBonus").getString("smsBonusEmola");
    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        for (Record record : listRecord) {
            BounesMonthly moRecord = (BounesMonthly) record;
            moRecord.setNodeName(holder.getNodeName());
            moRecord.setClusterName(holder.getClusterName());
        }
        return listRecord;
    }

    @Override
    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
        List<Record> listResult = new ArrayList<Record>();
        cal.setTime(new Date());
        cal.set(Calendar.HOUR_OF_DAY, 23);
        cal.set(Calendar.MINUTE, 59);
        cal.set(Calendar.SECOND, 59);
        cal.set(Calendar.MILLISECOND, 999);

        calNow.setTime(new Date());
        calNow.add(Calendar.MINUTE, 3);
        calNow.set(Calendar.HOUR_OF_DAY, 23);
        calNow.set(Calendar.MINUTE, 59);
        calNow.set(Calendar.SECOND, 59);
        calNow.set(Calendar.MILLISECOND, 999);

        calOneDay.setTime(new Date());
        calOneDay.set(Calendar.HOUR_OF_DAY, 23);
        calOneDay.set(Calendar.MINUTE, 59);
        calOneDay.set(Calendar.SECOND, 59);
        calOneDay.set(Calendar.MILLISECOND, 999);

        calFiveDays.setTime(new Date());
        calFiveDays.add(Calendar.DATE, 5);
        calFiveDays.set(Calendar.HOUR_OF_DAY, 23);
        calFiveDays.set(Calendar.MINUTE, 59);
        calFiveDays.set(Calendar.SECOND, 59);
        calFiveDays.set(Calendar.MILLISECOND, 999);

        calmonth.setTime(new Date());
        calmonth.add(Calendar.DATE, 30);
        calmonth.set(Calendar.HOUR_OF_DAY, 23);
        calmonth.set(Calendar.MINUTE, 59);
        calmonth.set(Calendar.SECOND, 59);
        calmonth.set(Calendar.MILLISECOND, 999);

        BigInteger tenMB = new BigInteger("10");
        BigInteger dataHeigh = new BigInteger("1048576");
        tenMB = tenMB.multiply(dataHeigh);
        long timeSt;
        boolean isPay;
        int totalInTopup;
        int totalInScraft;
        int totalRecargAki;
        int totalUttm;
        long days;
        for (Record record : listRecord) {
            timeSt = System.currentTimeMillis();
            isPay = false;
            totalInTopup = 0;
            totalInScraft = 0;
            totalRecargAki = 0;
            totalUttm = 0;
            BounesMonthly bn = (BounesMonthly) record;
            listResult.add(bn);
            try {
//          B1: Check over 30 day from create time
                long diff = new Date().getTime() - bn.getCreateTime().getTime();
                days = TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);
                days++; //add one more day
                if (days > 30) {
                    logger.warn("Over 30 days sub will be remove in Bounes_Monthly with isdn= " + bn.getIsdn());
                    bn.setResultCode("CPBM01");
                    bn.setDescription("Over 30 days sub will be remove in Bounes_Monthly with isdn= " + bn.getIsdn());
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    db.deleteBounesMonthly(bn);
                    db.insertBounesMonthlyHis(bn);
                    continue;
                }
//          B2: kiem tra neu la may moi
                if (bn.getBonusType() == 2) {
//          B3: kiem tra may co di cung sim hay khong        
                    String cacheHandset = db.checkCacheHandset(bn, bn.getIsdn(), bn.getImei());
                    if (cacheHandset != null && cacheHandset.trim().length() > 0) {
                        logger.warn("Sim attach with phone  " + bn.getIsdn());
                        if (!db.checkUssd(bn.getIsdn())) {
                            db.inserTrparpu300(bn.getIsdn());
                        }
                    } else {
                        logger.warn("Sim not attach with phone  " + bn.getIsdn() + " so now remove table get *155*15#");
                        bn.setResultCode("CPBM06");
                        bn.setDescription(" Sim not attach with phone  ; isdn = " + bn.getIsdn() + "time :" + timeSt);
                        bn.setDuration(System.currentTimeMillis() - timeSt);
                        bn.setCountProcess(bn.getCountProcess() + 1);
                        db.updateBounesMonthly(bn);
                        db.inserTrparpu300His(bn.getIsdn(), bn.getCreateTime());
                        continue;
                    }
                    //B3.1 : check luy ke nap the >= 20MT
                    totalInTopup = db.checkTopupMonth(bn.getIsdn());
                    if (totalInTopup >= 20) {
                        isPay = true;
                    } else {
                        totalInScraft = db.checkScraftCardMonth(bn.getIsdn());
                        if (totalInTopup + totalInScraft >= 20) {
                            isPay = true;
                        } else {
                            // check voi 2 doi tac
                            totalRecargAki = db.checkRecargAki(bn.getIsdn());
                            if (totalInTopup + totalInScraft + totalRecargAki >= 20) {
                                isPay = true;
                            } else {
                                totalUttm = db.checkUTTM(bn.getIsdn());
                                if (totalInTopup + totalInScraft + totalRecargAki + totalUttm >= 20) {
                                    isPay = true;
                                }
                            }
                        }
                    }
                    if (!isPay) {
                        //Chua du dieu kien nhan thuong
                        logger.warn("Not yet recharge 20MT with isdn : " + bn.getIsdn() + " so not add free Whatsapp FB");
                        bn.setResultCode("CPBM06");
                        bn.setDescription("Not yet recharge 20MT with isdn : " + bn.getIsdn() + " so not add free Whatsapp FB");
                        bn.setDuration(System.currentTimeMillis() - timeSt);
                        bn.setCountProcess(bn.getCountProcess() + 1);
                        db.updateBounesMonthly(bn);
                        continue;
                    } else {
                        if (db.checkBonusCusToday(bn.getIsdn())) {
                            // da thuc su duoc cong khuyen mai ngay hom nay 
                            logger.warn("today,Sim already add bounes  ; isdn = " + bn.getIsdn() + " so now continue");
                            bn.setCountProcess(bn.getCountProcess() + 1);
                            db.updateBounesMonthly(bn);
                            continue;
                        }
                        //du dieu kien nhan thuong
                        String description = "";
                        String rsCode = "";
                        // Cong KM data cho KH 100SMS/ ngay
                        String addSMS = pro.addSmsDataVoice(bn.getIsdn(), "100", "5011", sdf2.format(calOneDay.getTime()));
                        if (!"0".equals(addSMS)) {
                            logger.warn("Fail add 100SMS for isdn: " + bn.getIsdn() + " errorCode " + addSMS + "time " + timeSt);
                            rsCode = rsCode + "|CPBM03";
                            description = description + " " + bn.getDescription() + " \"||Result Charge Money 350 fail for sub A: \" + bn.getMsisdn() + \" errorCode \" + resultChargeMoney + \"time \" + timeSt ";
                        }
                        logger.warn(" Recharge 20MT with isdn : " + bn.getIsdn());
                        String enday = sdf2.format(cal.getTime()).substring(0, sdf2.format(cal.getTime()).length() - 6) + "000000";
                        String startday = sdf2.format(calNow.getTime());
                        //Cong Km Free FB Whatssap
                        String rsKMFBWS = pro.addPriceV2(bn.getIsdn(), "11515033", startday, enday);
                        if (!"0".equals(rsKMFBWS)) {
                            logger.warn("Fail to  Free FB Whatssap " + bn.getIsdn() + " errcode rsKMFBWS " + rsKMFBWS);
                            rsCode = "CPBM05";
                            description = "Fail to Free FB Whatssap " + bn.getIsdn() + " errcode rsKMFBWS " + rsKMFBWS;
                        }
                        if ("".equals(description)) {
                            bn.setDescription(description);
                        } else {
                            bn.setDescription("Add Free What-App| 100SMS sucsessfuly");
                        }
                        db.sendSms(bn.getIsdn(), smsReceiveSecond.replace("%DAYS%", bn.getCountProcess().toString()), "155");
                        int rsInsert = db.insertBounesMonthlyHis(bn);
                        if (rsInsert <= 0) {
                            logger.warn("Fail to insert BounesMonthlyHis " + bn.getIsdn() + " errcode rsInsert " + rsInsert);
                        }
                    }
                } //          B2: kiem tra neu la may cu
                else if (bn.getBonusType() == 1) {
//          kiem tra da cong chinh sach chua
                    if (db.checkBonusCusToday(bn.getIsdn())) {
                        // da thuc su duoc cong khuyen mai ngay hom nay 
                        logger.warn("today,Sim already add bounes  ; isdn = " + bn.getIsdn() + " so now continue");
                        bn.setCountProcess(bn.getCountProcess() + 1);
                        db.updateBounesMonthly(bn);
                        continue;
                    }
                    String description = "";
                    String rsCode = "";
                    logger.info("Start Add 10MB + 10SMS isdn " + bn.getIsdn() + " staff " + bn.getCreateStaff());
                    // Cong 10MB data cho KH
                    String addDataOneDay = pro.addSmsDataVoice(bn.getIsdn(), String.valueOf(tenMB), "5300", sdf2.format(calOneDay.getTime()));
                    if (!"0".equals(addDataOneDay)) {
                        logger.warn("Fail to add 10MB for ISDN " + bn.getIsdn() + " errcode result " + addDataOneDay);
                        rsCode = "CPBM07";
                        description = "Fail to add 10MB for ISDN " + bn.getIsdn();
                    }
                    // Cong KM data cho KH 10SMS
                    String addSMSOneDay = pro.addSmsDataVoice(bn.getIsdn(), "10", "5011", sdf2.format(calOneDay.getTime()));
                    if (!"0".equals(addSMSOneDay)) {
                        logger.warn("Fail to add 10SMS for ISDN: " + bn.getIsdn() + " errorCode " + addSMSOneDay + "time " + timeSt);
                        rsCode = rsCode + "|CPBM08";
                        description = description + " ||Fail to add 10MB for ISDN " + bn.getIsdn();
                    }
                    if (!"".equals(description)) {
                        bn.setDescription(description);
                    } else {
                        bn.setDescription("Add 10MB + 10SMS in day sucsessfuly");
                    }
                    db.sendSms(bn.getIsdn(), smsReceiveInday, "155");
                    int rsInsert = db.insertBounesMonthlyHis(bn);
                    if (rsInsert <= 0) {
                        logger.warn("Fail to insert BounesMonthlyHis " + bn.getIsdn() + " errcode rsInsert " + rsInsert);
                    }
                } else {
                    logger.warn("Invalid BonusType isdn : " + bn.getIsdn() + " so now continue");
                    continue;
                }
            } catch (Exception e) {
                logger.error("Someting Error CapitalPayBounesMonthly " + e.toString() + " so system delete Info " + bn.getIsdn());
                db.deleteBounesMonthly(bn);
                bn.setDescription("Had exception " + e.toString());
                bn.setResultCode("CPBM99");
                bn.setDuration(System.currentTimeMillis() - timeSt);
                db.insertBounesMonthlyHis(bn);
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
                append("|\tACTION_AUDIT_ID|").
                append("|\tisdn\t|").
                append("|\tcreate_time\t|").
                append("|\tcreate_staff\t|").
                append("|\temola_isdn\t|").
                append("|\tbonus_type\t|");
        for (Record record : listRecord) {
            BounesMonthly bn = (BounesMonthly) record;
            br.append("\r\n").
                    append("|\t").
                    append(bn.getActionAuditId()).
                    append("|\t").
                    append(bn.getIsdn()).
                    append("||\t").
                    append((bn.getCreateTime() != null ? sdf.format(bn.getCreateTime()) : null)).
                    append("||\t").
                    append(bn.getCreateStaff()).
                    append("||\t").
                    append(bn.geteMolaIsdn()).
                    append("||\t").
                    append(bn.getBonusType());
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
