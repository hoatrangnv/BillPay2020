/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.capital;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbPayFirstMobilePhone;
import com.viettel.paybonus.obj.FirstMobilePhone;
import com.viettel.threadfw.manager.AppManager;
import com.viettel.threadfw.process.ProcessRecordAbstract;
import com.viettel.vas.util.ExchangeClientChannel;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.ResourceBundle;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;
import com.viettel.paybonus.service.Exchange;
import java.math.BigInteger;

/**
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class CapitalFirstRegisterInfo extends ProcessRecordAbstract {

    Exchange pro;
    DbPayFirstMobilePhone db;
    SimpleDateFormat sdf = new SimpleDateFormat("ddMMyyyyHHmmssSSS");
    SimpleDateFormat sdfs = new SimpleDateFormat("yyyyMMddHHmmss");
    private SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddHHmmss");
    String smsReceiveFisrt;
    String smsReceiveInday;
    Calendar calOneDay = Calendar.getInstance();
    Calendar calFiveDays = Calendar.getInstance();
    Calendar calmonth = Calendar.getInstance();

    public CapitalFirstRegisterInfo() {
        super();
        logger = Logger.getLogger(FirstMobilePhone.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        db = new DbPayFirstMobilePhone();
        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
        smsReceiveFisrt = ResourceBundle.getBundle("configPayBonus").getString("smsPayFisrtMobilePhone");
        smsReceiveInday = ResourceBundle.getBundle("configPayBonus").getString("smsReceiveInday");
    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        for (Record record : listRecord) {
            FirstMobilePhone moRecord = (FirstMobilePhone) record;
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
        String mscNumCus;
        String mscNumChannel;
        String cellIdCus;
        String cellIdChannel;
        String cellCodeCus;
        String cellCodeChannel;

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
        boolean isNewDevice;
        for (Record record : listRecord) {
            timeSt = System.currentTimeMillis();
            FirstMobilePhone bn = (FirstMobilePhone) record;
            isNewDevice = false;
            mscNumCus = "";
            mscNumChannel = "";
            cellIdCus = "";
            cellIdChannel = "";
            cellCodeCus = "";
            cellCodeChannel = "";
            listResult.add(bn);
            try {
                //          B1 : Check over 30 day from create time
                long diff = new Date().getTime() - bn.getCreateTime().getTime();
                days = TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);
                days++; //add one more day
                if (days > 30) {
                    logger.warn("Over 30 days sub " + bn.getIsdn()
                            + " not yet topup or scraf card so ignore now id " + bn.getId());
                    bn.setResultCode("CPFMP01");
                    bn.setDescription("Over 30 days sub not yet topup or scraf card");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    db.deleteFirstMobilePhone(bn);
                    db.insertFirstMobilePhoneHis(bn, "");
                    continue;
                }
//          B2 : kiem tra CT ap dung voi MV cua MAC 
                if (!db.checkStaffInBranchMAC(bn.getCreateStaff().toUpperCase())) {
                    logger.warn("only apply with MAC Direct Sales channel, staff not apply, isdb :  " + bn.getIsdn() + "ActionAuditId " + bn.getActionAuditId());
                    bn.setResultCode("CPFMP02");
                    bn.setDescription("only apply with MAC Direct Sales channel , staff not apply, isdb :  " + bn.getIsdn() + "ActionAuditId " + bn.getActionAuditId());
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    db.deleteFirstMobilePhone(bn);
                    db.insertFirstMobilePhoneHis(bn, "");
                    continue;
                }
//           B3: Check cel of channel in MAC
                String isdnOfChannel = db.getTelByStaffCode(bn.getCreateStaff());
                if ("".equals(isdnOfChannel) || isdnOfChannel == null) {
                    logger.warn("Channel phone is empty with create_staff = " + bn.getCreateStaff());
                    bn.setResultCode("CPFMP03");
                    bn.setDescription("Channel phone is empty with create_staff = " + bn.getCreateStaff());
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    db.deleteFirstMobilePhone(bn);
                    db.insertFirstMobilePhoneHis(bn, "");
                    continue;
                } else {
                    logger.warn("Channel phone is not empty with create_staff = " + bn.getCreateStaff());
                    mscNumChannel = pro.getMSCInfor(isdnOfChannel, "");
                    if (mscNumChannel.trim().length() <= 0) {
                        logger.warn("Can not get mscNumChannel for channel with ISDN= " + isdnOfChannel + " System will count-up count_process to waiting");
//                        bn.setResultCode("CPFMP04");
//                        bn.setDescription("Can not get mscNumChannel for channel with ISDN= " + isdnOfChannel + " System will count-up count_process to waiting");
//                        bn.setDuration(System.currentTimeMillis() - timeSt);
//                        bn.setCountProcess(bn.getCountProcess() + 1);
//                        db.updateFirstMobilePhone(bn);
//                        continue;
                    } else {
                        cellIdChannel = pro.getCellIdRsString(isdnOfChannel, mscNumChannel, "");
                        if (cellIdChannel.trim().length() <= 0) {
                            logger.warn("Can not get cellIdChannel with mscNumChannel " + mscNumChannel + " " + bn.getID());
//                            bn.setResultCode("CPFMP05");
//                            bn.setDescription("Can not get cellIdChannel");
//                            bn.setDuration(System.currentTimeMillis() - timeSt);
//                            bn.setCountProcess(bn.getCountProcess() + 1);
//                            db.updateFirstMobilePhone(bn);
//                            continue;
                        } else {
                            String[] arrCellId = cellIdChannel.split("\\|");
                            if ((arrCellId != null) && (arrCellId.length == 2)) {
                                cellCodeChannel = db.getCell("", arrCellId[0].trim(), arrCellId[1].trim());
                                if (cellCodeChannel.trim().length() <= 0) {
                                    logger.warn("Can not get cellIdChannel " + isdnOfChannel + " ID  " + bn.getID());
//                                    bn.setResultCode("CPFMP06");
//                                    bn.setDescription("Can not get cellIdChannel");
//                                    bn.setDuration(System.currentTimeMillis() - timeSt);
//                                    bn.setCountProcess(bn.getCountProcess() + 1);
//                                    db.updateFirstMobilePhone(bn);
//                                    continue;
                                } else {
                                    if (!cellCodeChannel.trim().toUpperCase().contains("MAC")) {
                                        logger.warn("This staff_code not in cel MAC with staff_cdoe " + bn.getCreateStaff() + " and isdn= " + isdnOfChannel);
                                        bn.setResultCode("CPFMP08");
                                        bn.setDescription("This staff_code not in cel MAC with staff_cdoe " + bn.getCreateStaff() + " and isdn= " + isdnOfChannel);
                                        bn.setDuration(System.currentTimeMillis() - timeSt);
                                        db.deleteFirstMobilePhone(bn);
                                        db.insertFirstMobilePhoneHis(bn, "");
                                        continue;
                                    }
                                }
                            } else {
                                logger.warn("Invalid cellIdChannel " + isdnOfChannel + " ID  " + bn.getID());
//                                bn.setResultCode("CPFMP07");
//                                bn.setDescription("Invalid cellIdChannel");
//                                bn.setDuration(System.currentTimeMillis() - timeSt);
//                                bn.setCountProcess(bn.getCountProcess() + 1);
//                                db.updateFirstMobilePhone(bn);
//                                continue;
                            }
                        }
                    }
                }
//          B4 : Check dublicate by action_audit_id
                logger.info("Start check to make sure not duplicate process actionId " + bn.getActionAuditId()
                        + " id " + bn.getId() + " isdn: " + bn.getIsdn());
                if (db.checkAlreadyProcessRecord(bn.getActionAuditId())) {
                    logger.warn("Already process record in Pay_First_Mobile_Phone_his with action_audit_id=" + bn.getActionAuditId());
                    bn.setResultCode("CPFMP09");
                    bn.setDescription("Already process record in Pay_First_Mobile_Phone_his with action_audit_id=" + bn.getActionAuditId());
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    db.deleteFirstMobilePhone(bn);
                    db.insertFirstMobilePhoneHis(bn, "");
                    continue;
                }

//          B5 lay ra GetMscId va IMEI
                mscNumCus = pro.getMSCInfor(bn.getIsdn(), "");
                if (mscNumCus.trim().length() <= 0) {
                    logger.warn("Can not get mscNumCus with ISDN= " + bn.getIsdn() + " System will count-up count_process to waiting");
                    bn.setResultCode("CPFMP10");
                    bn.setDescription("Can not get mscNumCus with ISDN= " + bn.getIsdn() + " System will count-up count_process to waiting");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    bn.setCountProcess(bn.getCountProcess() + 1);
                    db.updateFirstMobilePhone(bn);
                    continue;
                }

                String toIMEI = pro.getIMEIByGetCellId(bn.getIsdn(), mscNumCus, "");
                if (toIMEI.equalsIgnoreCase("")) {
                    logger.warn("Can not get toIMEI with ISDN= " + bn.getIsdn() + " System will count-up count_process to waiting");
                    bn.setResultCode("CPFMP11");
                    bn.setDescription("Can not get toIMEI with ISDN= " + bn.getIsdn() + " System will count-up count_process to waiting");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    bn.setCountProcess(bn.getCountProcess() + 1);
                    db.updateFirstMobilePhone(bn);
                    continue;
                }

                //B6:   Kiem tra thue bao dang thuoc BTS nao 
//          GetCellId
                cellIdCus = pro.getCellIdRsString(bn.getIsdn(), mscNumCus, "");
                if (cellIdCus.trim().length() <= 0) {
                    logger.warn("Can not get Cell with mscNumCus " + mscNumCus + " " + bn.getID());
                    bn.setResultCode("CPFMP12");
                    bn.setDescription("Can not get cellIdCus");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    bn.setCountProcess(bn.getCountProcess() + 1);
                    db.updateFirstMobilePhone(bn);
                    continue;
                }

//          GetCellCode
                String[] arrCellId = cellIdCus.split("\\|");
                if ((arrCellId != null) && (arrCellId.length == 2)) {
                    cellCodeCus = db.getCell("", arrCellId[0], arrCellId[1]);
                    if (cellCodeCus.trim().length() <= 0) {
                        logger.warn("Can not get CellCode " + bn.getIsdn() + " ID  " + bn.getID());
                        bn.setResultCode("CPFMP13");
                        bn.setDescription("Can not get CellCode");
                        bn.setDuration(System.currentTimeMillis() - timeSt);
                        bn.setCountProcess(bn.getCountProcess() + 1);
                        db.updateFirstMobilePhone(bn);
                        continue;
                    }
                } else {
                    logger.warn("Invalid cellIdCus " + bn.getIsdn() + " ID  " + bn.getID());
                    bn.setResultCode("CPFMP14");
                    bn.setDescription("Invalid cellIdCus");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    bn.setCountProcess(bn.getCountProcess() + 1);
                    db.updateFirstMobilePhone(bn);
                    continue;
                }

                if (!cellCodeCus.trim().toUpperCase().contains("MAC") && !cellCodeCus.trim().toUpperCase().contains("MAT")
                        && !cellCodeCus.trim().toUpperCase().contains("INH") && !cellCodeCus.trim().toUpperCase().contains("GAZ")) {
                    logger.warn("only apply with BTS MAC, MAT, INH, GAZ , staff not apply, isdb :  " + bn.getIsdn() + " ActionAuditId " + bn.getActionAuditId());
                    bn.setResultCode("CPFMP15");
                    bn.setDescription("only apply with BTS MAC, MAT, INH, GAZ , staff not apply, isdb :  " + bn.getIsdn() + " ActionAuditId " + bn.getActionAuditId());
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    bn.setCountProcess(bn.getCountProcess() + 1);
                    db.updateFirstMobilePhone(bn);
                    continue;
                }

                //B7: Kiem tra xem co phai la may moi tren MDM hay khong 
                int checkNew = db.checkNewPhoneAttachDevice(toIMEI);
                if (checkNew == 1) {
                    logger.info("Pass condition 1, now continue check to make sure new device isdn : " + bn.getIsdn());
//                Thue bao moi attach vao thiet bi
                    Integer checkNotUser = db.checkNewDevice(toIMEI, bn.getIsdn());
                    if (checkNotUser == 0) {
                        isNewDevice = true;
                        logger.info("new device, new sim isdn : " + bn.getIsdn() + " imei: " + toIMEI);
//                    Thiet bi moi, gan sim moi
                        logger.info("Start check to make sure not duplicate process imei " + toIMEI
                                + " id " + bn.getId() + " isdn: " + bn.getIsdn());
                        if (db.checkAlreadyIMEIProcessRecord(toIMEI)) {
                            logger.warn("Already process record toIMEI " + toIMEI);
                            bn.setResultCode("CPFMP16");
                            bn.setDescription("Already process record in Pay_First_Mobile_Phone_his with imei= " + toIMEI);
                            bn.setDuration(System.currentTimeMillis() - timeSt);
                            db.deleteFirstMobilePhone(bn);
                            db.insertFirstMobilePhoneHis(bn, "");
                            continue;
                        } else {
                            //B8: Kiem tra neu dung va du dieu kien thi tra thuong
                            String description = "";
                            String rsCode = "";
                            //Cong KM money cho KH 100MT
                            String setExpireForA = pro.addBalanceWeek(bn.getIsdn(), "100", "2115", sdf2.format(calFiveDays.getTime()));
                            if (!"0".equals(setExpireForA)) {
                                logger.warn("Set expire price :addBalanceWeek 350 fail for sub A: " + bn.getIsdn() + " errorCode " + setExpireForA);
                                rsCode = "CPFMP17";
                                description = bn.getDescription() + "|| Set expire price :addBalanceWeek 350 fail for sub A: " + bn.getIsdn() + " errorCode " + setExpireForA;
                            }

                            // Cong KM data cho KH 100SMS
                            String resultChargeMoney = pro.addSmsDataVoice(bn.getIsdn(), "100", "5011", sdf2.format(calmonth.getTime()));
                            if (!"0".equals(resultChargeMoney)) {
                                logger.warn("result Charge Money 350 fail for sub A: " + bn.getIsdn() + " errorCode " + resultChargeMoney + "time " + timeSt);
                                rsCode = rsCode + "|CPFMP18";
                                description = description + " " + bn.getDescription() + " \"||Result Charge Money 350 fail for sub A: \" + bn.getMsisdn() + \" errorCode \" + resultChargeMoney + \"time \" + timeSt ";
                            }

                            // Cong data cho KH 1GB
                            String resultChargeData = pro.addSmsDataVoice(bn.getIsdn(), "1073741824", "5300", sdf2.format(calFiveDays.getTime()));
                            if (!"0".equals(resultChargeData)) {
                                logger.warn("Fail to charge resultChargeData " + bn.getIsdn() + " errcode resultChargeData " + resultChargeData);
                                rsCode = rsCode + "|CPFMP19";
                                description = description + "Fail to charge resultChargeData " + bn.getIsdn() + " errcode resultChargeData " + resultChargeData;
                            }
                            // Cong 30 ngay con lai
                            bn.setBonusType(2);
                            int rsInser = db.insertBounesMonth(bn, toIMEI);
                            if (rsInser <= 0) {
                                logger.warn("Fail to insertBounesMonth " + bn.getIsdn() + " errcode rsInser " + rsInser);
                                rsCode = rsCode + "|CPFMP20";
                                description = description + "|| Fail to insertBounesMonth " + bn.getIsdn() + " errcode rsInser " + rsInser;
                            }
                            // Thanh cong
                            if ("".equals(rsCode)) {
                                bn.setDescription("CPFMP00");
                            } else {
                                bn.setResultCode(rsCode);
                            }
                            if ("".equals(description)) {
                                bn.setDescription("Sucsessfuly add 100SMS|1GB|30 days for isdn: " + bn.getIsdn());
                            } else {
                                bn.setDescription(description);
                            }
                            bn.setDuration(System.currentTimeMillis() - timeSt);
                            //insert to Bounes_Monthly_His to check dublicate
                            db.insertBounesMonthlyHis(bn);
                            db.sendSms(bn.getIsdn(), smsReceiveFisrt, "155");
                            db.deleteFirstMobilePhone(bn);
                            db.insertFirstMobilePhoneHis(bn, toIMEI);
                            // insert vao bang nay de KH su dung *155*15#
                            db.inserTrparpu300(bn);
                        }
                    }
                }
                if (!isNewDevice) {
//               may cu, sim moi
                    logger.info("Old device, new sim isdn : " + bn.getIsdn() + " imei: " + toIMEI);
                    String description = "";
                    String rsCode = "";
                    logger.info("Start Add 10MB + 10SMS in day" + bn.getId() + " isdn " + bn.getIsdn() + " staff " + bn.getCreateStaff());
                    // Cong 10MB data cho KH
                    String addDataOneDay = pro.addSmsDataVoice(bn.getIsdn(), String.valueOf(tenMB), "5300", sdf2.format(calOneDay.getTime()));
                    if (!"0".equals(addDataOneDay)) {
                        logger.warn("Fail to add 10MB for ISDN " + bn.getIsdn() + " errcode result " + addDataOneDay);
                        rsCode = "CPFMP21";
                        description = "Fail to add 10MB for ISDN " + bn.getIsdn();
                    }
                    // Cong KM data cho KH 10SMS
                    String addSMSOneDay = pro.addSmsDataVoice(bn.getIsdn(), "10", "5011", sdf2.format(calOneDay.getTime()));
                    if (!"0".equals(addSMSOneDay)) {
                        logger.warn("Fail to add 10SMS for ISDN: " + bn.getIsdn() + " errorCode " + addSMSOneDay + "time " + timeSt);
                        rsCode = rsCode + "|CPFMP22";
                        description = description + " ||Fail to add 10MB for ISDN " + bn.getIsdn();
                    }
                    bn.setBonusType(1);
                    int rsInser = db.insertBounesMonth(bn, toIMEI);
                    if (rsInser <= 0) {
                        logger.warn("Fail to insertBounesMonth " + bn.getIsdn() + " errcode rsInser " + rsInser);
                        rsCode = rsCode + "|CPFMP23";
                        description = description + "|| Fail to insertBounesMonth " + bn.getIsdn() + " errcode rsInser " + rsInser;
                    }
                    if ("".equals(rsCode)) {
                        bn.setResultCode("CPFMP00");
                    } else {
                        bn.setResultCode(rsCode);
                    }
                    if ("".equals(description)) {
                        bn.setDescription("Sucsessfully add 10MB + 10SMS for ISDN " + bn.getIsdn() + " staff " + bn.getCreateStaff());
                    } else {
                        bn.setDescription(description);
                    }
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    //insert to Bounes_Monthly_His to check dublicate
                    db.insertBounesMonthlyHis(bn);
                    db.sendSms(bn.getIsdn(), smsReceiveInday, "155");
                    db.deleteFirstMobilePhone(bn);
                    db.insertFirstMobilePhoneHis(bn, toIMEI);
                    // insert vao bang nay de KH su dung *155*15#
                    db.inserTrparpu300(bn);
                    continue;
                }
            } catch (Exception e) {
                logger.error("Something Error CapitalPayFirstMobilePhone " + e.toString() + " so system delete Info " + bn.getIsdn());
                db.deleteFirstMobilePhone(bn);
                bn.setDescription("Had exception " + e.toString());
                bn.setResultCode("CPFMP99");
                bn.setDuration(System.currentTimeMillis() - timeSt);
                db.insertFirstMobilePhoneHis(bn, "");
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
                append("|\temola_isdnF\t|").
                append("|\tfirst_amount\t|").
                append("|\tsecond_amount\t|").
                append("|\tcount_process\t|").
                append("|\tlast_process\t|");
        for (Record record : listRecord) {
            FirstMobilePhone bn = (FirstMobilePhone) record;
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
