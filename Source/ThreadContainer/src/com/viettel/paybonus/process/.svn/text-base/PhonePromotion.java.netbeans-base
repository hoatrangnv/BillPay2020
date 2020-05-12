/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbPhonePromotion;
import com.viettel.paybonus.obj.PhoneNewPromotion;
import com.viettel.paybonus.obj.PhonePromotionConfig;
import com.viettel.paybonus.service.Exchange;
//import com.viettel.paybonus.service.XmlUtil;
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

/**
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class PhonePromotion extends ProcessRecordAbstract {

    Exchange pro;
    DbPhonePromotion db;
    String smsBounesNewPhone;
    Calendar calData = Calendar.getInstance();
    Calendar calSms = Calendar.getInstance();
    Calendar calBalance = Calendar.getInstance();

    public PhonePromotion() {
        super();
        logger = Logger.getLogger(PhoneNewPromotion.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        db = new DbPhonePromotion();
        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
        smsBounesNewPhone = ResourceBundle.getBundle("configPayBonus").getString("smsBounesNewPhone");
    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        for (Record record : listRecord) {
            PhoneNewPromotion moRecord = (PhoneNewPromotion) record;
            moRecord.setNodeName(holder.getNodeName());
            moRecord.setClusterName(holder.getClusterName());
        }
        return listRecord;
    }

    @Override
    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
        List<Record> listResult = new ArrayList<Record>();
        long timeSt;
        boolean receivedBonus = false;
        SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddHHmmss");
        for (Record record : listRecord) {
            timeSt = System.currentTimeMillis();
            PhoneNewPromotion bn = (PhoneNewPromotion) record;
            listResult.add(bn);
            String tmpIsdn = bn.getMsisdn();
            if (!tmpIsdn.startsWith("258")) {
                tmpIsdn = "258" + bn.getMsisdn();
            }

//          B1: Check to make sure not yet processing this record, not duplicate process for each record (base on action_audit_id)
            logger.info("Start check to make sure not duplicate process getImei " + bn.getImei()
                    + " id " + bn.getId() + " Imei: " + bn.getImei());
            PhonePromotionConfig config = db.getPromotionConfig(bn.getTac());
            if (config == null) {
                logger.warn("The configuration invalid " + bn.getImei() + " at Time : " + timeSt);
                bn.setErrCode("E4");
                bn.setDescription("The configuration invalid");
                continue;
            }
            if (bn.getImei() != null) {
                /*
                 Kiem tra moi IMEI chi duoc nhan KH 1 lan (Tru DCOM co the nhan nhieu lan)
                 HandSetType =1: Dien thoai
                 HandSetType =2: DCOM,Modem Wiffi
                 */
//				if (db.checkAlreadyProcessRecord(bn.getImei(), bn.getMsisdn()) && config.getHandSetType() != 2) {
//					logger.warn("Already processed record ,IMEI " + bn.getImei());
//					bn.setErrCode("E1");
//					bn.setDescription("Already processed record, result success");
//					continue;
//				}

                /*
                 Truong hop IMEI DCOM thay da duoc cong KH nhung xuat hien trong bang hlr_62xx_subscriber_cache
                 khong xu ly, cho den chu ky tiep theo
                 */
                if (bn.getInputType() == 1 && config.getHandSetType() == 2) {
                    if (db.checkIMEIModemWiffi(bn.getImei())) {
                        logger.warn("This imei already existed in queue table, waiting to next cycle ,IMEI " + bn.getImei());
                        bn.setErrCode("E4");
                        bn.setDescription("This imei already existed in queue table, waiting to next cycle");
                        continue;
                    }
                }
                /**
                 * Bacnx 20200312 Update check valid IMSI and INDN, each
                 * ISDN,IMSI can receive the bonus only one time , and the IMSI
                 * must belong to MVT
                 */
                if (config.getHandSetType() == 1) {
                    boolean checkReceivedByImsi = db.checkReceivedBonusMobile(bn.getImei(), null);
                    if (checkReceivedByImsi) {
                        logger.warn("IMSI already received bonus ,IMEI " + bn.getImei());
                        bn.setErrCode("E5");
                        bn.setDescription("IMSI a already received bonus");
                        continue;
                    }

                    boolean checkReceivedByIsdn = db.checkReceivedBonusMobile(null, bn.getMsisdn());
                    if (checkReceivedByIsdn) {
                        logger.warn("ISDN already received bonus ,ISDN " + bn.getMsisdn());
                        bn.setErrCode("E6");
                        bn.setDescription("ISDN a already received bonus");
                        continue;
                    }
                } else {
                    boolean checkReceivedByImsi = db.checkImsiReceivedBonusWifi(bn.getImei(), bn.getMsisdn());
                    if (checkReceivedByImsi) {
                        logger.warn("IMSI already received bonus ,IMEI " + bn.getImei());
                        bn.setErrCode("E5");
                        bn.setDescription("IMSI a already received bonus");

                        //<editor-fold defaultstate="collapsed" desc="UPDATE LOG DCOM-WIFI">
					/*
                         Check policy for DCOM.WIFI(Handset Type =2)
                         Neu la modem wifi thi day vao bang queue de theo doi cong cho cac thang sau
                         Moi lan xu ly se sinh ra 1 ban ghi vao bang nay, cac ban ghi cu se duoc cap nhat
                         status =0, ban ghi moi nhat se co status =1
                         Neu so thang con lai >= so lan cong km (count trong bang his) thi se dung lai
                         */
                        if (bn.getInputType() == 2 && config.getHandSetType() == 2) {
                            int validTime = db.checkValidTimeBonus(bn.getImei());
                            if (config.getRemainMonths() >= validTime) {
                                if (db.checkIMEIModemWiffi(bn.getImei())) {
                                    if (bn.getRemainMonths() > 0) {
                                        bn.setRemainMonths(bn.getRemainMonths() - 1);
                                    } else {
                                        bn.setRemainMonths(0);
                                    }
                                } else {
                                    bn.setRemainMonths(config.getRemainMonths() - 1);
                                }
                                db.updateHistoryModemWifi(bn.getImei());
                                if (bn.getRemainMonths() == 0) {
                                    bn.setStatus(0);
                                } else {
                                    bn.setStatus(1);
                                }
                                db.insertSubscriberWiffi(bn);
                            } else {
                                db.updateHistoryModemWifi(bn.getImei());
                                logger.warn("Over times add bonus: " + bn.getMsisdn() + " imei " + bn.getImei() + "times " + validTime);
                                continue;
                            }
                        }

                        //</editor-fold>
                        continue;
                    }

                    boolean checkReceivedByIsdn = db.checkIsdnReceivedBonusWifi(bn.getImei(), bn.getMsisdn());
                    if (checkReceivedByIsdn) {
                        logger.warn("ISDN already received bonus ,ISDN " + bn.getMsisdn());
                        bn.setErrCode("E6");
                        bn.setDescription("ISDN a already received bonus");
                        //<editor-fold defaultstate="collapsed" desc="UPDATE LOG DCOM-WIFI">
					/*
                         Check policy for DCOM.WIFI(Handset Type =2)
                         Neu la modem wifi thi day vao bang queue de theo doi cong cho cac thang sau
                         Moi lan xu ly se sinh ra 1 ban ghi vao bang nay, cac ban ghi cu se duoc cap nhat
                         status =0, ban ghi moi nhat se co status =1
                         Neu so thang con lai >= so lan cong km (count trong bang his) thi se dung lai
                         */
                        if (bn.getInputType() == 2 && config.getHandSetType() == 2) {
                            int validTime = db.checkValidTimeBonus(bn.getImei());
                            if (config.getRemainMonths() >= validTime) {
                                if (db.checkIMEIModemWiffi(bn.getImei())) {
                                    if (bn.getRemainMonths() > 0) {
                                        bn.setRemainMonths(bn.getRemainMonths() - 1);
                                    } else {
                                        bn.setRemainMonths(0);
                                    }
                                } else {
                                    bn.setRemainMonths(config.getRemainMonths() - 1);
                                }
                                db.updateHistoryModemWifi(bn.getImei());
                                if (bn.getRemainMonths() == 0) {
                                    bn.setStatus(0);
                                } else {
                                    bn.setStatus(1);
                                }
                                db.insertSubscriberWiffi(bn);
                            } else {
                                db.updateHistoryModemWifi(bn.getImei());
                                logger.warn("Over times add bonus: " + bn.getMsisdn() + " imei " + bn.getImei() + "times " + validTime);
                                continue;
                            }
                        }

                        //</editor-fold>
                        continue;
                    }
                }

                //Check handset belong MVT
                boolean checkImei = db.checkHSBelongMVT(bn.getImei());
                if (!checkImei) {
                    logger.warn("The IMEI is not belong Movitel ,IMEI " + bn.getImei());
                    bn.setErrCode("E7");
                    bn.setDescription("The IMEI is not belong Movitel");
                    //<editor-fold defaultstate="collapsed" desc="UPDATE LOG DCOM-WIFI">
					/*
                     Check policy for DCOM.WIFI(Handset Type =2)
                     Neu la modem wifi thi day vao bang queue de theo doi cong cho cac thang sau
                     Moi lan xu ly se sinh ra 1 ban ghi vao bang nay, cac ban ghi cu se duoc cap nhat
                     status =0, ban ghi moi nhat se co status =1
                     Neu so thang con lai >= so lan cong km (count trong bang his) thi se dung lai
                     */
                    if (bn.getInputType() == 2 && config.getHandSetType() == 2) {
                        int validTime = db.checkValidTimeBonus(bn.getImei());
                        if (config.getRemainMonths() >= validTime) {
                            if (db.checkIMEIModemWiffi(bn.getImei())) {
                                if (bn.getRemainMonths() > 0) {
                                    bn.setRemainMonths(bn.getRemainMonths() - 1);
                                } else {
                                    bn.setRemainMonths(0);
                                }
                            } else {
                                bn.setRemainMonths(config.getRemainMonths() - 1);
                            }
                            db.updateHistoryModemWifi(bn.getImei());
                            if (bn.getRemainMonths() == 0) {
                                bn.setStatus(0);
                            } else {
                                bn.setStatus(1);
                            }
                            db.insertSubscriberWiffi(bn);
                        } else {
                            db.updateHistoryModemWifi(bn.getImei());
                            logger.warn("Over times add bonus: " + bn.getMsisdn() + " imei " + bn.getImei() + "times " + validTime);
                            continue;
                        }
                    }

                    //</editor-fold>
                    continue;
                }

                //Check profile
                boolean checkNewProfileNew = db.checkCorrectProfile(bn.getMsisdn());
                if (!checkNewProfileNew) {
                    boolean checkNewOldProfile = db.checkCorrectOldProfile(bn.getMsisdn());
                    if (!checkNewOldProfile) {
                        logger.warn("Subcriber invalid - Profile incorrect isdn " + bn.getMsisdn());
                        bn.setErrCode("E8");
                        bn.setDescription("Profile incorrect");
                        //<editor-fold defaultstate="collapsed" desc="UPDATE LOG DCOM-WIFI">
					/*
                         Check policy for DCOM.WIFI(Handset Type =2)
                         Neu la modem wifi thi day vao bang queue de theo doi cong cho cac thang sau
                         Moi lan xu ly se sinh ra 1 ban ghi vao bang nay, cac ban ghi cu se duoc cap nhat
                         status =0, ban ghi moi nhat se co status =1
                         Neu so thang con lai >= so lan cong km (count trong bang his) thi se dung lai
                         */
                        if (bn.getInputType() == 2 && config.getHandSetType() == 2) {
                            int validTime = db.checkValidTimeBonus(bn.getImei());
                            if (config.getRemainMonths() >= validTime) {
                                if (db.checkIMEIModemWiffi(bn.getImei())) {
                                    if (bn.getRemainMonths() > 0) {
                                        bn.setRemainMonths(bn.getRemainMonths() - 1);
                                    } else {
                                        bn.setRemainMonths(0);
                                    }
                                } else {
                                    bn.setRemainMonths(config.getRemainMonths() - 1);
                                }
                                db.updateHistoryModemWifi(bn.getImei());
                                if (bn.getRemainMonths() == 0) {
                                    bn.setStatus(0);
                                } else {
                                    bn.setStatus(1);
                                }
                                db.insertSubscriberWiffi(bn);
                            } else {
                                db.updateHistoryModemWifi(bn.getImei());
                                logger.warn("Over times add bonus: " + bn.getMsisdn() + " imei " + bn.getImei() + "times " + validTime);
                                continue;
                            }
                        }

                        //</editor-fold>
                        continue;
                    }
                }

            } else {
                logger.warn("The input invalid imei is null " + bn.getImei() + " at Time : " + timeSt);
                bn.setErrCode("E2");
                bn.setDescription("The input invalid imei is null");
                continue;
            }
            if (bn.getTac() == null) {
                logger.warn("The input invalid TAC is null " + bn.getImei() + " at Time : " + timeSt);
                bn.setErrCode("E3");
                bn.setDescription("The input invalid TAC is null");
                continue;
            }

//</editor-fold>
            //B2: Add promotion dependent on each TAC
            //<editor-fold defaultstate="collapsed" desc="add data">
            if (config.getDataAmount() > 0 && config.getDataValidDays() > 0 && !"".equals(config.getBalanceType())) {
                String rsAddData = addBonusData(config.getDataAmount() * 1024 * 1024, config.getDataValidDays(), bn.getMsisdn(), config.getDataType());
                if (!"0".equals(rsAddData)) {
                    logger.warn("Add data failed to this isdn: " + bn.getMsisdn() + " errorCode " + rsAddData + "time " + timeSt);
                    bn.setErrCode(bn.getErrCode() + "|E2");
                    bn.setDescription(bn.getDescription() + "|" + rsAddData);
                } else {
                    logger.warn("Add data successful to this isdn: " + bn.getMsisdn() + " errorCode " + rsAddData + "time " + timeSt);
                    bn.setErrCode("0");
                    bn.setDescription(bn.getDescription() + "|Add data successful");
                    receivedBonus = true;
                }
            }
            //</editor-fold>
            //<editor-fold defaultstate="collapsed" desc="add sms">
            if (config.getSmsAmount() > 0 && config.getSmsValidDays() > 0 && !"".equals(config.getSmsType())) {
                calSms.setTime(new Date());
                calSms.add(Calendar.DATE, config.getSmsValidDays());
                String rsAddSMS = pro.addSmsDataVoice(bn.getMsisdn(), String.valueOf(config.getSmsAmount()), config.getSmsType(), sdf2.format(calSms.getTime()));
                if (!"0".equals(rsAddSMS)) {
                    logger.warn("Add SMS failed to this isdn: " + bn.getMsisdn() + " errorCode " + rsAddSMS + "time " + timeSt);
                    bn.setErrCode(bn.getErrCode() + "|E3");
                    bn.setDescription(bn.getDescription() + "|Add SMS failed " + rsAddSMS);
                } else {
                    logger.warn("Add SMS successful to this isdn: " + bn.getMsisdn() + " errorCode " + rsAddSMS + "time " + timeSt);
                    bn.setErrCode("0");
                    bn.setDescription(bn.getDescription() + "|Add SMS successful");
                    receivedBonus = true;
                }
            }
            //</editor-fold>
            //<editor-fold defaultstate="collapsed" desc="add balance">
            if (config.getBalanceAmount() > 0 && config.getBalanceValidDays() > 0 && !"".equals(config.getBalanceType())) {
                calBalance.setTime(new Date());
                calBalance.add(Calendar.DATE, config.getBalanceValidDays());
                String addBalance = pro.addBalanceWeek(bn.getMsisdn(), String.valueOf(config.getBalanceAmount()), config.getBalanceType(), sdf2.format(calBalance.getTime()));
                if (!"0".equals(addBalance)) {
                    logger.warn("Add banlace for this customer failed: " + bn.getMsisdn() + " errorCode " + addBalance);
                    bn.setErrCode(bn.getErrCode() + "|E3");
                    bn.setDescription(bn.getDescription() + "|Add balance failed" + addBalance);
                } else {
                    logger.warn("Add balance successful to this isdn: " + bn.getMsisdn() + " errorCode " + addBalance + "time " + timeSt);
                    bn.setErrCode("0");
                    bn.setDescription(bn.getDescription() + "|Add balance successful");
                    receivedBonus = true;
                }
            }
            //</editor-fold>

            //<editor-fold defaultstate="collapsed" desc="UPDATE LOG DCOM-WIFI">
			/*
             Check policy for DCOM.WIFI(Handset Type =2)
             Neu la modem wifi thi day vao bang queue de theo doi cong cho cac thang sau
             Moi lan xu ly se sinh ra 1 ban ghi vao bang nay, cac ban ghi cu se duoc cap nhat
             status =0, ban ghi moi nhat se co status =1
             Neu so thang con lai >= so lan cong km (count trong bang his) thi se dung lai
             */
            if (config.getHandSetType() == 2) {
                int validTime = db.checkValidTimeBonus(bn.getImei());
                if (config.getRemainMonths() >= validTime) {
                    if (db.checkIMEIModemWiffi(bn.getImei())) {
                        if (bn.getRemainMonths() > 0) {
                            bn.setRemainMonths(bn.getRemainMonths() - 1);
                        } else {
                            bn.setRemainMonths(0);
                        }
                    } else {
                        bn.setRemainMonths(config.getRemainMonths() - 1);
                    }
                    db.updateHistoryModemWifi(bn.getImei());
                    if (bn.getRemainMonths() == 0) {
                        bn.setStatus(0);
                    } else {
                        bn.setStatus(1);
                    }
                    db.insertSubscriberWiffi(bn);
                } else {
                    db.updateHistoryModemWifi(bn.getImei());
                    logger.warn("Over times add bonus: " + bn.getMsisdn() + " imei " + bn.getImei() + "times " + validTime);
                    continue;
                }
            }
            if (receivedBonus) {
                db.sendSms(bn.getMsisdn(), config.getSmsToCus(), "86142");
                logger.info("Send SMS to customer " + bn.getMsisdn() + " " + config.getSmsToCus());
            } else {
                logger.info("Nothing added...please check the configuration " + bn.getMsisdn());
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
            PhoneNewPromotion bn = (PhoneNewPromotion) record;
            br.append("\r\n").
                    append("|\t").
                    append(bn.getMsisdn()).
                    append("||\t").
                    append((bn.getDatetime() != null ? sdf.format(bn.getDatetime()) : null)).
                    append("||\t").
                    append(bn.getClusterName()).
                    append("||\t").
                    append(bn.getImei());
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

    public String addBonusData(long dataValueMB, int validDay, String isdn, String balanceType) {
        String msisdn = isdn;
        if (!msisdn.startsWith("258")) {
            msisdn = "258" + msisdn;
        }
        Date sysdate = new Date();
        Calendar calExpireTime = Calendar.getInstance();
        calExpireTime.setTime(sysdate);
        calExpireTime.add(Calendar.DATE, validDay);
        SimpleDateFormat sdfEx = new SimpleDateFormat("yyyyMMddHHmmss");
        logger.info("Start Add data " + msisdn + " value data(MB)  " + dataValueMB);
        String resultAddData = pro.addSmsDataVoice(msisdn, (dataValueMB) + "", balanceType, sdfEx.format(calExpireTime.getTime()));
        if (!"0".equals(resultAddData)) {
            logger.info("Add data un-successfull result " + resultAddData + " " + msisdn + " value data(MB)  " + dataValueMB);
            return "Add data un-successfull";
        }
        return "0";
    }
}
