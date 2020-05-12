/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbCmpreProcessor;
import com.viettel.paybonus.database.DbSubProfileProcessor;
import com.viettel.paybonus.obj.Bonus;
import com.viettel.paybonus.service.Exchange;
import com.viettel.threadfw.manager.AppManager;
import java.util.List;
import org.apache.log4j.Logger;
import com.viettel.threadfw.process.ProcessRecordAbstract;
import com.viettel.vas.util.ExchangeClientChannel;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.ResourceBundle;

/**
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class SendSmsMarketing extends ProcessRecordAbstract {

    Exchange pro;
    DbCmpreProcessor db;
    private Long sleepTime = 60 * 1000L;
    String countryCode;
    String day1;
    String day2;
    String day3;
    String day4;
    String day5;
    String day6;
    String day7;
    String day8;
    String day9;

    public SendSmsMarketing() {
        super();
        logger = Logger.getLogger(SendSmsMarketing.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        countryCode = ResourceBundle.getBundle("configPayBonus").getString("country_code");
        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
        db = new DbCmpreProcessor();

        day1 = ResourceBundle.getBundle("configPayBonus").getString("day1");
        day2 = ResourceBundle.getBundle("configPayBonus").getString("day2");
        day3 = ResourceBundle.getBundle("configPayBonus").getString("day3");
        day4 = ResourceBundle.getBundle("configPayBonus").getString("day4");
        day5 = ResourceBundle.getBundle("configPayBonus").getString("day5");
        day6 = ResourceBundle.getBundle("configPayBonus").getString("day6");
        day7 = ResourceBundle.getBundle("configPayBonus").getString("day7");
        day8 = ResourceBundle.getBundle("configPayBonus").getString("day8");
        day9 = ResourceBundle.getBundle("configPayBonus").getString("day9");
//        List<Config> result = db.getConfig();
//        for (int i = 0; i < result.size(); i++) {
//            if (result.get(i).getValuesId().equals("Day1")) {
//                day1 = result.get(i).getValuesName();
//                continue;
//            }
//            if (result.get(i).getValuesId().equals("Day2")) {
//                day2 = result.get(i).getValuesName();
//                continue;
//            }
//            if (result.get(i).getValuesId().equals("Day3")) {
//                day3 = result.get(i).getValuesName();
//                continue;
//            }
//            if (result.get(i).getValuesId().equals("Day4")) {
//                day4 = result.get(i).getValuesName();
//                continue;
//            }
//            if (result.get(i).getValuesId().equals("Day5")) {
//                day5 = result.get(i).getValuesName();
//                continue;
//            }
//            if (result.get(i).getValuesId().equals("Day6")) {
//                day6 = result.get(i).getValuesName();
//                continue;
//            }
//            if (result.get(i).getValuesId().equals("Day7")) {
//                day7 = result.get(i).getValuesName();
//                continue;
//            }
//            if (result.get(i).getValuesId().equals("Day8")) {
//                day8 = result.get(i).getValuesName();
//                continue;
//            }
//            if (result.get(i).getValuesId().equals("Day9")) {
//                day9 = result.get(i).getValuesName();
//                continue;
//            }
//        }
    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        return listRecord;
    }

    @Override
    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
        List<Record> listResult = new ArrayList<Record>();
        for (Record record : listRecord) {
            Bonus bn = (Bonus) record;
            listResult.add(bn);
            if (bn.getDaySentSms() == 0L) {
                logger.warn("-------------- Start send SMS day 1 with sub_profile_id = " + bn.getIsdnCustomer() + "-----------");
                if (!db.checkAlreadyWarningInDay(bn.getIsdnCustomer(), day1)
                        && !db.checkAlreadyWarningInDayQueue(bn.getIsdnCustomer(), day1)) {
                    db.sendSms(bn.getIsdnCustomer(), day1, "86904");
                    db.updateSentSms(1, Integer.parseInt(bn.getID()));
                } else {
                    logger.warn("-------------- Dublicate send sms in day0 with sub_profile_id = " + bn.getIsdnCustomer() + "-----------");
                    db.updateSentSms(1, Integer.parseInt(bn.getID()));
                }
                continue;
            }
            if (bn.getDaySentSms() == 1L) {
                logger.warn("-------------- Start send SMS day 2 with sub_profile_id = " + bn.getIsdnCustomer() + "-----------");
                if (!db.checkAlreadyWarningInDay(bn.getIsdnCustomer(), day2)
                        && !db.checkAlreadyWarningInDayQueue(bn.getIsdnCustomer(), day2)) {
                    db.sendSms(bn.getIsdnCustomer(), day2, "86904");
                    db.updateSentSms(2, Integer.parseInt(bn.getID()));
                } else {
                    logger.warn("-------------- Dublicate send sms in day1 with sub_profile_id = " + bn.getIsdnCustomer() + "-----------");
                    db.updateSentSms(2, Integer.parseInt(bn.getID()));
                }
                continue;
            }
            if (bn.getDaySentSms() == 2L) {
                logger.warn("-------------- Start send SMS day 3 with sub_profile_id = " + bn.getIsdnCustomer() + "-----------");
                if (!db.checkAlreadyWarningInDay(bn.getIsdnCustomer(), day3)
                        && !db.checkAlreadyWarningInDayQueue(bn.getIsdnCustomer(), day3)) {
                    db.sendSms(bn.getIsdnCustomer(), day3, "86904");
                    db.updateSentSms(3, Integer.parseInt(bn.getID()));
                } else {
                    logger.warn("-------------- Dublicate send sms in day2 with sub_profile_id = " + bn.getIsdnCustomer() + "-----------");
                    db.updateSentSms(3, Integer.parseInt(bn.getID()));
                }
                continue;
            }
            if (bn.getDaySentSms() == 3L) {
                logger.warn("-------------- Start send SMS day 4 with sub_profile_id = " + bn.getIsdnCustomer() + "-----------");
                if (!db.checkAlreadyWarningInDay(bn.getIsdnCustomer(), day4)
                        && !db.checkAlreadyWarningInDayQueue(bn.getIsdnCustomer(), day4)) {
                    db.sendSms(bn.getIsdnCustomer(), day4, "86904");
                    db.updateSentSms(4, Integer.parseInt(bn.getID()));
                } else {
                    logger.warn("-------------- Dublicate send sms in day3 with sub_profile_id = " + bn.getIsdnCustomer() + "-----------");
                    db.updateSentSms(4, Integer.parseInt(bn.getID()));
                }
                continue;
            }
            if (bn.getDaySentSms() == 4L) {
                logger.warn("-------------- Start send SMS day 5 with sub_profile_id = " + bn.getIsdnCustomer() + "-----------");
                if (!db.checkAlreadyWarningInDay(bn.getIsdnCustomer(), day5)
                        && !db.checkAlreadyWarningInDayQueue(bn.getIsdnCustomer(), day5)) {
                    db.sendSms(bn.getIsdnCustomer(), day5, "86904");
                    db.updateSentSms(5, Integer.parseInt(bn.getID()));
                } else {
                    logger.warn("-------------- Dublicate send sms in day4 with sub_profile_id = " + bn.getIsdnCustomer() + "-----------");
                    db.updateSentSms(5, Integer.parseInt(bn.getID()));
                }
                continue;
            }
            if (bn.getDaySentSms() == 5L) {
                logger.warn("-------------- Start send SMS day 6 with sub_profile_id = " + bn.getIsdnCustomer() + "-----------");
                if (!db.checkAlreadyWarningInDay(bn.getIsdnCustomer(), day6)
                        && !db.checkAlreadyWarningInDayQueue(bn.getIsdnCustomer(), day6)) {
                    db.sendSms(bn.getIsdnCustomer(), day6, "86904");
                    db.updateSentSms(6, Integer.parseInt(bn.getID()));
                } else {
                    logger.warn("-------------- Dublicate send sms in day5 with sub_profile_id = " + bn.getIsdnCustomer() + "-----------");
                    db.updateSentSms(6, Integer.parseInt(bn.getID()));
                }
                continue;
            }
            if (bn.getDaySentSms() == 6L) {
                logger.warn("-------------- Start send SMS day 7 with sub_profile_id = " + bn.getIsdnCustomer() + "-----------");
                if (!db.checkAlreadyWarningInDay(bn.getIsdnCustomer(), day7)
                        && !db.checkAlreadyWarningInDayQueue(bn.getIsdnCustomer(), day7)) {
                    db.sendSms(bn.getIsdnCustomer(), day7, "86904");
                    db.updateSentSms(7, Integer.parseInt(bn.getID()));
                } else {
                    logger.warn("-------------- Dublicate send sms in day6 with sub_profile_id = " + bn.getIsdnCustomer() + "-----------");
                    db.updateSentSms(7, Integer.parseInt(bn.getID()));
                }
                continue;
            }
            if (bn.getDaySentSms() == 7L) {
                logger.warn("-------------- Start send SMS day 8 with sub_profile_id = " + bn.getIsdnCustomer() + "-----------");
                if (!db.checkAlreadyWarningInDay(bn.getIsdnCustomer(), day8)
                        && !db.checkAlreadyWarningInDayQueue(bn.getIsdnCustomer(), day8)) {
                    db.sendSms(bn.getIsdnCustomer(), day8, "86904");
                    db.updateSentSms(8, Integer.parseInt(bn.getID()));
                } else {
                    logger.warn("-------------- Dublicate send sms in day7 with sub_profile_id = " + bn.getIsdnCustomer() + "-----------");
                    db.updateSentSms(8, Integer.parseInt(bn.getID()));
                }
                continue;
            }
            if (bn.getDaySentSms() == 8L) {
                logger.warn("-------------- Start send SMS day 9 with sub_profile_id = " + bn.getIsdnCustomer() + "-----------");
                if (!db.checkAlreadyWarningInDay(bn.getIsdnCustomer(), day8)
                        && !db.checkAlreadyWarningInDayQueue(bn.getIsdnCustomer(), day8)) {
                    db.sendSms(bn.getIsdnCustomer(), day9, "86904");
                    db.updateSentSms(9, Integer.parseInt(bn.getID()));
                } else {
                    logger.warn("-------------- Dublicate send sms in day8 with sub_profile_id = " + bn.getIsdnCustomer() + "-----------");
                    db.updateSentSms(9, Integer.parseInt(bn.getID()));
                }
                continue;
            }

        }
        logger.info("Sleeping " + sleepTime);
        Thread.sleep(sleepTime);
        listRecord.clear();
        return listResult;
    }

    @Override
    public void printListRecord(List<Record> listRecord) throws Exception {
        StringBuilder br = new StringBuilder();
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        br.setLength(0);
        br.append("\r\n").
                append("|\tID|").
                append("|\tACTION_AUDIT_ID|").
                append("|\tPK_ID|").
                append("|\tCREATE_STAFF\t|").
                append("|\tCREATE_SHOP\t|").
                append("|\tCREATE_TIME\t|").
                append("|\tCHECK_STAFF\t|").
                append("|\tCHECK_INFO\t|").
                append("|\tCHECK_TIME\t|").
                append("|\tISDN\t|").
                append("|\tBONUS_STATUS\t|").
                append("|\tREASON_ID\t|").
                append("|\tBLOCK_OCS_HLR\t|").
                append("|\tDAY_SENT_SMS\t|");
        for (Record record : listRecord) {
            Bonus bn = (Bonus) record;
            br.append("\r\n").
                    append("|\t").
                    append(bn.getId()).
                    append("||\t").
                    append(bn.getActionAuditId()).
                    append("||\t").
                    append(bn.getPkId()).
                    append("||\t").
                    append(bn.getUserName()).
                    append("||\t").
                    append(bn.getShopCode()).
                    append("||\t").
                    append((bn.getIssueDateTime() != null ? sdf.format(bn.getIssueDateTime()) : null)).
                    append("||\t").
                    append(bn.getStaffCheck()).
                    append("||\t").
                    append(bn.getCheckInfo()).
                    append("||\t").
                    append((bn.getTimeCheck() != null ? sdf.format(bn.getTimeCheck()) : null)).
                    append("||\t").
                    append(bn.getIsdnCustomer()).
                    append("||\t").
                    append(bn.getBonusStatus()).
                    append("||\t").
                    append(bn.getReasonId()).
                    append("||\t").
                    append(bn.getBlockOcsHlr()).
                    append("||\t").
                    append(bn.getDaySentSms());
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
