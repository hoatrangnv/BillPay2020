///*
// * To change this template, choose Tools | Templates
// * and open the template in the editor.
// */
//package com.viettel.paybonus.process;
//
//import com.viettel.cluster.agent.integration.Record;
//import com.viettel.paybonus.database.DbSalary;
//import com.viettel.paybonus.database.DbVipSub;
//import com.viettel.paybonus.obj.SalaryInfo;
//import com.viettel.paybonus.obj.VipSubDetail;
//import com.viettel.paybonus.service.Exchange;
//import com.viettel.threadfw.manager.AppManager;
//import java.util.List;
//import org.apache.log4j.Logger;
//import com.viettel.threadfw.process.ProcessRecordAbstract;
//import com.viettel.vas.util.ExchangeClientChannel;
//import java.text.SimpleDateFormat;
//import java.util.ArrayList;
//import java.util.Calendar;
//import java.util.Date;
//import java.util.ResourceBundle;
//
///**
// *
// * @author HuyNQ13
// * @version 1.0
// * @since 24-03-2016
// */
//public class CalculateSalary extends ProcessRecordAbstract {
//
////    Exchange pro;
//    DbSalary db;
//    SimpleDateFormat sdf;
//    Long minKit;
//    Long minScraft;
//    Long minHanset;
//    Long minEmola;
//    Double rateScraft;
//    Double rateKit;
//    Double rateHandset;
//    Double rateEmola;
//
//    public CalculateSalary() {
//        super();
//        logger = Logger.getLogger(CalculateSalary.class);
//    }
//
//    @Override
//    public void initBeforeStart() throws Exception {
//        minKit = Long.valueOf(ResourceBundle.getBundle("configSalary").getString("minKit"));
//        minScraft = Long.valueOf(ResourceBundle.getBundle("configSalary").getString("minScraft"));
//        minHanset = Long.valueOf(ResourceBundle.getBundle("configSalary").getString("minHanset"));
//        minEmola = Long.valueOf(ResourceBundle.getBundle("configSalary").getString("minEmola"));
//        rateScraft = Double.valueOf(ResourceBundle.getBundle("configSalary").getString("rateScraft"));
//        rateKit = Double.valueOf(ResourceBundle.getBundle("configSalary").getString("rateKit"));
//        rateHandset = Double.valueOf(ResourceBundle.getBundle("configSalary").getString("rateHandset"));
//        rateEmola = Double.valueOf(ResourceBundle.getBundle("configSalary").getString("rateEmola"));
////        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
//        db = new DbSalary();
//        sdf = new SimpleDateFormat("yyyy-MM");
//    }
//
//    @Override
//    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
//        return listRecord;
//    }
//
//    @Override
//    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
//        List<Record> listResult = new ArrayList<Record>();
//        boolean isNewCycle;
//        String salaryCycle;
//        Long countWorkDay;
//        Long countChannelVisit;
//        String listAbsentDay;
//        String listChannelVisit;
//        Long moneyFix;
//        Long moneyPetrol;
//        Long moneyPhone;
//        Long bonusScraft;
//        Long bonusKit;
//        Long bonusHandset;
//        Long bonusTopup;
//        Long bonusEmola;
//        Long percentKit;
//        Long percentScraft;
//        Long percentHandset;
//        Long percentEmola;
//        Long countSaleKit;
//        Long countSaleScraft;
//        Long countSaleHandset;
//        Long countSaleEmola;
//        Long totalSalary;
//        String listTransId;
//        for (Record record : listRecord) {
//            isNewCycle = false;
//            SalaryInfo si = (SalaryInfo) record;
////            Step 1: Check new cycle salary (is the second day of month, must reset value), if true set cycle_id
//            Calendar cal = Calendar.getInstance();
//            cal.setTime(new Date());
//            int dayOfMonth = cal.get(Calendar.DAY_OF_MONTH);
//            if (dayOfMonth == 2) {
//                logger.info("New cycle, reset all value money, bonus, percent, count " + si.getStaffCode());
//                isNewCycle = true;
//                cal.add(Calendar.MONTH, -1); // get last month
//                salaryCycle = sdf.format(cal.getTime());
//                countWorkDay = 30l;
//                countChannelVisit = 0l;
//                listAbsentDay = "";
//                listChannelVisit = "";
//                moneyFix = si.getMoneyFix();
//                moneyPetrol = si.getMoneyPetrol();
//                moneyPhone = si.getMoneyPhone();
//                bonusScraft = 0l;
//                bonusKit = 0l;
//                bonusHandset = 0l;
//                bonusTopup = 0l;
//                bonusEmola = 0l;
//                percentKit = 0l;
//                percentScraft = 0l;
//                percentHandset = 0l;
//                percentEmola = 0l;
//                countSaleKit = 0l;
//                countSaleScraft = 0l;
//                countSaleHandset = 0l;
//                countSaleEmola = 0l;
//                totalSalary = 0l;
//                listTransId = "";
//            } else {
//                logger.info("Normal day, get and set last money, bonus, percent, count " + si.getStaffCode());
//                countWorkDay = si.getCountWorkDay();
//                countChannelVisit = si.getCountChannelVisit();
//                listAbsentDay = si.getListAbsentDay();
//                listChannelVisit = ""; //list channel visit only calculate in current day
//                moneyFix = si.getMoneyFix();
//                moneyPetrol = si.getMoneyPetrol();
//                moneyPhone = si.getMoneyPhone();
//                bonusScraft = si.getBonusScraft();
//                bonusKit = si.getBonusKit();
//                bonusHandset = si.getBonusHandset();
//                bonusTopup = si.getBonusTopup();
//                bonusEmola = si.getBonusEmola();
//                percentKit = 0l; // percen will calculate again, no need get set last value
//                percentScraft = 0l;
//                percentHandset = 0l;
//                percentEmola = 0l;
//                countSaleKit = si.getCountSaleKit();
//                countSaleScraft = si.getCountSaleScraft();
//                countSaleHandset = si.getCountSaleHandset();
//                countSaleEmola = si.getCountSaleEmola();
//                totalSalary = ;
//                listTransId = "";
//            }
////            Step 2: Check pending days (system hung not process, now must run again).
//
////            Step 3: Check absent day, set count_work_day again, and get list trans_id and list_channel.
//
////            Step 4: Calculate revenue by day for scraft card, kit and handset.
//
////            Step 5: Calculate bonus
//
////            Step 6: Calculate percent complete
//
////            Step 7: Calculate total salary
//
//
//            sbDes = new StringBuilder();
//            sbDes.setLength(0);
//            errCode = "";
//            rsMoney = "";
//            rsSms = "";
//            rsSmsOut = "";
//            rsData = "";
//            rsVoice = "";
//            rsVoiceOut = "";
//            isPreSub = false;
//            isPosSub = false;
//            VipSubDetail bn = (VipSubDetail) record;
//            listResult.add(bn);
//            if ("0".equals(bn.getResultCode())) {
////                Step -1 check isdn is Movitel sub
//                isPreSub = db.checkPreSub(bn.getIsdn());
//                if (!isPreSub) {
//                    isPosSub = db.checkPosSub(bn.getIsdn());
//                    if (!isPosSub) {
//                        logger.warn("Not Movitel subscriber " + bn.getIsdn());
//                        bn.setResultCode("E98");
//                        bn.setDescription("Already process record");
//                        continue;
//                    }
//                }
////                Step 0: calculate next_process_time
//                logger.info("Start calculate next process time for sub " + bn.getIsdn()
//                        + " current processtime " + sdf.format(bn.getHisNextProcessTime()));
//                Calendar cal = Calendar.getInstance();
//                cal.setTime(bn.getHisNextProcessTime());
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
////                Step 1: Check to make sure not yet processing this record, not duplicate process for each record
//                logger.info("Start check to make sure not duplicate process id " + bn.getVipSubDetailId() + " isdn " + bn.getIsdn());
//                if (bn.getHisNextProcessTime() != null
//                        && db.checkAlreadyProcessRecord(bn.getIsdn(), sdf.format(bn.getHisNextProcessTime()))) {
//                    logger.warn("Already process record actionId " + bn.getVipSubDetailId());
//                    bn.setResultCode("E01");
//                    bn.setDescription("Already process record");
//                    continue;
//                }
////                Step 2: check and add money
//                if (bn.getMoneyAcc() != null && !"".equals(bn.getMoneyAcc().trim())
//                        && bn.getMoneyValue() != null && !"".equals(bn.getMoneyValue().trim())) {
//                    logger.info("start add money accountid " + bn.getMoneyAcc() + " value " + bn.getMoneyValue()
//                            + " isdn " + bn.getIsdn());
//                    rsMoney = pro.addMoney(bn.getIsdn(), bn.getMoneyValue(), bn.getMoneyAcc());
//                    if ("0".equals(rsMoney)) {
//                        logger.info("successfully add money accountid " + bn.getMoneyAcc() + " value " + bn.getMoneyValue()
//                                + " isdn " + bn.getIsdn());
//                        sbDes.append(bn.getMoneyAcc()).append("|").append(bn.getMoneyValue()).append("ok").append("|");
//                    } else {
//                        logger.error("fail to add money accountid " + bn.getMoneyAcc() + " value " + bn.getMoneyValue()
//                                + " isdn " + bn.getIsdn());
//                        sbDes.append(bn.getMoneyAcc()).append("|").append(bn.getMoneyValue()).append("fail").append("|");
//                        errCode = "E02";
//                    }
//                }
////                Step 3: Check and add sms
//                if (bn.getSmsAcc() != null && !"".equals(bn.getSmsAcc().trim())
//                        && bn.getSmsValue() != null && !"".equals(bn.getSmsValue().trim())) {
//                    logger.info("start add sms accountid " + bn.getSmsAcc() + " value " + bn.getSmsValue()
//                            + " isdn " + bn.getIsdn());
//                    rsSms = pro.addSmsDataVoice(bn.getIsdn(), bn.getSmsValue(), bn.getSmsAcc(), null);
//                    if ("0".equals(rsSms)) {
//                        logger.info("successfully add sms accountid " + bn.getSmsAcc() + " value " + bn.getSmsValue()
//                                + " isdn " + bn.getIsdn());
//                        sbDes.append(bn.getSmsAcc()).append("|").append(bn.getSmsValue()).append("ok").append("|");
//                    } else {
//                        logger.error("fail to add sms accountid " + bn.getSmsAcc() + " value " + bn.getSmsValue()
//                                + " isdn " + bn.getIsdn());
//                        sbDes.append(bn.getSmsAcc()).append("|").append(bn.getSmsValue()).append("fail").append("|");
//                        if (!"E02".equals(errCode)) {
//                            errCode = "E03";
//                        } else {
//                            errCode = "E04";
//                        }
//                    }
//                }
////                Step 4: Check and add sms out
//                if (bn.getSmsOutAcc() != null && !"".equals(bn.getSmsOutAcc().trim())
//                        && bn.getSmsOutValue() != null && !"".equals(bn.getSmsOutValue().trim())) {
//                    logger.info("start add smsout accountid " + bn.getSmsOutAcc() + " value " + bn.getSmsOutValue()
//                            + " isdn " + bn.getIsdn());
//                    rsSmsOut = pro.addSmsDataVoice(bn.getIsdn(), bn.getSmsOutValue(), bn.getSmsOutAcc(), null);
//                    if ("0".equals(rsSmsOut)) {
//                        logger.info("successfully add smsout accountid " + bn.getSmsOutAcc() + " value " + bn.getSmsOutValue()
//                                + " isdn " + bn.getIsdn());
//                        sbDes.append(bn.getSmsOutAcc()).append("|").append(bn.getSmsOutValue()).append("ok").append("|");
//                    } else {
//                        logger.error("fail to add smsout accountid " + bn.getSmsOutAcc() + " value " + bn.getSmsOutValue()
//                                + " isdn " + bn.getIsdn());
//                        sbDes.append(bn.getSmsOutAcc()).append("|").append(bn.getSmsOutValue()).append("fail").append("|");
//                        if (!"E02".equals(errCode) && !"E03".equals(errCode) && !"E04".equals(errCode)) {
//                            errCode = "E05";
//                        } else if ("E04".equals(errCode)) {
//                            errCode = "E06";
//                        } else if ("E02".equals(errCode)) {
//                            errCode = "E07";
//                        } else if ("E03".equals(errCode)) {
//                            errCode = "E08";
//                        }
//                    }
//                }
////                Step 5: Check and add data
//                if (bn.getDataAcc() != null && !"".equals(bn.getDataAcc().trim())
//                        && bn.getDataValue() != null && !"".equals(bn.getDataValue().trim())) {
//                    logger.info("start add data accountid " + bn.getDataAcc() + " value " + bn.getDataValue()
//                            + " isdn " + bn.getIsdn());
//                    int dataValue = Integer.valueOf(bn.getDataValue()) * 1024 * 1024;
//                    logger.info("Convert value from megabyte to byte for sub " + bn.getIsdn() + " byteValue " + String.valueOf(dataValue));
//                    rsData = pro.addSmsDataVoice(bn.getIsdn(), String.valueOf(dataValue), bn.getDataAcc(), null);
//                    if ("0".equals(rsData)) {
//                        logger.info("successfully add data accountid " + bn.getDataAcc() + " value " + bn.getDataValue()
//                                + " isdn " + bn.getIsdn());
//                        sbDes.append(bn.getDataAcc()).append("|").append(bn.getDataValue()).append("ok").append("|");
//                    } else {
//                        logger.error("fail to add data accountid " + bn.getDataAcc() + " value " + bn.getDataValue()
//                                + " isdn " + bn.getIsdn());
//                        sbDes.append(bn.getDataAcc()).append("|").append(bn.getDataValue()).append("fail").append("|");
//                        if (!"E02".equals(errCode) && !"E03".equals(errCode) && !"E04".equals(errCode) && !"E05".equals(errCode)) {
//                            errCode = "E09";
//                        } else if ("E06".equals(errCode)) {
//                            errCode = "E10";
//                        } else if ("E07".equals(errCode)) {
//                            errCode = "E11";
//                        } else if ("E08".equals(errCode)) {
//                            errCode = "E12";
//                        } else if ("E02".equals(errCode)) {
//                            errCode = "E13";
//                        } else if ("E03".equals(errCode)) {
//                            errCode = "E14";
//                        } else if ("E04".equals(errCode)) {
//                            errCode = "E15";
//                        } else if ("E05".equals(errCode)) {
//                            errCode = "E16";
//                        }
//                    }
//                }
////                Step 6: Check and add voice
//                if (bn.getVoiceAcc() != null && !"".equals(bn.getVoiceAcc().trim())
//                        && bn.getVoiceValue() != null && !"".equals(bn.getVoiceValue().trim())) {
//                    logger.info("start add Voice accountid " + bn.getVoiceAcc() + " value " + bn.getVoiceValue()
//                            + " isdn " + bn.getIsdn());
//                    rsVoice = pro.addSmsDataVoice(bn.getIsdn(), bn.getVoiceValue(), bn.getVoiceAcc(), null);
//                    if ("0".equals(rsVoice)) {
//                        logger.info("successfully add Voice accountid " + bn.getVoiceAcc() + " value " + bn.getVoiceValue()
//                                + " isdn " + bn.getIsdn());
//                        sbDes.append(bn.getVoiceAcc()).append("|").append(bn.getVoiceValue()).append("ok").append("|");
//                    } else {
//                        logger.error("fail to add Voice accountid " + bn.getVoiceAcc() + " value " + bn.getVoiceValue()
//                                + " isdn " + bn.getIsdn());
//                        sbDes.append(bn.getVoiceAcc()).append("|").append(bn.getVoiceValue()).append("fail").append("|");
//                        if (!"E02".equals(errCode) && !"E03".equals(errCode) && !"E04".equals(errCode) && !"E05".equals(errCode)
//                                && !"E09".equals(errCode)) {
//                            errCode = "E17";
//                        } else if ("E06".equals(errCode)) {
//                            errCode = "E18";
//                        } else if ("E07".equals(errCode)) {
//                            errCode = "E19";
//                        } else if ("E08".equals(errCode)) {
//                            errCode = "E20";
//                        } else if ("E02".equals(errCode)) {
//                            errCode = "E21";
//                        } else if ("E03".equals(errCode)) {
//                            errCode = "E22";
//                        } else if ("E04".equals(errCode)) {
//                            errCode = "E22";
//                        } else if ("E05".equals(errCode)) {
//                            errCode = "E23";
//                        } else if ("E10".equals(errCode)) {
//                            errCode = "E24";
//                        } else if ("E11".equals(errCode)) {
//                            errCode = "E25";
//                        } else if ("E12".equals(errCode)) {
//                            errCode = "E26";
//                        } else if ("E13".equals(errCode)) {
//                            errCode = "E27";
//                        } else if ("E14".equals(errCode)) {
//                            errCode = "E28";
//                        } else if ("E15".equals(errCode)) {
//                            errCode = "E29";
//                        } else if ("E16".equals(errCode)) {
//                            errCode = "E30";
//                        } else if ("E09".equals(errCode)) {
//                            errCode = "E31";
//                        }
//                    }
//                }
////                Step 7: Check and add voice out
//                if (bn.getVoiceOutAcc() != null && !"".equals(bn.getVoiceOutAcc().trim())
//                        && bn.getVoiceOutValue() != null && !"".equals(bn.getVoiceOutValue().trim())) {
//                    logger.info("start add VoiceOut accountid " + bn.getVoiceOutAcc() + " value " + bn.getVoiceOutValue()
//                            + " isdn " + bn.getIsdn());
//                    rsVoiceOut = pro.addSmsDataVoice(bn.getIsdn(), bn.getVoiceOutValue(), bn.getVoiceOutAcc(), null);
//                    if ("0".equals(rsVoiceOut)) {
//                        logger.info("successfully add VoiceOut accountid " + bn.getVoiceOutAcc() + " value " + bn.getVoiceOutValue()
//                                + " isdn " + bn.getIsdn());
//                        sbDes.append(bn.getVoiceOutAcc()).append("|").append(bn.getVoiceOutValue()).append("ok").append("|");
//                    } else {
//                        logger.error("fail to add VoiceOut accountid " + bn.getVoiceOutAcc() + " value " + bn.getVoiceOutValue()
//                                + " isdn " + bn.getIsdn());
//                        sbDes.append(bn.getVoiceOutAcc()).append("|").append(bn.getVoiceOutValue()).append("fail").append("|");
//                    }
//                }
////                Step 8: Check all results                
//                if (("".equals(rsMoney) || "0".equals(rsMoney))
//                        && ("".equals(rsData) || "0".equals(rsData))
//                        && ("".equals(rsSms) || "0".equals(rsSms))
//                        && ("".equals(rsSmsOut) || "0".equals(rsSmsOut))
//                        && ("".equals(rsVoice) || "0".equals(rsVoice))
//                        && ("".equals(rsVoiceOut) || "0".equals(rsVoiceOut))) {
//                    logger.info("All steps success " + bn.getIsdn());
//                    bn.setResultCode("0");
//                    bn.setDescription("All benefits added");
//                    continue;
//                } else {
//                    if (!"".equals(errCode)) {
//                        logger.warn("Some step fail " + bn.getIsdn());
//                        bn.setResultCode(errCode);
//                        bn.setDescription(sbDes.toString());
//                        continue;
//                    } else {
//                        logger.warn("Have step fail but not define, let see description in DB " + bn.getIsdn());
//                        bn.setResultCode("E99");
//                        bn.setDescription(sbDes.toString());
//                        continue;
//                    }
//                }
//            } else {
//                logger.warn("After validate respone code is fail id " + bn.getVipSubDetailId()
//                        + " so continue with other transaction");
//                continue;
//            }
//        }
//        listRecord.clear();
//        return listResult;
//    }
//
//    @Override
//    public void printListRecord(List<Record> listRecord) throws Exception {
//        StringBuilder br = new StringBuilder();
//        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
//        br.setLength(0);
//        br.append("\r\n").
//                append("|\tSTAFF_CODE|").
//                append("|\tTOTAL_SALARY\t|").
//                append("|\tLAST_PROCESS_TIME\t|");
//        for (Record record : listRecord) {
//            SalaryInfo bn = (SalaryInfo) record;
//            br.append("\r\n").
//                    append("|\t").
//                    append(bn.getStaffCode()).
//                    append("||\t").
//                    append(bn.getTotalSalary()).
//                    append("||\t").
//                    append((bn.getLastProcessTime() != null ? sdf.format(bn.getLastProcessTime()) : null));
//        }
//        logger.info(br);
//    }
//
//    @Override
//    public List<Record> processException(List<Record> listRecord, Exception ex) {
////        logger.warn("TEMPLATE process exception record: " + ex.toString());
////        for (Record record : listRecord) {
////            logger.info("TEMPLATE let convert to recort type you want and then set errCode, errDesc at here");
//////            MoRecord moRecord = (MoRecord) record;
//////            moRecord.setMessage("Thao tac that bai!");
//////            moRecord.setErrCode("-5");
////        }
//        return listRecord;
//    }
//
//    @Override
//    public boolean startProcessRecord() {
//        return true;
//    }
//}
