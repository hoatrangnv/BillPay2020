///*
// * To change this template, choose Tools | Templates
// * and open the template in the editor.
// */
//package com.viettel.paybonus.process;
//
//import com.viettel.cluster.agent.integration.Record;
//import com.viettel.paybonus.database.DbBounesMonthly;
//import com.viettel.paybonus.obj.BounesMonthly;
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
//public class PayBounesMonthly extends ProcessRecordAbstract {
//
//    Exchange pro;
//    DbBounesMonthly db;
//    private SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddHHmmss");
//    String smsReceiveSecond ;
//    public PayBounesMonthly() {
//        super();
//        logger = Logger.getLogger(BounesMonthly.class);
//    }
//
//    @Override
//    public void initBeforeStart() throws Exception {
//        db = new DbBounesMonthly();
////        pro = new Exchange(ExchangeClientChannel.getInstance("D:\\STUDY\\Project\\Movitel\\mBCCS_MOZ_FULL\\PayBonus\\etc\\exchange_client.cfg").getInstanceChannel(), logger);
//        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
//        smsReceiveSecond = ResourceBundle.getBundle("configPayBonus").getString("smsReceiveSecond");
//    }
//
//    @Override
//    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
//        for (Record record : listRecord) {
//            BounesMonthly moRecord = (BounesMonthly) record;
//            moRecord.setNodeName(holder.getNodeName());
//            moRecord.setClusterName(holder.getClusterName());
//        }
//        return listRecord;
//    }
//
//    @Override
//    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
//        List<Record> listResult = new ArrayList<Record>();
//        Calendar cal = Calendar.getInstance();
//        cal.setTime(new Date());
//        cal.add(Calendar.DATE,1);
//        long timeSt;
//        for (Record record : listRecord) {
//            timeSt = System.currentTimeMillis();
//            BounesMonthly bn = (BounesMonthly) record;
//            listResult.add(bn);
//
////           B0: Kiem tra neu da cong het roi thi xoa di 
//            if (bn.getCountProcess() <= 0) {
//                logger.warn("Already add 33 days bonues  ; isdn = " + bn.getIsdn() + "time :" + timeSt);
//                bn.setResultCode("E1");
//                bn.setDescription("Already add 33 days bonues  ; isdn = " + bn.getIsdn() + "time :" + timeSt);
//                bn.setDuration(System.currentTimeMillis() - timeSt);
//                db.deleteBounesMonthly(bn);
//                db.insertBounesMonthlyHis(bn);
//            }
//
////          B1 : kiem tra da thuc su duoc cong khuyen mai ngay hom nay chua  -- check maybe don't need becase config in file agent
//            if (db.checkCacheHandset(bn)) {
//                // da thuc su duoc cong khuyen mai ngay hom nay roi
//                logger.warn("today,Sim already add bounes  ; isdn = " + bn.getIsdn() + "time :" + timeSt);
//                continue;
//            };
//
//            //B2: Kiem tra xem may co cung voi sim khong
//            String cacheHandset = db.checkCacheHandset(bn, "258" + bn.getIsdn(), bn.getImei());
//            if (cacheHandset != null && cacheHandset.trim().length() > 0) {
//                //Sim van dang trong may nay de nhan thuong
//                db.updateBounesMonthly(bn);
//            } else {
//                logger.warn("Sim not attach with phone  " + bn.getIsdn());
//                bn.setResultCode("E2");
//                bn.setDescription(" Sim not attach with phone  ; isdn = " + bn.getIsdn() + "time :" + timeSt);
//                bn.setDuration(System.currentTimeMillis() - timeSt);
//                db.updateBounesMonthly(bn);
//                continue;
//            }
//
//            //B3: Cong KM data cho KH 300MB
//            String resultCode = "";
//            String description = "";
//            String resultChargeMoney = pro.addSmsDataVoice(bn.getIsdn(), "314572800", "5300", sdf2.format(cal.getTime()));
//            if (!"0".equals(resultChargeMoney)) {
//                logger.warn("Fail to charge money " + bn.getIsdn() + " errcode chargemoney " + resultChargeMoney);
//                resultCode = "E3";
//                description = "Fail to charge money " + bn.getIsdn() + " errcode chargemoney " + resultChargeMoney;
//            }
//
//            // cong sms
//            String resultSmsDataVoice = pro.addSmsDataVoice(bn.getIsdn(), "30", "5011", sdf2.format(cal.getTime()));
//            if (!"0".equals(resultSmsDataVoice)) {
//                logger.warn("Fail to add SmsData Voice " + bn.getIsdn() + " errcode resultSmsDataVoice " + resultSmsDataVoice);
//                resultCode = resultCode + "E4";
//                description = description + "|| Fail to add SmsData Voice " + bn.getIsdn() + " errcode resultSmsDataVoice " + resultSmsDataVoice;
//                bn.setResultCode(resultCode);
//                bn.setDescription(description);
//                bn.setDuration(System.currentTimeMillis() - timeSt);
//            }
//
//            db.sendSms(bn.getIsdn(), smsReceiveSecond.replace("%DAYS%", bn.getCountProcess().toString()), "86142");
//            int rsInsert = db.insertBounesMonthlyHis(bn);
//            if (rsInsert <= 0) {
//                logger.warn("Fail to insert BounesMonthlyHis " + bn.getIsdn() + " errcode rsInsert " + rsInsert);
//            }
//        }
//        listRecord.clear();
////        Thread.sleep(1000 * 60 * 5);
//        return listResult;
//    }
//
//    @Override
//    public void printListRecord(List<Record> listRecord) throws Exception {
//        StringBuilder br = new StringBuilder();
//        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
//        br.setLength(0);
//        br.append("\r\n").
//                append("|\tACTION_AUDIT_ID|").
//                append("|\tisdn\t|").
//                append("|\tcreate_time\t|").
//                append("|\tcreate_staff\t|").
//                append("|\temola_isdnF\t|").
//                append("|\tfirst_amount\t|").
//                append("|\tsecond_amount\t|").
//                append("|\tcount_process\t|").
//                append("|\tlast_process\t|");
//        for (Record record : listRecord) {
//            BounesMonthly bn = (BounesMonthly) record;
//            br.append("\r\n").
//                    append("|\t").
//                    append(bn.getIsdn()).
//                    append("||\t").
//                    append((bn.getCreateTime() != null ? sdf.format(bn.getCreateTime()) : null)).
//                    append("||\t").
//                    append(bn.getCreateStaff()).
//                    append("||\t").
//                    append(bn.geteMolaIsdn());
//        }
//        logger.info(br);
//    }
//
//    @Override
//    public List<Record> processException(List<Record> listRecord, Exception ex) {
//        return listRecord;
//    }
//
//    @Override
//    public boolean startProcessRecord() {
//        return true;
//    }
//}
