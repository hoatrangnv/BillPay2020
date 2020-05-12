///*
// * To change this template, choose Tools | Templates
// * and open the template in the editor.
// */
//package com.viettel.paybonus.process;
//
//import com.viettel.cluster.agent.integration.Record;
//import com.viettel.paybonus.database.DbKitVipRenew;
//import com.viettel.paybonus.obj.AccountInfo;
//import com.viettel.paybonus.obj.KitVas;
//import com.viettel.paybonus.service.Exchange;
//import com.viettel.paybonus.service.Service;
//import com.viettel.threadfw.manager.AppManager;
//import java.util.List;
//import org.apache.log4j.Logger;
//import com.viettel.threadfw.process.ProcessRecordAbstract;
//import com.viettel.vas.util.ExchangeClientChannel;
//import java.text.SimpleDateFormat;
//import java.util.ArrayList;
//import java.util.ResourceBundle;
//
///**
// *
// * @author HuyNQ13
// * @version 1.0
// * @since 24-03-2016
// */
//public class KitVipRenew extends ProcessRecordAbstract {
//
//    Exchange pro;
//    Service services;
//    DbKitVipRenew db;
//    String pricePlanVipProduct;
//    String[] arrPricePlanVipProduct;
//    String priceplanA;
//    String priceplanB;
//    String expireTimeA;
//    String effectiveTimeA;
//    String expireTimeB;
//    String effectiveTimeB;
//    SimpleDateFormat sdf;
//    String urlRenewMCA;
//
//    //LinhNBV modified on September 04 2017: Add variables for message
//    public KitVipRenew() {
//        super();
//        logger = Logger.getLogger(KitVipRenew.class);
//    }
//
//    @Override
//    public void initBeforeStart() throws Exception {
//        services = new Service(ExchangeClientChannel.getInstance("../etc/service_client.cfg").getInstanceChannel(), logger);
//        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
//        db = new DbKitVipRenew();
//        pricePlanVipProduct = ResourceBundle.getBundle("configPayBonus").getString("pricePlanVipProduct");
//        arrPricePlanVipProduct = pricePlanVipProduct.split("\\|");
//        sdf = new SimpleDateFormat("yyyyMMddHHmmss");
//        urlRenewMCA = ResourceBundle.getBundle("configPayBonus").getString("urlRenewMCA");
//    }
//
//    @Override
//    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
//        for (Record record : listRecord) {
//            KitVas moRecord = (KitVas) record;
//            moRecord.setNodeName(holder.getNodeName());
//            moRecord.setClusterName(holder.getClusterName());
//        }
//        return listRecord;
//    }
//
//    @Override
//    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
//        List<Record> listResult = new ArrayList<Record>();
//        for (Record record : listRecord) {
//            KitVas bn = (KitVas) record;
//            listResult.add(bn);
//            if ("0".equals(bn.getResultCode())) {
//                for (String tempPricePlan : arrPricePlanVipProduct) {
//                    if (tempPricePlan.contains(bn.getProductCode())) {
//                        String[] arrTempPricePlan = tempPricePlan.split("\\&");
//                        priceplanA = arrTempPricePlan[1];
//                        priceplanB = arrTempPricePlan[2];
//                        break;
//                    }
//                }
////                Step 1:  View account info
//                AccountInfo accInfo = pro.viewAccInfo("258" + bn.getIsdn());
//                expireTimeA = accInfo.getProductExpires().get(priceplanA);
//                effectiveTimeA = accInfo.getProductEffectives().get(priceplanA);
//                expireTimeB = accInfo.getProductExpires().get(priceplanB);
//                effectiveTimeB = accInfo.getProductEffectives().get(priceplanB);
//                if (expireTimeB != null) {
////                        Step 3.1: Remove price plan A
//                    String resultRemoveA = pro.removePrice("258" + bn.getIsdn(), priceplanA);
//                    logger.info("Remove PricePlan A" + priceplanA
//                            + " sub " + bn.getIsdn() + " result " + resultRemoveA);
//
//                    if (!"0".equals(resultRemoveA) & !"102010227".equals(resultRemoveA)) {
//                        bn.setResultCode(resultRemoveA);
//                        bn.setDescription("Remove price plan A not success.");
//                        continue;
//                    }
//                    String resultAddPriceA = pro.addPriceV2("258" + bn.getIsdn(), priceplanA, expireTimeB, "20370101000000");
//                    logger.info("Add again PricePlan A" + priceplanA
//                            + " sub " + bn.getIsdn() + " result " + resultAddPriceA);
//                    if (!"0".equals(resultAddPriceA)) {
//                        bn.setResultCode(resultAddPriceA);
//                        bn.setDescription("Add price plan A not success.");
//                        continue;
//                    }
////                        Step 3.3: Renew MCA
//                    String renewMCA = services.renewMCA("258" + bn.getIsdn(), urlRenewMCA);
//                    logger.info("Renew MCA: "
//                            + " sub " + bn.getIsdn() + " result " + renewMCA);
////                        Step 3.4: Renew CRBT (API not yet).
//
////                    Step 3.5: Update last_extend_date
//                    db.updateLastExtendDate(bn.getIsdn());
//                } else {
//                    bn.setResultCode("04");
//                    bn.setDescription("ExpireTime B is null.");
//                    continue;
//                }
//            } else {
//                logger.warn("After validate respone code is fail kit_vas_id " + bn.getKitVasId()
//                        + " so continue with other transaction");
//                continue;
//            }            
//        }
//        listRecord.clear();
//        Thread.sleep(1000 * 60 * 60 * 6);
//        return listResult;
//    }
//
//    @Override
//    public void printListRecord(List<Record> listRecord) throws Exception {
//        StringBuilder br = new StringBuilder();
//        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
//        br.setLength(0);
//        br.append("\r\n").
//                append("|\tKIT_VAS_ID|").
//                append("|\tISDN|").
//                append("|\tSERIAL\t|").
//                append("|\tCREATE_USER\t|").
//                append("|\tPRODUCT_CODE\t|");
//        for (Record record : listRecord) {
//            KitVas bn = (KitVas) record;
//            br.append("\r\n").
//                    append("|\t").
//                    append(bn.getKitVasId()).
//                    append("||\t").
//                    append(bn.getIsdn()).
//                    append("||\t").
//                    append(bn.getSerial()).
//                    append("||\t").
//                    append(bn.getCreateUser()).
//                    append("||\t").
//                    append(bn.getProductCode());
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
