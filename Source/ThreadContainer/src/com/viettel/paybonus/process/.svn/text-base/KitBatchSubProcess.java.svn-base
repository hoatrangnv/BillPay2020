///*
// * To change this template, choose Tools | Templates
// * and open the template in the editor.
// */
//package com.viettel.paybonus.process;
//
//import com.viettel.cluster.agent.integration.Record;
//import com.viettel.paybonus.database.DbKitBatchSubProcessor;
//import com.viettel.paybonus.obj.KitBatchSub;
//import com.viettel.paybonus.service.Exchange;
//import com.viettel.threadfw.manager.AppManager;
//import java.util.List;
//import org.apache.log4j.Logger;
//import com.viettel.threadfw.process.ProcessRecordAbstract;
//import com.viettel.vas.util.ExchangeClientChannel;
//import java.text.SimpleDateFormat;
//import java.util.ArrayList;
//import java.util.Date;
//
///**
// *
// * @author HuyNQ13
// * @version 1.0
// * @since 24-03-2016
// */
//public class KitBatchSubProcess extends ProcessRecordAbstract {
//
//    Exchange pro;
//    DbKitBatchSubProcessor db;
//    public KitBatchSubProcess() {
//        super();
//        logger = Logger.getLogger(KitBatchSubProcess.class);
//    }
//
//    @Override
//    public void initBeforeStart() throws Exception {
//        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
//        db = new DbKitBatchSubProcessor();
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
//        long shopId = 1000909;
//        long staffId = 1299172;
//        double amount = 1000;
//        long reasonId = 3488;
//        int channelTypeId = 14;
//        String staffCode = "MV_SOF_BOD_HIEUTT";
//        long bonusValue = 400;
//        String isdnWalletHieuNT = "870023630";
//        boolean sendSMSCharityFloods = Boolean.TRUE;
//        boolean saveActionAudit = Boolean.FALSE;
//        for (Record record : listRecord) {
//            KitBatchSub bn = (KitBatchSub) record;
//            listResult.add(bn);
//            bn.setDescription("");
//            if (isNullOrEmpty(bn.getIsdn())) {
//                logger.info("The input value invalid, isdn " + bn.getIsdn());
//                bn.setDescription("The input value invalid, isdn is null or empty");
//                bn.setResultCode("E05");
//                continue;
//            }
//            String msisdn = bn.getIsdn().startsWith("258") ? bn.getIsdn() : "258" + bn.getIsdn();
//            //Create transaction
//            if (db.checkProcessedStatus(msisdn)) {
//                //Already processed
//                logger.info("The number already processed." + bn.getIsdn() + ",product code:" + bn.getProductCode());
//                bn.setResultCode("E01");
//                bn.setDescription("The number already processed");
//                continue;
//            }
//
//            if (db.checkCreateTransactionAndPayBonus(msisdn)) {
//                sendSMSCharityFloods = Boolean.FALSE;
//                //Creat tran saction
//                logger.info("Create transaction for this numnber." + bn.getIsdn() + ",product code:" + bn.getProductCode());
//                //bn.setDescription(bn.getDescription() + ";Create transaction for this numnber");
//
//                long saleSericeId = 26005783;
//                long saleServicePrice = 20085454;
//
//                Long saleTransId = db.getSequence("SALE_TRANS_SEQ", "dbsm");
//
//                Long subId = db.getSubID(msisdn);
//                if (isNullOrEmpty(subId)) {
//                    logger.info("Cannot get isdn info." + bn.getIsdn() + ",product code:" + bn.getProductCode());
//                    bn.setResultCode("E02");
//                    bn.setDescription(bn.getDescription() + ";Cannot get isdn info");
//                    continue;
//                }
//                int resInsertST = db.insertSaleTrans(saleTransId, shopId, staffId, saleSericeId, saleServicePrice, amount, subId, msisdn.substring(3), reasonId);
//                if (resInsertST == 1) {
//                    bn.setDescription(bn.getDescription() + ";Create sale trans successfully");
//                    //insert sale trans detail
//                    Long saleTransDetailId1 = db.getSequence("SALE_TRANS_DETAIL_SEQ", "dbsm");
//                    int resInsertSTD1 = db.insertSaleTransDetail(saleTransDetailId1, saleTransId, null, null, saleSericeId, saleServicePrice + "", null, null, null, null, "CN_DIAMOND_PRE", "Connect Diamond_Pre", "CN_DIAMOND_PRE", "Connect Diamond_Pre", "17", null, null, String.valueOf(amount), amount, 0.0);
//                    logger.info("Create sale trans detail 1: " + bn.getIsdn() + "result : " + resInsertSTD1 + ", amount :" + amount);
//                    Long saleTransDetailId2 = db.getSequence("SALE_TRANS_DETAIL_SEQ", "dbsm");
//                    int resInsertSTD2 = db.insertSaleTransDetail(saleTransDetailId2, saleTransId, "202008", "507453", saleSericeId, null, "1", "Mobile Number", "MN", "Isdn free mobile", null, "Connect Diamond_Pre", "MN", "MN", null, "17", null, "0", 0.0, 0.0);
//                    if (resInsertSTD2 < 1) {
//                        logger.info("Failed to create sale trans detail." + bn.getIsdn() + ",product code:" + bn.getProductCode());
//                        bn.setResultCode("E04");
//                        bn.setDescription(bn.getDescription() + ";Failed to create sale trans detail");
//                        continue;
//                    } else {
//                        Long saleTransSerialId = db.getSequence("SALE_TRANS_SERIAL_SEQ", "dbsm");
//                        int resInsertSTS = db.insertSaleTransSerial(saleTransSerialId, saleTransDetailId2, 202008L, msisdn.substring(3));
//                        if (resInsertSTS < 1) {
//                            bn.setDescription(bn.getDescription() + ";Create sale trans serial failed ");
//                        } else {
//                            bn.setDescription(bn.getDescription() + ";Create sale trans serial successfully ");
//                        }
//                    }
//
//                } else {
//                    logger.info("Failed to create sale trans" + bn.getIsdn() + ",product code:" + bn.getProductCode());
//                    bn.setDescription(bn.getDescription() + ";Failed to create sale trans");
//                    bn.setResultCode("E03");
//                }
//
//                //Start Paybonus
//                Long[] actionInfos = db.getActionInfo(msisdn);
//                long actionAuditId = 0;
//                String actionCode = "";
//
//                if (isNullOrEmpty(actionInfos) || isNullOrEmpty(actionInfos[0]) || isNullOrEmpty(actionInfos[1])) {
//                    actionAuditId = db.getActionAuditIdSeq();
//                    actionCode = "615";
//                    saveActionAudit = Boolean.TRUE;
//                } else {
//                    actionAuditId = actionInfos[0];
//                    actionCode = String.valueOf(actionInfos[1]);
//                }
//
//                if (actionAuditId == 0) {
//                    logger.info("Cannot get action audit id " + bn.getIsdn() + ",product code:" + bn.getProductCode());
//                    bn.setDescription(bn.getDescription() + ";Cannot get action audit id");
//                    bn.setResultCode("E07");
//                    continue;
//                }
//                SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
//                String eWalletResponse = pro.callEwallet(actionAuditId, channelTypeId, isdnWalletHieuNT, bonusValue,
//                        actionCode + "", staffCode, sdf.format(new Date()), db);
//                if ("01".equals(eWalletResponse)) {
//                    logger.info("Pay Bonus success for actionId " + bn.getActionAuditId() + " isdnEmola "
//                            + msisdn + " amount " + bonusValue);
//                    bn.setResultCode("0");
//                    bn.setDescription(bn.getDescription() + ";Bonus success:" + bonusValue);
//                    db.sendSms(isdnWalletHieuNT, "[Connect kit batch]Bonus successfull for the isdn " + msisdn, "86904");
//                } else {
//                    logger.error("Pay Bonus fail for actionId " + actionInfos[0]
//                            + " isdnEmola " + isdnWalletHieuNT + " amount " + bonusValue);
//                    bn.setResultCode("E08");
//                    bn.setDescription(bn.getDescription() + ";Pay Bonus failed:" + eWalletResponse);
//                }
//                if (saveActionAudit) {
//                    db.insertActionAudit(actionAuditId, actionCode, reasonId, subId, shopId + "", staffCode, "PayCommission DIAMOND");
//                }
//                //End Paybonus
//            } else {
//                logger.info("NOT Create transaction and paybonus for this numnber." + bn.getIsdn() + ",product code:" + bn.getProductCode());
//                bn.setDescription(bn.getDescription() + ";Don't create transaction and paybonus for this numnber");
//                bn.setResultCode("1");
//            }
//
//            //Send SMS charity
//            SimpleDateFormat sf = new SimpleDateFormat("ddMMyyyyHHmmss");
//            String fromDateStr = "26032019000000";
//            String toDateStr = "26042019235959";
//
//            Date fromDate = sf.parse(fromDateStr);
//            Date toDate = sf.parse(toDateStr);
//            Date currDate = sf.parse(bn.getReceiveDate());
//
//            if (currDate.compareTo(fromDate) >= 0 && currDate.compareTo(toDate) <= 0 && sendSMSCharityFloods) {
//                db.sendSms(msisdn, "Obrigado por participar do Programa da Movitel de Apoio as Vitimas do Ciclone IDAI. A sua doacao: 50 MT. Para detalhes acesse o nosso Website ou Pagina Facebook", "86904");
//                bn.setDescription(bn.getDescription() + ";Send SMS charity floods");
//                bn.setCharity(1);
//            } else {
//                bn.setDescription(bn.getDescription() + ";Dont send SMS charity floods");
//                bn.setCharity(0);
//            }
//        }
//        listRecord.clear();
//        return listResult;
//    }
//
//    /**
//     * Convert from Megabytes to Bytes
//     *
//     * @param mbDataStr
//     * @return
//     */
//    public long getPromotionMoney(String moneyStr) {
//        long mbData = 0;
//        try {
//            if (moneyStr != null && moneyStr.trim().length() > 0) {
//                mbData = Long.valueOf(moneyStr.trim()) * 100;
//            }
//        } catch (Exception ex) {
//            logger.error("getPromotionMoney ERROR : " + ex.getMessage());
//            return 0;
//        }
//        return mbData;
//    }
//
//    /**
//     * Get valid date when add promotion
//     *
//     * @param mbDateStr
//     * @return
//     */
//    public int getValidDays(String mbDateStr) {
//        int valDate = 0;
//        try {
//            if (mbDateStr != null && mbDateStr.trim().length() > 0) {
//                valDate = Integer.valueOf(mbDateStr.trim());
//                return valDate;
//            }
//        } catch (Exception ex) {
//            logger.error("getValidDays ERROR: " + ex.getMessage());
//            return 0;
//        }
//        return valDate;
//    }
//
//    @Override
//    public void printListRecord(List<Record> listRecord) throws Exception {
//        StringBuilder br = new StringBuilder();
//        br.setLength(0);
//        br.append("\r\n").
//                append("|\tSERIAL|").
//                append("|\tisdn\t|").
//                append("|\tPRODUCT_CODE\t|").
//                append("|\tPRODUCT_FEE\t|");
//        for (Record record : listRecord) {
//            KitBatchSub bn = (KitBatchSub) record;
//            br.append("\r\n").
//                    append("|\t").
//                    append(bn.getSerial()).
//                    append("||\t").
//                    append(bn.getIsdn()).
//                    append("||\t").
//                    append(bn.getProductCode()).
//                    append("||\t").
//                    append(bn.getMoneyProduct()).
//                    append("||\t");
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
//
//    public boolean isNullOrEmpty(Object ob) {
//        if (ob != null && ob.toString().trim().length() > 0) {
//            return Boolean.FALSE;
//        }
//        return Boolean.TRUE;
//    }
//}
