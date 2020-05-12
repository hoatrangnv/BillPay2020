/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbKitElitePrePaidProcessor;
import com.viettel.paybonus.obj.KitBatch;
import com.viettel.paybonus.obj.KitElitePrepaid;
import com.viettel.paybonus.obj.Offer;
import com.viettel.paybonus.obj.ProductConnectKit;
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
import java.util.HashMap;
import java.util.ResourceBundle;

/**
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class KitElitePrepaidProcess extends ProcessRecordAbstract {

    Exchange pro;
    DbKitElitePrePaidProcessor db;
    String eliteMsgRenewPrepaid;
    String lstIsdnReceiveError;
    String[] arrIsdnReceiveError;
    SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddHHmmss");
    SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

    public KitElitePrepaidProcess() {
        super();
        logger = Logger.getLogger(KitElitePrepaidProcess.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
        db = new DbKitElitePrePaidProcessor();
        eliteMsgRenewPrepaid = ResourceBundle.getBundle("configPayBonus").getString("eliteMsgRenewPrepaid");
        lstIsdnReceiveError = ResourceBundle.getBundle("configPayBonus").getString("lstIsdnReceiveError");
        arrIsdnReceiveError = lstIsdnReceiveError.split("\\|");
    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        for (Record record : listRecord) {
            KitElitePrepaid moRecord = (KitElitePrepaid) record;
            moRecord.setNodeName(holder.getNodeName());
            moRecord.setClusterName(holder.getClusterName());
        }
        return listRecord;
    }

    @Override
    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
        List<Record> listResult = new ArrayList<Record>();
        String staffCode;
        String productCode;
        long timeSt;
        Long actionAuditId;
        boolean isVasProduct, isBranchProduct, isEliteProduct, isDataSim;
        for (Record record : listRecord) {
            timeSt = System.currentTimeMillis();
            staffCode = "";
//            moneyProduct = "";
            actionAuditId = 0L;
            isVasProduct = false;
            isBranchProduct = false;
            isEliteProduct = false;
            isDataSim = false;
            KitElitePrepaid bn = (KitElitePrepaid) record;
            listResult.add(bn);
            if ("0".equals(bn.getResultCode())) {
                staffCode = bn.getCreateUser();
                bn.setActionAuditId(actionAuditId);
                if (staffCode == null || "".equals(staffCode)) {
                    logger.info("create_user in KIT_ELITE_PREPAID is null or empty, id " + bn.getId());
                    bn.setResultCode("01");
                    bn.setDescription("CreateStaff in KIT_ELITE_PREPAID is null or empty");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    continue;
                }
//                Step 1: Check prepaid by batch
                if (bn.getKitBatchId() != null && bn.getKitBatchId() > 0) {
                    logger.info("Renew by batch, kitBatchId: " + bn.getKitBatchId());
//                    Step 1.1: Get list detail...
                    List<KitBatch> lstKitBatchDetail = db.getListKitBatchDetail(bn.getKitBatchId());
                    if (lstKitBatchDetail.isEmpty()) {
                        logger.info("List KitBatchDetail is null or empty, id " + bn.getKitBatchId());
                        bn.setResultCode("02");
                        bn.setDescription("List KitBatchDetail is null or empty");
                        bn.setDuration(System.currentTimeMillis() - timeSt);
                        continue;
                    }
//                    Step 1.2: Convert KitBatch --> Single
                    for (KitBatch kitBatch : lstKitBatchDetail) {
                        if (kitBatch.getInputType() == 0 && (kitBatch.getProductAddOn() == null || kitBatch.getProductAddOn().isEmpty())) {
                            logger.info("Old subscriber, add_on is null or empty, no need insert kit_elite_prepaid, isdn: " + kitBatch.getIsdn());
                            continue;
                        }
                        String tempProductCode = "";
                        if (kitBatch.getProductAddOn() != null && !kitBatch.getProductAddOn().isEmpty()) {
                            tempProductCode = kitBatch.getProductAddOn();
                        } else {
                            tempProductCode = kitBatch.getProductCode();
                        }
                        bn.setProductCode(tempProductCode);
                        ProductConnectKit productPrepaid = db.getProductInfo(bn.getIsdn(), tempProductCode);
                        if (productPrepaid == null) {
                            logger.warn("Cannot get productInfo, id " + bn.getId() + ", productCode: " + tempProductCode);
                            continue;
                        }
                        int prepaidMonth = bn.getPrepaidMonth() - 1;
                        if (kitBatch.getInputType() != 1) {
                            logger.info("Old subscriber, so prepaid month = paidMonth: " + bn.getPrepaidMonth()
                                    + ",isdn: " + kitBatch.getIsdn() + ", kitBatchId: " + bn.getKitBatchId());
                            String tmpIsdn = kitBatch.getIsdn();
                            if (!tmpIsdn.startsWith("258")) {
                                tmpIsdn = "258" + tmpIsdn;
                            }
                            bn.setIsdn(tmpIsdn);
                            Long moId = db.getSequence("MO_SEQ", productPrepaid.getVasConnection());
                            if (db.checkBranchPromotionProduct(tempProductCode)) {
                                logger.info("Branch promotion product, start insert mo for buy more...isdn: " + bn.getIsdn());
                                String param = bn.getCreateUser() + "|0|4";
                                String actionType = "2";
                                db.insertMO(tmpIsdn, tempProductCode, productPrepaid.getVasConnection(), param, actionType, productPrepaid.getVasChannel(), moId);
                                bn.setMoId(moId);
                                bn.setResultCode("0");
                                bn.setDescription("Insert MO for seperate batch");
                                db.insertKitElitePrepaidHis(bn);
                                //check and insert branch promotion sub --> update expiretime ...
                                Date sysdate = new Date();
                                Calendar calExpireTime = Calendar.getInstance();
                                calExpireTime.setTime(sysdate);
                                calExpireTime.add(Calendar.DATE, 30);
                                if (db.checkPromotionSub(tmpIsdn)) {
                                    db.updateBranchPromotionSub(tmpIsdn, sdf2.format(calExpireTime.getTime()));
                                } else {
                                    db.insertBranchPromotionSub(bn.getId(), tmpIsdn, param.toUpperCase(),
                                            tempProductCode, sdf2.format(calExpireTime.getTime()));
                                }
                            } else if (db.checkVasProduct(tempProductCode)) {
                                logger.info("Vas product, start insert mo for register vas in seperate batch...isdn: " + bn.getIsdn());
                                String param = "pt|1|1|4";
                                db.insertMO(tmpIsdn, tempProductCode, productPrepaid.getVasConnection(), param, productPrepaid.getVasActionType(), productPrepaid.getVasChannel(), moId);
                                bn.setMoId(moId);
                                bn.setResultCode("0");
                                bn.setDescription("Insert MO for seperate batch");
                                db.insertKitElitePrepaidHis(bn);
                            }
                        }
                        if (db.checkKitElitePrepaid(kitBatch.getIsdn(), tempProductCode)) {
                            //Update prepaid
                            int result = db.updateKitElitePrepaid(prepaidMonth, kitBatch.getIsdn(), tempProductCode);
                            if (result != 1) {
                                String message = "KitElitePrepaid - Subscriber in batch already exists in KitElitePrepaid "
                                        + "-> update new prepaid month: " + bn.getPrepaidMonth() + ", isdn" + kitBatch.getIsdn() + ", kitBatchId: " + bn.getKitBatchId() + " fail.";
                                for (String isdn : arrIsdnReceiveError) {
                                    db.sendSms(isdn, message, "86904");
                                }
                            }
                        } else {
                            //Insert subscriber in batch to single
                            int result = db.insertKitElitePrepaid(kitBatch.getIsdn(), prepaidMonth, bn.getCreateUser(), tempProductCode);
                            if (result != 1) {
                                String message = "KitElitePrepaid - Convert subscriber in batch :" + kitBatch.getKitBatchId() + " to single mode is fail.";
                                for (String isdn : arrIsdnReceiveError) {
                                    db.sendSms(isdn, message, "86904");
                                }
                            }
                        }
                    }
//                    Step 1.3: Update status of batch = 0
                    int result = db.disablePrepaidBatch(bn.getId());
                    if (result != 1) {
                        String message = "KitElitePrepaid - disablePrepaidBatch, kitElitePrepaidId :" + bn.getId() + " is fail. Disable by hand.";
                        for (String isdn : arrIsdnReceiveError) {
                            db.sendSms(isdn, message, "86904");
                        }
                    }
                    logger.info("Finish convert batch to single, id " + bn.getKitBatchId());
                    bn.setResultCode("03");
                    bn.setDescription("Finish convert batch to single");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    continue;
                } else {
                    if (db.countSubPrepaid(bn.getIsdn(), bn.getProductCode()) > 1) {
                        int resultDisable = db.disablePrepaidBatch(bn.getId());
                        if (resultDisable != 1) {
                            String message = "KitElitePrepaid - disablePrepaidSubscriber, kitElitePrepaidId :" + bn.getId() + " is fail. Disable by hand.";
                            for (String isdn : arrIsdnReceiveError) {
                                db.sendSms(isdn, message, "86904");
                            }
                        }
                        //Update prepaid
                        int result = db.updateKitElitePrepaid(bn.getPrepaidMonth(), bn.getIsdn(), bn.getProductCode());
                        if (result != 1) {
                            String message = "KitElitePrepaid - Subscriber already exists in KitElitePrepaid "
                                    + "-> update new prepaid month: " + bn.getPrepaidMonth() + ", isdn" + bn.getIsdn() + ", kitBatchId: " + bn.getKitBatchId() + " fail.";
                            for (String isdn : arrIsdnReceiveError) {
                                db.sendSms(isdn, message, "86904");
                            }
                        }
                        logger.info("Finish merge isdn prepaid duplicate, id " + bn.getKitBatchId());
                        bn.setResultCode("04");
                        bn.setDescription("Finish merge isdn prepaid duplicate");
                        bn.setDuration(System.currentTimeMillis() - timeSt);
                        continue;
                    }
                    String tmpIsdn = bn.getIsdn();
                    if (!tmpIsdn.startsWith("258")) {
                        tmpIsdn = "258" + bn.getIsdn();
                    }
                    logger.info("Start process prepaid for single sub, isdn: " + bn.getIsdn() + ", productCode: " + bn.getProductCode());
                    productCode = bn.getProductCode();
                    if (productCode == null || "".equals(productCode)) {
                        logger.warn("productCode in KIT_ELITE_PREPAID is null or empty, id " + bn.getId());
                        bn.setResultCode("05");
                        bn.setDescription("CreateStaff in KIT_ELITE_PREPAID is null or empty");
                        bn.setDuration(System.currentTimeMillis() - timeSt);
                        continue;
                    }
                    bn.setProductCode(productCode);
                    if (db.checkBranchPromotionProduct(productCode)) {
                        isBranchProduct = true;
                    } else if (db.checkVasProduct(productCode)) {
                        isVasProduct = true;
                    } else if (db.checkEliteProduct(productCode)) {
                        isEliteProduct = true;
                    } else if ("DATA_SIM".equals(productCode)) {
                        isDataSim = true;
                    }
                    if (!isBranchProduct && !isVasProduct && !isEliteProduct && !isDataSim) {
                        logger.warn("Product code is invalid, id " + bn.getId() + ", productCode: " + productCode);
                        bn.setResultCode("06");
                        bn.setDescription("productCode not in branch promotion or vas or elite");
                        bn.setDuration(System.currentTimeMillis() - timeSt);
                        continue;
                    }
                    ProductConnectKit productPrepaid = db.getProductInfo(bn.getIsdn(), productCode);
                    if (productPrepaid == null) {
                        logger.warn("Cannot get productInfo, id " + bn.getId() + ", productCode: " + productCode);
                        bn.setResultCode("07");
                        bn.setDescription("productCode is not Elite");
                        bn.setDuration(System.currentTimeMillis() - timeSt);
                        continue;
                    }
                    bn.setMoneyProduct(productPrepaid.getMoneyFee());
                    String subMbInfo = db.getSubMbInfo(bn.getIsdn());
                    if (subMbInfo.isEmpty()) {
                        logger.warn("Cannot get subscriber info, isdn " + bn.getIsdn());
                        bn.setResultCode("08");
                        bn.setDescription("Cannot get subscriber info");
                        bn.setDuration(System.currentTimeMillis() - timeSt);
                        continue;
                    }
                    String[] arrSubInfo = subMbInfo.split("\\|");
                    Long subId = Long.valueOf(arrSubInfo[0]);
                    String currentProductCode = arrSubInfo[1];
                    if (!currentProductCode.toUpperCase().equals(productCode.toUpperCase()) && isEliteProduct) {
                        ProductConnectKit productInfo = db.getProductInfo(bn.getIsdn(), currentProductCode);
                        if (productInfo == null) {
                            this.logger.warn("productCode is not Elite, id " + bn.getId());
                            bn.setResultCode("09");
                            bn.setDescription("productCode is not Elite");
                            bn.setDuration(Long.valueOf(System.currentTimeMillis() - timeSt));
                            continue;
                        }
                        logger.info("productCode prepaid difference currentProduct --> Add money return for customer");
                        String expireTime = "";
                        HashMap<String, String> lstParams = new HashMap<String, String>();
                        lstParams.put("MSISDN", tmpIsdn);
                        String original = pro.getOriginalOfCommand(bn.getIsdn(), "OCSHW_INTEGRATIONENQUIRY", lstParams);
                        if (!original.isEmpty()) {
                            List<Offer> listOffer = pro.parseListOffer(original);
                            if (listOffer != null && !listOffer.isEmpty()) {
                                for (Offer offer : listOffer) {
                                    if (productInfo.getPpBuyMore() != null && productInfo.getPpBuyMore().equals(offer.getId())) {
                                        Calendar cal = Calendar.getInstance();
                                        cal.setTime(sdf1.parse(offer.getRecurringDate()));
                                        cal.add(Calendar.DATE, 30);
                                        expireTime = sdf2.format(cal.getTime());
                                        logger.info("ExpireDate of isdn: " + bn.getIsdn() + ", expire: " + expireTime);
                                        break;
                                    }
                                }
                            } else {
                                logger.warn("List Offer of subscriber is empty, isdn: " + bn.getIsdn());
                            }

                        } else {
                            logger.warn("Cannot get original of subscriber " + bn.getIsdn());
                            bn.setResultCode("10");
                            bn.setDescription("Cannot get original of subscriber on vOCS");
                            bn.setDuration(System.currentTimeMillis() - timeSt);
                            continue;
                        }
                        boolean isRenew = false;
                        if (!expireTime.isEmpty()) {
                            Date sysdate = new Date();
                            Date dateExpire = sdf2.parse(expireTime);
                            if (dateExpire.before(sysdate)) {
                                logger.info("Already expire, need to renew for subscriber: " + bn.getIsdn() + ", expire: " + expireTime);
                                isRenew = true;
                            }
                        }
                        if (!isRenew && expireTime.isEmpty()) {
                            isRenew = true;
                            logger.info("Don't have expireTime on OCS -> Already expire  --> need to renew: " + bn.getIsdn() + ", expire: " + expireTime);
                        }
                        if (isRenew) {
                            logger.info("begin call topupPrePaid, isdn: " + bn.getIsdn()
                                    + ", moneyProduct: " + productPrepaid.getMoneyFee());
                            String rs = pro.addMoney(bn.getIsdn(), productPrepaid.getMoneyFee(), "1");
                            bn.setResultTopup(rs);
                            if ("0".equals(rs)) {
                                logger.info("Add money for renew product: " + productCode + " successfully, isdn: " + bn.getIsdn());
                                db.sendSms(bn.getIsdn(), eliteMsgRenewPrepaid.replace("XXXX", productPrepaid.getMoneyFee()), "86904");
                                bn.setDescription("addMoney for renew successfully.");
                                bn.setDuration(System.currentTimeMillis() - timeSt);
                                actionAuditId = db.getSequence("SEQ_ACTION_AUDIT", "cm_pre");
                                bn.setActionAuditId(actionAuditId);
                                db.insertActionAudit(actionAuditId, subId, "Add money for renew eLite subscriber.");
                                bn.setResultCode("0");
                                bn.setDuration(Long.valueOf(System.currentTimeMillis() - timeSt));
                                db.updateKitElitePrepaid(bn.getId());
                                continue;
                            } else {
                                bn.setResultCode("11");
                                bn.setDescription("addMoney for renew unsuccessfully.");
                                bn.setDuration(System.currentTimeMillis() - timeSt);
                                logger.info("Add money for renew product: " + productCode + " unsuccessfully, isdn: " + bn.getIsdn());
                                continue;
                            }
                        } else {
                            logger.warn("No need topup for subscriber " + bn.getIsdn());
                            bn.setResultCode("12");
                            bn.setDescription("No need topup for subscriber");
                            bn.setDuration(System.currentTimeMillis() - timeSt);
                            continue;
                        }

                    }

                    String expireTime = "";
                    if (isEliteProduct || isVasProduct) {
                        String pricePlan = "";
                        if (isEliteProduct) {
                            pricePlan = productPrepaid.getPpBuyMore();
                        } else {
                            pricePlan = productPrepaid.getVasPricePlain();
                        }
                        HashMap<String, String> lstParams = new HashMap<String, String>();
                        lstParams.put("MSISDN", tmpIsdn);
                        String original = pro.getOriginalOfCommand(bn.getIsdn(), "OCSHW_INTEGRATIONENQUIRY", lstParams);
                        if (!original.isEmpty()) {
                            List<Offer> listOffer = pro.parseListOffer(original);
                            if (listOffer != null && !listOffer.isEmpty()) {
                                for (Offer offer : listOffer) {
                                    if (pricePlan.equals(offer.getId())) {
                                        Calendar cal = Calendar.getInstance();
                                        cal.setTime(sdf1.parse(offer.getRecurringDate()));
                                        cal.add(Calendar.DATE, 30);
                                        expireTime = sdf2.format(cal.getTime());
                                        logger.info("ExpireDate of isdn: " + bn.getIsdn() + ", expire: " + expireTime + ", product: " + productCode);
                                        break;
                                    }
                                }
                            } else {
                                logger.warn("List Offer of subscriber is empty, isdn: " + bn.getIsdn());
                            }

                        } else {
                            logger.warn("Cannot get original of subscriber " + bn.getIsdn());
                            bn.setResultCode("13");
                            bn.setDescription("Cannot get original of subscriber on vOCS");
                            bn.setDuration(System.currentTimeMillis() - timeSt);
                            continue;
                        }
                    } else if (isDataSim) {
                        Date sysdate = new Date();
                        Calendar calendar = Calendar.getInstance();
                        calendar.setTime(sysdate);
                        calendar.add(Calendar.DATE, -30);
                        logger.info("sysdate-30 = " + sdf2.format(calendar.getTime()) + ", createTime: " + bn.getCreateTime().toString());
                        if (calendar.getTime().after(bn.getCreateTime())) {
                            logger.info("sysdate - 30 > create_time, already expire, start add 4GB for subscriber...:" + bn.getIsdn());
                            calendar.setTime(sysdate);
                            calendar.add(Calendar.DATE, 30);
                            pro.addSmsDataVoice(bn.getIsdn(), "4294967296", "4300", sdf2.format(calendar.getTime()));
                            db.updateCreateTimeKitElitePrepaid(bn.getIsdn(), productCode);
                            db.updateKitElitePrepaid(bn.getId());
                            db.insertActionAudit(actionAuditId, subId, "Add price plan buymore for subscriber prepaid.");
                            bn.setResultCode("0");
                            bn.setDescription("Add policy success for data sim");
                            bn.setDuration(System.currentTimeMillis() - timeSt);
                        } else {
                            logger.info("sysdate - 30 < create_time, not yet expire, no need add 4GB, isdn: " + bn.getIsdn());
                            bn.setResultCode("16");
                            bn.setDescription("No need add policy for data sim");
                            bn.setDuration(System.currentTimeMillis() - timeSt);
                        }

                        continue;
                    } else {
                        logger.info("Start getExpireTime of BranchPromotion product: " + productCode + ", isdn: " + bn.getIsdn());
                        expireTime = db.getExpireTimeBranchPromotionProduct(tmpIsdn);
                    }

                    boolean isRenew = false;
                    if (!expireTime.isEmpty()) {
                        Date sysdate = new Date();
                        Date dateExpire = sdf2.parse(expireTime);
                        if (dateExpire.before(sysdate)) {
                            logger.info("Already expire, need to renew for subscriber: " + bn.getIsdn() + ", expire: " + expireTime);
                            isRenew = true;
                        }
                    }
                    if (!isRenew && expireTime.isEmpty()) {
                        isRenew = true;
                        logger.info("Don't have expireTime on OCS -> Already expire  --> need to renew: " + bn.getIsdn() + ", expire: " + expireTime);
                    }
                    if (isRenew) {
                        if (isEliteProduct || isVasProduct) {
                            String pricePlan = "";
                            if (isEliteProduct) {
                                pricePlan = productPrepaid.getPpBuyMore();
                            } else {
                                pricePlan = productPrepaid.getVasPricePlain();
                            }
                            logger.info("Start add price plan buymore for subscriber to renew: " + bn.getIsdn() + ", expire: " + expireTime);
                            String resultAddPrice = pro.addPrice(bn.getIsdn(), pricePlan, "", "30");
                            if (!"0".equals(resultAddPrice)) {
                                logger.warn("Fail to add priceplan " + bn.getIsdn() + " errcode " + resultAddPrice + " now rollback money");
                                bn.setResultCode("14");
                                bn.setDescription("Fail to add priceplan, must be rollback money ");
                                bn.setDuration(System.currentTimeMillis() - timeSt);
                                continue;
                            }
                        } else {
                            logger.info("Branch promotion product, start insert mo for buy more...isdn: " + bn.getIsdn());
                            productPrepaid.setVasParam(bn.getCreateUser() + "|0|4");
                            productPrepaid.setVasActionType("2");
                            Long moId = db.getSequence("MO_SEQ", productPrepaid.getVasConnection());
                            db.insertMO(tmpIsdn, productCode, productPrepaid.getVasConnection(), productPrepaid.getVasParam(), productPrepaid.getVasActionType(), productPrepaid.getVasChannel(), moId);
                            bn.setMoId(moId);
                            //check and insert branch promotion sub --> update expiretime ...
                            Date sysdate = new Date();
                            Calendar calExpireTime = Calendar.getInstance();
                            calExpireTime.setTime(sysdate);
                            calExpireTime.add(Calendar.DATE, 30);
                            if (db.checkPromotionSub(tmpIsdn)) {
                                db.updateBranchPromotionSub(tmpIsdn, sdf2.format(calExpireTime.getTime()));
                            } else {
                                db.insertBranchPromotionSub(bn.getId(), tmpIsdn, productPrepaid.getVasParam().toUpperCase(),
                                        productCode, sdf2.format(calExpireTime.getTime()));
                            }
                        }

                    } else {
                        logger.warn("No need add price plan buy more for subscriber " + bn.getIsdn());
                        bn.setResultCode("15");
                        bn.setDescription("No need add price plan buy more for subscriber");
                        bn.setDuration(System.currentTimeMillis() - timeSt);
                        continue;
                    }
                    bn.setResultCode("0");
                    bn.setDescription("Add price plan buy more successfully, isdn: " + bn.getIsdn());
                    bn.setDuration(Long.valueOf(System.currentTimeMillis() - timeSt));
                    actionAuditId = db.getSequence("SEQ_ACTION_AUDIT", "cm_pre");
                    bn.setActionAuditId(actionAuditId);
                    db.insertActionAudit(actionAuditId, subId, "Add price plan buymore for subscriber prepaid.");
                    db.updateKitElitePrepaid(bn.getId());
                    if (!isBranchProduct) {
                        Long moId = db.getSequence("MO_SEQ", productPrepaid.getVasConnection());
                        db.insertMoHis(productPrepaid.getVasConnection(), tmpIsdn, subId, productCode, productPrepaid.getVasParam(),
                                Long.valueOf(productPrepaid.getVasActionType()), Long.valueOf(productPrepaid.getMoneyFee()), moId);
                    }
                    db.sendSms(bn.getIsdn(), productPrepaid.getSmsPrepaid(), "86904");
                }


            } else {
                logger.warn("After validate respone code is fail "
                        + " id " + bn.getId() + " staffCode: " + bn.getCreateUser()
                        + " so continue with other transaction");
                bn.setDescription("After validate respone code is fail");
                bn.setDuration(System.currentTimeMillis() - timeSt);
                continue;
            }
        }
        listRecord.clear();
        return listResult;
    }

    @Override
    public void printListRecord(List<Record> listRecord) throws Exception {
        StringBuilder br = new StringBuilder();
        SimpleDateFormat sdf1 = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        br.setLength(0);
        br.append("\r\n").
                append("|\tID|").
                append("|\tISDN|").
                append("|\tCREATE_TIME|").
                append("|\tPREPAID_MONTH\t|").
                append("|\tREMAIN_MONTH\t|").
                append("|\tPREPAID_TYPE\t|").
                append("|\tKIT_BATCH_ID\t|").
                append("|\tSTATUS\t|").
                append("|\tREMAIN_TIME\t|").
                append("|\tPRODUCT_CODE\t|").
                append("|\tCREATE_USER\t|");
        for (Record record : listRecord) {
            KitElitePrepaid bn = (KitElitePrepaid) record;
            br.append("\r\n").
                    append("|\t").
                    append(bn.getId()).
                    append("||\t").
                    append(bn.getIsdn()).
                    append("||\t").
                    append(bn.getCreateTime() != null ? sdf1.format(bn.getCreateTime()) : null).
                    append("||\t").
                    append(bn.getPrepaidMonth()).
                    append("||\t").
                    append(bn.getRemainMonth()).
                    append("||\t").
                    append((bn.getPrepaidType())).
                    append("||\t").
                    append(bn.getKitBatchId() != null ? bn.getKitBatchId() : "").
                    append("||\t").
                    append(bn.getStatus()).
                    append("||\t").
                    append(bn.getRemainTime() != null ? sdf1.format(bn.getRemainTime()) : null).
                    append("||\t").
                    append(bn.getProductCode()).
                    append("||\t").
                    append(bn.getCreateUser());
        }
        logger.info(br);
    }

    @Override
    public List<Record> processException(List<Record> listRecord, Exception ex) {
        logger.info("processException...");
        return listRecord;
    }

    @Override
    public boolean startProcessRecord() {
        return true;
    }
}
