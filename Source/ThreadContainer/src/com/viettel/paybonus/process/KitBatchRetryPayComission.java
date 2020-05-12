/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbKitBatchRetryPayComission;
import com.viettel.paybonus.obj.Comission;
import com.viettel.paybonus.obj.KitBatch;
import com.viettel.paybonus.obj.KitBatchInfo;
import com.viettel.paybonus.obj.SaleServices;
import com.viettel.paybonus.obj.SaleServicesPrice;
import com.viettel.paybonus.service.Exchange;
import com.viettel.threadfw.manager.AppManager;
import java.util.List;
import org.apache.log4j.Logger;
import com.viettel.threadfw.process.ProcessRecordAbstract;
import com.viettel.vas.util.ExchangeClientChannel;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.ResourceBundle;

/**
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class KitBatchRetryPayComission extends ProcessRecordAbstract {

    Exchange pro;
    DbKitBatchRetryPayComission db;
    String lstIsdnReceiveError;
    String[] arrIsdnReceiveError;
    String kitBatchDevUnitCommission;
    String[] arrKitBatchDevUnitCommission;
    ArrayList<HashMap> lstMapKitBatchDevUnitCommission;
    String[] arrProductBranchPromotion;
    ArrayList<HashMap> lstMapProductBranchPromotion;
    String vasCodeAmountMoney;
    String[] arrVasCodeAmountMoney;
    String vasCodeAmountRate;
    ArrayList<HashMap> lstMapVasCodeAmount;
    String vasCodeConnection;
    String[] arrVasCodeConnection;
    ArrayList<HashMap> lstMapVasCodeConnection;
    String vasCodeParam;
    String[] arrVasCodeParam;
    ArrayList<HashMap> lstMapVasCodeParam;
    String vasCodeActionType;
    String[] arrVasCodeActionType;
    ArrayList<HashMap> lstMapVasCodeActionType;
    String vasCodeChannel;
    String[] arrVasCodeChannel;
    ArrayList<HashMap> lstMapVasCodeChannel;
    String kitConnectUpdateTransCodeFail;
    String eliteKitBatchProductBranchPromotion;
    String bonusRateForPrepaidMonth;

    public KitBatchRetryPayComission() {
        super();
        logger = Logger.getLogger(KitBatchRetryPayComission.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
        db = new DbKitBatchRetryPayComission();

        lstIsdnReceiveError = ResourceBundle.getBundle("configPayBonus").getString("lstIsdnReceiveError");
        arrIsdnReceiveError = lstIsdnReceiveError.split("\\|");



        kitBatchDevUnitCommission = ResourceBundle.getBundle("configPayBonus").getString("KitBatchDevelopmentUnit");
        arrKitBatchDevUnitCommission = kitBatchDevUnitCommission.split("\\|");
        lstMapKitBatchDevUnitCommission = new ArrayList<HashMap>();
        for (String tmp : arrKitBatchDevUnitCommission) {
            String[] arrTmp = tmp.split("\\:");
            HashMap map = new HashMap();
            map.put(arrTmp[0].trim(), arrTmp[1].trim());
            lstMapKitBatchDevUnitCommission.add(map);
        }
        eliteKitBatchProductBranchPromotion = ResourceBundle.getBundle("configPayBonus").getString("eliteKitBatchProductBranchPromotion");
        arrProductBranchPromotion = eliteKitBatchProductBranchPromotion.split("\\|");
        lstMapProductBranchPromotion = new ArrayList<HashMap>();
        for (String tmp : arrProductBranchPromotion) {
            String[] arrTmp = tmp.split("\\:");
            HashMap map = new HashMap();
            map.put(arrTmp[0].trim(), arrTmp[1].trim());
            lstMapProductBranchPromotion.add(map);
        }
        vasCodeAmountMoney = ResourceBundle.getBundle("configPayBonus").getString("vasCodeAmountMoney");
        arrVasCodeAmountMoney = vasCodeAmountMoney.split("\\|");
        lstMapVasCodeAmount = new ArrayList<HashMap>();
        for (String vasAmount : arrVasCodeAmountMoney) {
            String[] tmp = vasAmount.split("\\:");
            HashMap map = new HashMap();
            map.put(tmp[0].trim().toUpperCase(), tmp[1].trim());
            lstMapVasCodeAmount.add(map);
        }
        vasCodeAmountRate = ResourceBundle.getBundle("configPayBonus").getString("vasCodeAmountRate");
        vasCodeConnection = ResourceBundle.getBundle("configPayBonus").getString("vasCodeConnection");
        arrVasCodeConnection = vasCodeConnection.split("\\|");
        lstMapVasCodeConnection = new ArrayList<HashMap>();
        for (String vasConn : arrVasCodeConnection) {
            String[] arrConn = vasConn.split("\\:");
            HashMap map = new HashMap();
            map.put(arrConn[0].trim().toUpperCase(), arrConn[1].trim());
            lstMapVasCodeConnection.add(map);
        }

        vasCodeParam = ResourceBundle.getBundle("configPayBonus").getString("vasCodeParam");
        arrVasCodeParam = vasCodeParam.split("\\|");
        lstMapVasCodeParam = new ArrayList<HashMap>();
        for (String vasConn : arrVasCodeParam) {
            String[] arrConn = vasConn.split("\\:");
            HashMap map = new HashMap();
            map.put(arrConn[0].trim().toUpperCase(), arrConn[1].trim());
            lstMapVasCodeParam.add(map);
        }

        vasCodeActionType = ResourceBundle.getBundle("configPayBonus").getString("vasCodeActionType");
        arrVasCodeActionType = vasCodeActionType.split("\\|");
        lstMapVasCodeActionType = new ArrayList<HashMap>();
        for (String vasConn : arrVasCodeActionType) {
            String[] arrConn = vasConn.split("\\:");
            HashMap map = new HashMap();
            map.put(arrConn[0].trim().toUpperCase(), arrConn[1].trim());
            lstMapVasCodeActionType.add(map);
        }

        vasCodeChannel = ResourceBundle.getBundle("configPayBonus").getString("vasCodeChannel");
        arrVasCodeChannel = vasCodeChannel.split("\\|");
        lstMapVasCodeChannel = new ArrayList<HashMap>();
        for (String vasConn : arrVasCodeChannel) {
            String[] arrConn = vasConn.split("\\:");
            HashMap map = new HashMap();
            map.put(arrConn[0].trim().toUpperCase(), arrConn[1].trim());
            lstMapVasCodeChannel.add(map);
        }
        kitConnectUpdateTransCodeFail = ResourceBundle.getBundle("configPayBonus").getString("kitConnectUpdateTransCodeFail");
        bonusRateForPrepaidMonth = ResourceBundle.getBundle("configPayBonus").getString("bonusRateForPrepaidMonth");

    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        for (Record record : listRecord) {
            KitBatchInfo moRecord = (KitBatchInfo) record;
            moRecord.setNodeName(holder.getNodeName());
            moRecord.setClusterName(holder.getClusterName());
        }
        return listRecord;
    }

    @Override
    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
        List<Record> listResult = new ArrayList<Record>();
        String staffCode;
        long timeSt;
        Long totalCommission;
        long totalCommissionForPrepaidMonth;
        boolean isAddMonth;
        SimpleDateFormat sdf = new SimpleDateFormat("ddMMyyyyHHmmssSSS");

        for (Record record : listRecord) {
            timeSt = System.currentTimeMillis();
            staffCode = "";
            isAddMonth = false;
            totalCommission = 0L;
            totalCommissionForPrepaidMonth = 0;
            KitBatchInfo bn = (KitBatchInfo) record;
            listResult.add(bn);
            if ("0".equals(bn.getResultCode())) {
//                Step 1: Get List connect success by batch...
                List<KitBatch> lstKitBatchConnectSuccess = db.getListKitBatchConnectSuccess(bn.getKitBatchId());
                if (lstKitBatchConnectSuccess.isEmpty()) {
                    logger.warn("List KitBatchConnectSuccess is null or empty, no need to retry pay commission, id " + bn.getKitBatchId());
                    bn.setResultCode("02");
                    bn.setDescription("List KitBatchConnectSuccess is null or empty");
                    bn.setTotalSuccess(0);
                    bn.setTotalFail(0);
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    continue;
                }
                bn.setTotalSuccess(lstKitBatchConnectSuccess.size());
                bn.setTotalFail(0);
                for (KitBatch kitBatch : lstKitBatchConnectSuccess) {
                    Comission comission = db.getComissionStaff(staffCode, kitBatch.getProductCode(), kitBatch.getIsdn());
                    if (comission != null) {//LinhNBV 20180903: DATA_SIM is not vip product, not need insert table kit_vas.
                        totalCommission += comission.getBonusCenter();
                        logger.info("Commssion of product: " + kitBatch.getProductCode() + " is: " + comission.getBonusCenter()
                                + ", totalCommission: " + totalCommission + ", id: " + bn.getKitBatchId() + ", staffCode: " + staffCode);
                    } else {
                        logger.warn("Product code:  " + kitBatch.getProductCode() + " is not VIP. "
                                + "Don't have commission value. " + kitBatch.getIsdn() + ", id: " + bn.getKitBatchId() + ", staffCode: " + staffCode);
                    }
                }
                staffCode = bn.getCreateUser();
                if (staffCode == null || "".equals(staffCode)) {
                    logger.warn("create_user in kit_batch_info is null or empty, id " + bn.getKitBatchId());
                    bn.setResultCode("01");
                    bn.setDescription("CreateStaff in Sub_Profile_Info is null or empty");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    continue;
                }
                String tel = db.getTelByStaffCode(staffCode);
                if (bn.getAddMonth() == null || (bn.getAddMonth() != null && Integer.parseInt(bn.getAddMonth()) == 0)) {
                    //that mean 
                    logger.warn("ConnectKitBatch from: " + bn.getChannelType()
                            + ", addMonth is null or zero, must increase to default is 1 months, id " + bn.getKitBatchId());
                    bn.setAddMonth("1");
                }
                if (bn.getAddMonth() != null && Integer.parseInt(bn.getAddMonth()) > 1) {
                    isAddMonth = true;
                    bn.setAddMonth(String.valueOf(Integer.parseInt(bn.getAddMonth()) - 1));//So thang dong truoc = Total - thang hien tai
                }
//                Step 2: Connect KIT
                for (KitBatch kitBatch : lstKitBatchConnectSuccess) {
//                        Step 2.1: Check ProductCode mapping
                    String productCode = kitBatch.getProductCode().toUpperCase();
                    if (productCode == null || productCode.trim().length() <= 0) {
                        logger.info("productCode is empty, cannot connect kit for sim: " + kitBatch.getSerial()
                                + ", isdn: " + kitBatch.getIsdn() + ", staff: " + staffCode);
                        db.updateKitBatchDetail(bn.getKitBatchId(), kitBatch.getSerial(), kitBatch.getIsdn(), "ERR_01",
                                "productCode is empty", bn.getNodeName(), null, null);
                        continue;
                    }
                    double amountVas = 0;
//                    Step 2.0: Check branch promotion config...
                    String tmpBranchPromotionCode = "";
                    for (int i = 0; i < lstMapProductBranchPromotion.size(); i++) {
                        if (lstMapProductBranchPromotion.get(i).containsKey(kitBatch.getProductCode().trim().toUpperCase())) {
                            tmpBranchPromotionCode = lstMapProductBranchPromotion.get(i).get(kitBatch.getProductCode().trim().toUpperCase()).toString();
                            logger.info("branchPromotionCode: " + tmpBranchPromotionCode
                                    + ", isdn: " + kitBatch.getIsdn() + ", staff: " + staffCode);
                            break;
                        }
                    }
                    if (tmpBranchPromotionCode.length() > 0) {
                        logger.info("branchPromotionCode: " + tmpBranchPromotionCode + ", vasCode = productCode, productCode = branchPromotionCode, vasCode: " + productCode
                                + ", isdn: " + kitBatch.getIsdn() + ", staff: " + staffCode);
                        productCode = tmpBranchPromotionCode;
                        String vasCode = kitBatch.getProductCode().toUpperCase();
                        kitBatch.setProductCode(productCode);
                        kitBatch.setVasCode(vasCode);
                    }
                    if (kitBatch.getVasCode() != null && !kitBatch.getVasCode().isEmpty() && kitBatch.getVasCode().length() > 0) {
                        for (int i = 0; i < lstMapVasCodeAmount.size(); i++) {
                            if (lstMapVasCodeAmount.get(i).containsKey(kitBatch.getVasCode().trim().toUpperCase())) {
                                amountVas += Double.valueOf(lstMapVasCodeAmount.get(i).get(kitBatch.getVasCode().trim().toUpperCase()).toString()) * Double.valueOf(vasCodeAmountRate);
                                logger.error("Total money will be minus on eMola system when register VAS: " + amountVas
                                        + ", VAS_CODE: " + kitBatch.getVasCode() + ", isdn: " + kitBatch.getIsdn());
                                break;
                            }
                        }
                    }
                    kitBatch.setAmountVas(amountVas);

//                        Step 2.2: Calculate saleService Fee + Fee of Isdn
                    Long reasonId = db.getReasonIdByProductCode(productCode, kitBatch.getIsdn());
                    if (reasonId == null) {
                        logger.info("Can not find reasonId, cannot connect kit for sim: " + kitBatch.getSerial()
                                + ", isdn: " + kitBatch.getIsdn() + ", staff: " + staffCode);
                        db.updateKitBatchDetail(bn.getKitBatchId(), kitBatch.getSerial(), kitBatch.getIsdn(), "ERR_02",
                                "Reason is empty, not yet define for this product", bn.getNodeName(), null, null);
                        continue;
                    }
                    kitBatch.setReasonId(reasonId);
                    String saleServiceCode = db.getSaleServiceCode(reasonId, productCode, kitBatch.getIsdn());
                    if (saleServiceCode == null) {
                        logger.info("Can not find saleServiceCode, cannot connect kit for sim: " + kitBatch.getSerial()
                                + ", isdn: " + kitBatch.getIsdn() + ", staff: " + staffCode);
                        db.updateKitBatchDetail(bn.getKitBatchId(), kitBatch.getSerial(), kitBatch.getIsdn(), "ERR_03",
                                "saleServiceCode not yet define for this product", bn.getNodeName(), null, null);
                        continue;
                    }
                    kitBatch.setSaleServiceCode(saleServiceCode);
                    SaleServices saleService = db.getSaleService(saleServiceCode, kitBatch.getIsdn());
                    if (saleService == null) {
                        logger.info("Can not find saleService, cannot connect kit for sim: " + kitBatch.getSerial()
                                + ", isdn: " + kitBatch.getIsdn() + ", staff: " + staffCode);
                        db.updateKitBatchDetail(bn.getKitBatchId(), kitBatch.getSerial(), kitBatch.getIsdn(), "ERR_04",
                                "Can not find saleService", bn.getNodeName(), null, null);
                        continue;
                    }
                    kitBatch.setSaleServices(saleService);
                    SaleServicesPrice saleServicesPrice = db.getSaleServicesPrice(saleService.getSaleServicesId(), kitBatch.getIsdn());
                    if (isAddMonth) {
                        double rateBonusPrepaidMonth = 0.0;
                        try {
                            rateBonusPrepaidMonth = Double.parseDouble(bonusRateForPrepaidMonth);
                        } catch (NumberFormatException ex) {
                            ex.printStackTrace();
                            rateBonusPrepaidMonth = 0.0;
                        }
                        totalCommissionForPrepaidMonth += Math.round(rateBonusPrepaidMonth * saleServicesPrice.getPrice() * Integer.parseInt(bn.getAddMonth()));
                        logger.info("Kit Batch have value addMonth: " + bn.getAddMonth() + ", sum totalMoney with addMonth, "
                                + "id: " + bn.getKitBatchId() + ", staffCode: " + staffCode);
                    }

                }
                String shopCode = db.getShopCodeByStaffCode(staffCode);
                if (totalCommissionForPrepaidMonth > 0) {
                    totalCommission = totalCommission + totalCommissionForPrepaidMonth;
                    logger.info("Kit Batch have value addMonth: " + bn.getAddMonth() + ", bonus more for prepaid month, " + totalCommissionForPrepaidMonth
                            + "id: " + bn.getKitBatchId() + ", staffCode: " + staffCode);
                }
                if (totalCommission > 0) {
                    String isdnWallet = "";
                    if (bn.getChannelType() != null && "MBCCS".equals(bn.getChannelType().toUpperCase())) {
                        isdnWallet = db.getIsdnWalletByStaffCode(staffCode);
                    } else {
                        for (int i = 0; i < lstMapKitBatchDevUnitCommission.size(); i++) {
                            if (lstMapKitBatchDevUnitCommission.get(i).containsKey(bn.getUnitCode().toUpperCase().trim())) {
                                isdnWallet = lstMapKitBatchDevUnitCommission.get(i).get(bn.getUnitCode().toUpperCase().trim()).toString();
                                logger.info("isdnWallet of DevelopmentUnit: " + bn.getUnitCode() + ", isdn: " + isdnWallet
                                        + ", id: " + bn.getKitBatchId() + ", staffCode: " + staffCode);
                                break;
                            }
                        }
                    }
                    if (isdnWallet != null && !isdnWallet.isEmpty()) {
                        logger.info("start to pay commission...isdn: " + isdnWallet
                                + ", id: " + bn.getKitBatchId() + ", staffCode: " + staffCode);
                        try {
                            Long actionAuditId = db.getSequence("seq_action_audit", "cm_pre");
                            String eWalletResponse = pro.callEwalletV3(actionAuditId, 14, isdnWallet, totalCommission, "615",
                                    staffCode, sdf.format(new Date()) + bn.getKitBatchId(), db);
                            if ("01".equals(eWalletResponse)) {
                                //Save Log actionAudit
                                db.insertActionAudit(actionAuditId, "Connect KIT by Batch success for batchId: " + bn.getKitBatchId() + ".",
                                        bn.getCustId(), shopCode, staffCode);
                                logger.info("Pay commission success for kitBatchId " + bn.getKitBatchId() + " isdnEmola "
                                        + isdnWallet + " amount " + totalCommission);
                                String msg = "Pay commission success for kitBatchId " + bn.getKitBatchId() + " isdnEmola "
                                        + isdnWallet + " amount " + totalCommission;
                                db.sendSms(tel, msg, "86904");
                                bn.setResultCode("0");
                                bn.setDescription("Finish retry pay comission for kitBatchId: " + bn.getKitBatchId() + ".");
                                bn.setTotalSuccess(lstKitBatchConnectSuccess.size());
                                bn.setTotalFail(0);
                                bn.setDuration(System.currentTimeMillis() - timeSt);
                            } else {
                                String msg = "Failt to pay commission for kitBatchId " + bn.getKitBatchId() + " isdnEmola "
                                        + isdnWallet + " amount " + totalCommission;
                                db.sendSms(tel, msg, "86904");
                                bn.setResultCode("99");
                                bn.setDescription("Retry pay comission for kitBatchId: " + bn.getKitBatchId() + " is fail.");
                                bn.setDuration(System.currentTimeMillis() - timeSt);
                            }
                        } catch (Exception ex) {
                            ex.printStackTrace();
                            String msg = "Exception when pay commission for kitBatchId " + bn.getKitBatchId() + " isdnEmola "
                                    + isdnWallet + " amount " + totalCommission + ", ex: " + ex.getLocalizedMessage();
                            db.sendSms("258870093239", msg, "86904");
                        }

                    } else {
                        logger.info("Cannot get isdnWallet of user, so cannot pay commission"
                                + ", id: " + bn.getKitBatchId() + ", staffCode: " + staffCode);
                    }
                } else {
                    logger.info("Value of commission is zero,no need to pay"
                            + ", id: " + bn.getKitBatchId() + ", staffCode: " + staffCode);
                }
            } else {
                logger.warn("After validate respone code is fail "
                        + " id " + bn.getKitBatchId() + " staffCode: " + bn.getCreateUser()
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
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        br.setLength(0);
        br.append("\r\n").
                append("|\tKIT_BATCH_ID|").
                append("|\tCREATE_USER|").
                append("|\tCREATE_TIME|").
                append("|\tUNIT_CODE\t|").
                append("|\tCUST_ID\t|").
                append("|\tPAY_TYPE\t|").
                append("|\tBANK_NAME\t|").
                append("|\tBANK_TRAN_CODE\t|").
                append("|\tBANK_TRAN_AMOUNT\t|").
                append("|\tEMOLA_ACCOUNT\t|").
                append("|\tEMOLA_VOUCHER_CODE\t|").
                append("|\tADD_MONTH\t|");
        for (Record record : listRecord) {
            KitBatchInfo bn = (KitBatchInfo) record;
            br.append("\r\n").
                    append("|\t").
                    append(bn.getKitBatchId()).
                    append("||\t").
                    append(bn.getCreateUser()).
                    append("||\t").
                    append((bn.getCreateTime() != null ? sdf.format(bn.getCreateTime()) : null)).
                    append("||\t").
                    append(bn.getUnitCode()).
                    append("||\t").
                    append(bn.getCustId()).
                    append("||\t").
                    append((bn.getPayType())).
                    append("||\t").
                    append(bn.getBankName() != null ? bn.getBankName() : "").
                    append("||\t").
                    append(bn.getBankTransCode() != null ? bn.getBankTransCode() : "").
                    append("||\t").
                    append(bn.getBankTransAmount() != null ? bn.getBankTransAmount() : "").
                    append("||\t").
                    append(bn.getEmolaAccount() != null ? bn.getEmolaAccount() : "").
                    append("||\t").
                    append(bn.getEmolaVoucherCode() != null ? bn.getEmolaVoucherCode() : "").
                    append("||\t").
                    append(bn.getAddMonth() != null ? bn.getAddMonth() : "");
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
