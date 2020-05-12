/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbKitChangeToStudentProcessor;
import com.viettel.paybonus.obj.Bonus;
import com.viettel.paybonus.obj.Price;
import com.viettel.paybonus.obj.ProductConnectKit;
import com.viettel.paybonus.obj.ResponseWallet;
import com.viettel.paybonus.obj.StockModel;
import com.viettel.paybonus.service.BankTransferUtils;
import com.viettel.paybonus.service.EWalletUtil;
import com.viettel.paybonus.service.Exchange;
import com.viettel.threadfw.manager.AppManager;
import java.util.List;
import org.apache.log4j.Logger;
import com.viettel.threadfw.process.ProcessRecordAbstract;
import com.viettel.vas.util.ExchangeClientChannel;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.ResourceBundle;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class KitChangeToStudent extends ProcessRecordAbstract {

    Exchange pro;
    DbKitChangeToStudentProcessor db;
    String msgSystemError;
    String lstIsdnReceiveError;
    String[] arrIsdnReceiveError;
    String transactionError;
    String productCodeNotFound;
    SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddHHmmss");
    String saleServicePriceNotFound;
    String kitConnectHandsetNotFound;
    String kitConnectPriceHandsetNotFound;
    String kitConnectSerialHandsetNotFound;
    String kitConnectUpdateTransCodeFail;
    String kitChangeToStudentSuccess;
    String kitChangeToStudentSuccessChannel;

    public KitChangeToStudent() {
        super();
        logger = Logger.getLogger(KitChangeToStudent.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
        db = new DbKitChangeToStudentProcessor();
        msgSystemError = ResourceBundle.getBundle("configPayBonus").getString("olaSystemError");
        lstIsdnReceiveError = ResourceBundle.getBundle("configPayBonus").getString("lstIsdnReceiveError");
        arrIsdnReceiveError = lstIsdnReceiveError.split("\\|");
        saleServicePriceNotFound = ResourceBundle.getBundle("configPayBonus").getString("saleServicePriceNotFound");
        transactionError = ResourceBundle.getBundle("configPayBonus").getString("transactionError");
        productCodeNotFound = ResourceBundle.getBundle("configPayBonus").getString("productCodeNotFound");
        kitConnectHandsetNotFound = ResourceBundle.getBundle("configPayBonus").getString("kitConnectHandsetNotFound");
        kitConnectPriceHandsetNotFound = ResourceBundle.getBundle("configPayBonus").getString("kitConnectPriceHandsetNotFound");
        kitConnectSerialHandsetNotFound = ResourceBundle.getBundle("configPayBonus").getString("kitConnectSerialHandsetNotFound");
        kitConnectUpdateTransCodeFail = ResourceBundle.getBundle("configPayBonus").getString("kitConnectUpdateTransCodeFail");
        kitChangeToStudentSuccess = ResourceBundle.getBundle("configPayBonus").getString("kitChangeToStudentSuccess");
        kitChangeToStudentSuccessChannel = ResourceBundle.getBundle("configPayBonus").getString("kitChangeToStudentSuccessChannel");

    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        for (Record record : listRecord) {
            Bonus moRecord = (Bonus) record;
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
        String orgRequestId;
        SimpleDateFormat sdf = new SimpleDateFormat("ddMMyyyyHHmmssSSS");
        Double priceOfHandset;
        Price priceOfHandsetObj;
        double comissionHandset;
        Long priceOfProduct;


        for (Record record : listRecord) {
            timeSt = System.currentTimeMillis();
            staffCode = "";
            orgRequestId = "";
            Bonus bn = (Bonus) record;
            priceOfHandset = 0.0;
            priceOfHandsetObj = null;
            comissionHandset = 0;
            priceOfProduct = 0L;
            listResult.add(bn);
            if ("0".equals(bn.getResultCode())) {
                staffCode = bn.getUserName();
                if ("".equals(staffCode)) {
                    logger.warn("CreateStaff in Sub_Profile_Info is null or empty, id " + bn.getId());
                    bn.setResultCode("01");
                    bn.setDescription("CreateStaff in Sub_Profile_Info is null or empty");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    db.rollbackMainProduct(bn.getAgentId(), bn.getId());
                    continue;
                } else {
                    bn.setStaffCode(staffCode);
                }
                if (bn.getIsdnCustomer() == null || bn.getIsdnCustomer().trim().length() <= 0) {
                    logger.warn("ISDN is null or empty, id " + bn.getId());
                    bn.setResultCode("02");
                    bn.setDescription("ISDN is null or empty");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    db.rollbackMainProduct(bn.getAgentId(), bn.getId());
                    continue;
                }
                Long totalAmount = 0L;
                if (bn.getVasCode() != null && !bn.getVasCode().isEmpty()) {
                    logger.info("start get price of add-on vasCode: " + bn.getVasCode() + ", isdn: " + bn.getIsdnCustomer());
                    priceOfProduct = db.getPriceProductConnectKit(bn.getVasCode());
                    if (priceOfProduct == null) {
                        logger.warn("Can not find priceProduct, isdn " + bn.getIsdnCustomer() + ", vasCode: " + bn.getVasCode());
                        String tel = db.getTelByStaffCode(staffCode);
                        db.sendSms(tel, saleServicePriceNotFound, "86142");
                        bn.setResultCode("03");
                        bn.setDescription("Can not find price of add-on");
                        bn.setDuration(System.currentTimeMillis() - timeSt);
                        db.rollbackMainProduct(bn.getAgentId(), bn.getId());
                        continue;
                    }
                } else {
                    priceOfProduct = db.getPriceProductConnectKit(bn.getProductCode());
                    if (priceOfProduct == null) {
                        logger.warn("Can not find priceProduct, isdn " + bn.getIsdnCustomer() + ", vasCode: " + bn.getVasCode());
                        String tel = db.getTelByStaffCode(staffCode);
                        db.sendSms(tel, saleServicePriceNotFound, "86142");
                        bn.setResultCode("03");
                        bn.setDescription("Can not find price of add-on");
                        bn.setDuration(System.currentTimeMillis() - timeSt);
                        db.rollbackMainProduct(bn.getAgentId(), bn.getId());
                        continue;
                    }
                }
                totalAmount = totalAmount + priceOfProduct;
                Long reasonId = 201007L;//EMOLA_CONNECT_NEW 20191227 don't need get reasonId by productCode, now fix a reason
                //get price of handset...
                if (bn.getHandsetModel() != null && !bn.getHandsetModel().isEmpty()) {
                    String mainProductConnect = "";
                    if (bn.getMainProduct() != null && !bn.getMainProduct().isEmpty()) {
                        mainProductConnect = db.getMainProductConnectKit(bn.getMainProduct());
                        if (mainProductConnect.isEmpty()) {
                            logger.warn("Cannot get mainProduct, isdn " + bn.getIsdnCustomer());
                            String tel = db.getTelByStaffCode(staffCode);
                            String message = productCodeNotFound;
                            db.sendSms(tel, message, "86142");
                            bn.setResultCode("04");
                            bn.setDescription("Cannot get mainProduct");
                            bn.setDuration(System.currentTimeMillis() - timeSt);
                            db.rollbackMainProduct(bn.getAgentId(), bn.getId());
                            continue;
                        }
                    }
                    logger.info("start calculate price of handset with discount, no need to pay bonus for customer: "
                            + bn.getHandsetModel() + ", sub:  " + bn.getIsdnCustomer());
                    String[] arrHandsetInfo = bn.getHandsetModel().split("\\|");
                    String stockModelCode = arrHandsetInfo[0].trim();
                    String priceType = arrHandsetInfo[1].trim();
//                        Step 1: Get information of stockModel by stockModelId
                    Long stockModelId = db.getStockModelId(stockModelCode);
                    StockModel stockModel = db.findStockModelById(stockModelId);
                    if (stockModel != null) {
                        String shopPath = db.getShopPathByStaffCode(staffCode);
                        String[] arrShopPath = shopPath.split("\\_");//_7282_shopConfig_shopChildren
                        String shopId = "";
                        if (arrShopPath.length > 2) {
                            shopId = arrShopPath[2];
                        } else {
                            shopId = arrShopPath[1];
                        }
                        String pricePolicy = db.getPricePolicyHandset(mainProductConnect, stockModelCode, shopId);
//                            Step 2: find price for sale (saleRetail)
                        String priceTypeConfig = db.getBasedConfigConnectKit(staffCode, "revenue_type_handset");
                        String[] arrPriceTypeConfig = priceTypeConfig.split("\\|");
                        for (String tmp : arrPriceTypeConfig) {
                            String[] arrTmp = tmp.split("\\-");
                            if (arrTmp[1].trim().equals(mainProductConnect) && "1".equals(arrTmp[0].trim())) {
                                logger.info("Main product: " + mainProductConnect + ", Price and discount is Special, "
                                        + "firstly to assign price to retail price, isdn: " + bn.getIsdnCustomer());
                                Price tmpPriceSpecial = db.getPriceByStockModelCode(stockModelCode, priceType, pricePolicy);
                                if (tmpPriceSpecial == null) {
                                    logger.warn("Cannot get mainProduct, isdn " + bn.getIsdnCustomer());
                                    String tel = db.getTelByStaffCode(staffCode);
                                    String message = productCodeNotFound;
                                    db.sendSms(tel, message, "86142");
                                    bn.setResultCode("13");
                                    bn.setDescription("Cannot get mainProduct");
                                    bn.setDuration(System.currentTimeMillis() - timeSt);
                                    db.rollbackMainProduct(bn.getAgentId(), bn.getId());
                                    continue;
                                }
                                priceType = "1";
                                pricePolicy = "1";
                                long discount = db.getDiscountForHandset(stockModelId, mainProductConnect, shopId);
                                Price tmpPriceHandset = db.getPriceByStockModelCode(stockModelCode, priceType, pricePolicy);
                                if (tmpPriceHandset == null) {
                                    logger.warn("Cannot get mainProduct, isdn " + bn.getIsdnCustomer());
                                    String tel = db.getTelByStaffCode(staffCode);
                                    String message = productCodeNotFound;
                                    db.sendSms(tel, message, "86142");
                                    bn.setResultCode("14");
                                    bn.setDescription("Cannot get mainProduct");
                                    bn.setDuration(System.currentTimeMillis() - timeSt);
                                    db.rollbackMainProduct(bn.getAgentId(), bn.getId());
                                    continue;
                                }
                                comissionHandset = tmpPriceHandset.getPrice() - tmpPriceSpecial.getPrice() + discount;
                                logger.info("Handset have special price, "
                                        + "comission for sale handset = priceRetail(" + tmpPriceHandset.getPrice() + ") - priceSpecial(" + tmpPriceSpecial.getPrice() + ") + discount(" + discount + ") "
                                        + "result: " + comissionHandset);
                                break;
                            }
                        }
                        priceOfHandsetObj = db.getPriceByStockModelCode(stockModelCode, priceType, pricePolicy);
                        if (priceOfHandsetObj != null) {
                            if (!"1".equals(priceType)) {
                                logger.info("Follow promotion price, price of handset is promotion: " + priceOfHandsetObj.getPrice() + ", priceType: " + priceType + ", pricePolicy: " + pricePolicy
                                        + bn.getHandsetModel() + ", isdn: " + bn.getIsdnCustomer());
                            } else {
                                logger.info("Follow special price, price of handset is retail: " + priceOfHandsetObj.getPrice() + ", priceType: " + priceType + ", pricePolicy: " + pricePolicy
                                        + bn.getHandsetModel() + ", isdn: " + bn.getIsdnCustomer());
                            }
                            priceOfHandset = priceOfHandsetObj.getPrice();// - discountHandset;
                            logger.info("Price of Handset " + stockModelCode + ":" + priceOfHandset + ", handsetModel: "
                                    + bn.getHandsetModel() + ", isdn:  " + bn.getIsdnCustomer());
//                            Get serial of handset
                            String handsetSerial = db.getSerialOfHandset(stockModelCode, staffCode);
                            if (handsetSerial.isEmpty()) {
                                logger.info("cannot find serial of handset: "
                                        + stockModelCode + ", of staff: " + staffCode + ", sub:  " + bn.getIsdnCustomer());
                                for (String isdn : arrIsdnReceiveError) {
                                    db.sendSms(isdn, "cannot find serial handset: " + stockModelCode + ", of staff: " + staffCode
                                            + ", isdnCustomer: " + bn.getIsdnCustomer(), "86142");
                                }
                                logger.warn("Can not find serial of handset: " + bn.getHandsetModel()
                                        + " isdn " + bn.getIsdnCustomer());
                                String tel = db.getTelByStaffCode(staffCode);
                                db.sendSms(tel, kitConnectSerialHandsetNotFound, "86142");
                                bn.setResultCode("05");
                                bn.setDescription("Cannot find serial of handset");
                                bn.setDuration(System.currentTimeMillis() - timeSt);
                                db.rollbackMainProduct(bn.getAgentId(), bn.getId());
                                continue;
                            }
                            bn.setHandsetSerial(handsetSerial);
                        } else {
                            logger.info("cannot find price for saleRetail by serial: "
                                    + bn.getHandsetModel() + ", sub:  " + bn.getIsdnCustomer());
                            for (String isdn : arrIsdnReceiveError) {
                                db.sendSms(isdn, "cannot find price for saleRetail, serialHanset: " + bn.getHandsetModel() + ", subProfileId: " + bn.getId()
                                        + ", isdnCustomer: " + bn.getIsdnCustomer(), "86142");
                            }
                            logger.warn("Can not find price for handset: " + bn.getHandsetModel()
                                    + " isdn " + bn.getIsdnCustomer());
                            String tel = db.getTelByStaffCode(staffCode);
                            db.sendSms(tel, kitConnectPriceHandsetNotFound, "86142");
                            bn.setResultCode("06");
                            bn.setDescription("Cannot find stockModel by stockModelCode");
                            bn.setDuration(System.currentTimeMillis() - timeSt);
                            db.rollbackMainProduct(bn.getAgentId(), bn.getId());
                            continue;
                        }
                    } else {
                        logger.info("cannot find stockModel by stockModelCode: "
                                + stockModelCode + ", sub:  " + bn.getIsdnCustomer());
                        for (String isdn : arrIsdnReceiveError) {
                            db.sendSms(isdn, "Cannot find stockModel by stockModelCode: " + stockModelCode
                                    + ", isdnCustomer: " + bn.getIsdnCustomer(), "86142");
                        }
                        logger.warn("Can not find price for handset: " + bn.getHandsetModel() + ", subProfileId: " + bn.getId()
                                + " isdn " + bn.getIsdnCustomer());
                        String tel = db.getTelByStaffCode(staffCode);
                        db.sendSms(tel, kitConnectHandsetNotFound, "86142");
                        bn.setResultCode("07");
                        bn.setDescription("Cannot find stockModel by stockModelCode");
                        bn.setDuration(System.currentTimeMillis() - timeSt);
                        db.rollbackMainProduct(bn.getAgentId(), bn.getId());
                        continue;
                    }
                }
                //Payment voucher via eMola.
                if ((totalAmount + priceOfHandset) > 0) {
                    if (bn.getPayMethod() == 0 && bn.getBankName() != null && bn.getBankName().length() > 0
                            && bn.getBankTranAmount() != null && bn.getBankTranAmount().length() > 0
                            && bn.getBankTranCode() != null && bn.getBankTranCode().length() > 0) {
                        logger.info("start update transcode: " + bn.getBankTranCode() + ", bankName: " + bn.getBankName() + ", amount: "
                                + bn.getBankTranAmount() + ", isdn " + bn.getIsdnCustomer());
                        if (Double.parseDouble(bn.getBankTranAmount()) >= totalAmount + priceOfHandset) {
                            String checkTrans = BankTransferUtils.callWSSOAPupdateTrans(bn.getBankTranCode(), bn.getBankName(), bn.getBankTranAmount(),
                                    staffCode, staffCode);
                            if ("00".equalsIgnoreCase(checkTrans)) {
                                logger.info("Update TransCode successfully, transcode: " + bn.getBankTranCode() + ", bankName: " + bn.getBankName() + ", amount: "
                                        + bn.getBankTranAmount() + ", isdn " + bn.getIsdnCustomer());
                            } else {
                                logger.info("Update TransCode unsuccessfully, start rollback...., transcode: " + bn.getBankTranCode() + ", bankName: " + bn.getBankName() + ", amount: "
                                        + bn.getBankTranAmount() + ", isdn " + bn.getIsdnCustomer());
                                //rollbackTrans
                                BankTransferUtils.callWSSOAProllbackTrans(bn.getBankTranCode(), bn.getBankName(),
                                        bn.getBankTranAmount(), staffCode, staffCode);
                                //Update status sub_id_no ve 0
                                logger.warn("Fail to update transCode, for handset: " + bn.getHandsetModel() + ", subProfileId: " + bn.getId()
                                        + " isdn " + bn.getIsdnCustomer());
                                String tel = db.getTelByStaffCode(staffCode);
                                db.sendSms(tel, kitConnectUpdateTransCodeFail, "86142");
                                bn.setResultCode("08");
                                bn.setDescription("Fail to update TransCode");
                                bn.setDuration(System.currentTimeMillis() - timeSt);
                                db.rollbackMainProduct(bn.getAgentId(), bn.getId());
                                continue;
                            }
                        } else {
                            logger.info("Total amount (include handset): " + totalAmount + ", less than bankTransAmount: " + bn.getBankTranAmount()
                                    + ", isdn " + bn.getIsdnCustomer());
                            //rollbackTrans
                            BankTransferUtils.callWSSOAProllbackTrans(bn.getBankTranCode(), bn.getBankName(),
                                    bn.getBankTranAmount(), staffCode, staffCode);
                            //Update status sub_id_no ve 0
                            logger.warn("BankTransAmount less than totalAmount for handset: " + bn.getHandsetModel() + ", subProfileId: " + bn.getId()
                                    + " isdn " + bn.getIsdnCustomer());
                            String tel = db.getTelByStaffCode(staffCode);
                            db.sendSms(tel, kitConnectUpdateTransCodeFail, "86142");
                            bn.setResultCode("09");
                            bn.setDescription("BankTransAmount less than totalAmount.");
                            bn.setDuration(System.currentTimeMillis() - timeSt);
                            db.rollbackMainProduct(bn.getAgentId(), bn.getId());
                            continue;
                        }
                    } else if (bn.getPayMethod() == 2) {
                        logger.info("staffCode: " + staffCode + " using POS to connect charge money, so no need charge money and clear debit (later), "
                                + "referenceId: " + bn.getReferenceId() + ", isdn " + bn.getIsdnCustomer());
                    } else {
                        logger.info("start call eMola to pay saleServices, isdn " + bn.getIsdnCustomer());
                        ResponseWallet responseWallet = EWalletUtil.paymentVoucher(db, bn, (totalAmount + priceOfHandset),
                                logger, bn.getIsdnCustomer());//???? Save log ewallet...
                        if (responseWallet == null || !"01".equals(responseWallet.getResponseCode())) {
                            //Pay not success >>> so we will revert all....
                            logger.warn("Fail to charge Emola for handset: " + bn.getHandsetModel() + ", subProfileId: " + bn.getId()
                                    + " isdn " + bn.getIsdnCustomer());
                            String tel = db.getTelByStaffCode(staffCode);
                            db.sendSms(tel, transactionError, "86142");
                            bn.setResultCode("10");
                            bn.setDescription("Fail to charge Emola");
                            bn.setDuration(System.currentTimeMillis() - timeSt);
                            db.rollbackMainProduct(bn.getAgentId(), bn.getId());
                            continue;
                        }
                        orgRequestId = responseWallet.getRequestId();//RequestId using when revertTransaction
                    }
                } else {
                    logger.info("Total money less than 0 so not charge " + totalAmount + " isdn " + bn.getIsdnCustomer());
                }
                //Topup and insert MO
                ProductConnectKit productConnectKit = null;
                if (bn.getVasCode() != null && !bn.getVasCode().isEmpty()) {
                    String mainProduct = db.getMainProduct("MOVSTU1", bn.getIsdnCustomer());
                    if (mainProduct == null) {
                        logger.warn("Can not find mainProduct, for handset: " + bn.getHandsetModel() + ", subProfileId: " + bn.getId()
                                + " isdn " + bn.getIsdnCustomer());
                        //Revert money
                        if (bn.getPayMethod() == 1) {
                            EWalletUtil.revertTransaction(db, bn, logger, orgRequestId, bn.getIsdnCustomer());
                        }
                        String tel = db.getTelByStaffCode(staffCode);
                        db.sendSms(tel, msgSystemError, "86142");
                        bn.setResultCode("11");
                        bn.setDescription("Can not find price plan code to active on Provisioning.");
                        bn.setDuration(System.currentTimeMillis() - timeSt);
                        db.rollbackMainProduct(bn.getAgentId(), bn.getId());
                        continue;
                    }
                    String resultChange = pro.changeProduct(bn.getIsdnCustomer(), mainProduct);
                    if (!"0".equals(resultChange)) {
                        logger.warn("Failt to change product " + bn.getIsdnCustomer() + " old " + bn.getProductCode() + " new MOVSTU1");
                        if (bn.getPayMethod() == 1) {
                            EWalletUtil.revertTransaction(db, bn, logger, orgRequestId, bn.getIsdnCustomer());
                        }
                        String tel = db.getTelByStaffCode(staffCode);
                        db.sendSms(tel, msgSystemError, "86142");
                        bn.setResultCode("12");
                        bn.setDescription("Failt to change product.");
                        bn.setDuration(System.currentTimeMillis() - timeSt);
                        db.rollbackMainProduct(bn.getAgentId(), bn.getId());
                        continue;
                    }
//                    Insert MO
                    productConnectKit = db.getProductConnectKit(bn.getVasCode());
                    db.updateSubMb(bn.getIsdnCustomer(), "MOVSTU1", 3379);
                } else {
//                    Insert MO convert to Elite...
                    productConnectKit = db.getProductConnectKit(bn.getProductCode());
                }
                if (productConnectKit != null && productConnectKit.getMoneyFee() != null && !productConnectKit.getMoneyFee().isEmpty()) {
//                    Date now = new Date();
//                    String strDate = sdf2.format(now);
//                    String requestIdTopup = "KIT_" + strDate + bn.getId();
                    String rsTopup = pro.addMoney(bn.getIsdnCustomer(), productConnectKit.getMoneyFee(), "1");
//                    String rsTopup = pro.topupWS(requestIdTopup, bn.getIsdnCustomer(), productConnectKit.getMoneyFee());
                    if ("0".equals(rsTopup)) {
                        logger.info("Topup successfully for sub: " + bn.getIsdnCustomer() + ", productCode: " + productConnectKit.getProductCode()
                                + ", amount: " + productConnectKit.getMoneyFee());
                        String connName = productConnectKit.getVasConnection();
                        String param = productConnectKit.getVasParam();
                        String actionType = productConnectKit.getVasActionType();
                        String channel = productConnectKit.getVasChannel();
//                            Insert MO
                        if (param != null && "?".equals(param)) {
                            param = staffCode;
                        }
                        int rsInsertMo = db.insertMO(bn.getIsdnCustomer(), productConnectKit.getProductCode(), connName, param, actionType, channel);
                        if (rsInsertMo == 1) {
                            logger.info("Insert MO successfully for sub: " + bn.getIsdnCustomer() + ", productCode: " + productConnectKit.getProductCode() + ", amount: "
                                    + productConnectKit.getMoneyFee());
                        } else {
                            logger.info("Topup successfully but insert MO unsuccessfully for sub: " + bn.getIsdnCustomer() + ", productCode: " + productConnectKit.getProductCode()
                                    + ", amount: " + productConnectKit.getMoneyFee());
                        }
                    } else {
                        logger.info("Topup unsuccessfully for sub: " + bn.getIsdnCustomer() + ", productCode: " + productConnectKit.getProductCode()
                                + ", amount: " + productConnectKit.getMoneyFee());
                    }

                } else {
                    logger.info("Cannot get amount topup, isdn: " + bn.getIsdnCustomer());
                }

                if (bn.getHandsetModel() != null && !bn.getHandsetModel().isEmpty()) {
                    logger.info("start sale handset with discount, no need to pay bonus for customer: "
                            + bn.getHandsetModel() + ", sub:  " + bn.getIsdnCustomer());
                    String[] arrHandsetInfo = bn.getHandsetModel().split("\\|");
                    String stockModelCode = arrHandsetInfo[0].trim();
//                        Step 1: Get information of stockModel by stockModelId
                    Long stockModelId = db.getStockModelId(stockModelCode);
                    StockModel stockModel = db.findStockModelById(stockModelId);
                    if (stockModel != null) {
//                                Step 3.1: Make SaleTrans, SaleTransDetail, SaleTransSerial...
                        Long saleTransId = db.getSequence("SALE_TRANS_SEQ", "dbsm");
                        String strTempId = db.getShopIdStaffIdByStaffCode(staffCode);//SHOP_ID|STAFF_ID --> When channel sale handset --> make saleTrans for Manager of channel
                        String[] arrTempId = strTempId.split("\\|");
                        int rsMakeSaleTrans = db.insertSaleTrans(saleTransId, Long.valueOf(arrTempId[0]), Long.valueOf(arrTempId[1]),
                                0L, 0L, priceOfHandset, 0L,
                                "", 200351L, orgRequestId, bn.getPayMethod(), "", 0, 0, bn.getId());
                        if (rsMakeSaleTrans != 1) {
                            //Insert fail, insert log, send sms
                            db.insertLogMakeSaleTransFail(saleTransId, bn.getIsdnCustomer(), bn.getSimSerial(), Long.valueOf(arrTempId[0]),
                                    Long.valueOf(arrTempId[1]), 0L, 0L,
                                    200351L, 0L, 0L, "SALE_TRANS");
                            for (String isdn : arrIsdnReceiveError) {
                                db.sendSms(isdn, "Make saleTrans fail, more detail in table: kit_make_sale_trans_fail. saleTransId: "
                                        + saleTransId, "86142");
                            }
                        }
                        //Make sale trans order if payment by bankTransfer...
                        if (bn.getPayMethod() == 0 && bn.getBankName() != null && bn.getBankName().length() > 0
                                && bn.getBankTranAmount() != null && bn.getBankTranAmount().length() > 0
                                && bn.getBankTranCode() != null && bn.getBankTranCode().length() > 0) {
                            int rsMakeSaleTransOrder = db.insertSaleTransOrder(bn.getBankName(), bn.getBankTranAmount(),
                                    bn.getBankTranCode(), saleTransId, "Clear by bankTransfer from change product to STUDENT");
                            if (rsMakeSaleTransOrder != 1) {
                                //Insert fail, insert log, send sms
                                db.insertLogMakeSaleTransFail(saleTransId, bn.getIsdnCustomer(), bn.getSimSerial(), Long.valueOf(arrTempId[0]),
                                        Long.valueOf(arrTempId[1]), 0L, 0L,
                                        reasonId, 0L, 0L, "SALE_TRANS_ORDER");
                                for (String isdn : arrIsdnReceiveError) {
                                    db.sendSms(isdn, "Make saleTransOrder fail, more detail in table: kit_make_sale_trans_fail. saleTransId: "
                                            + saleTransId + ", kitBatchId: " + bn.getKitBatchId(), "86142");
                                }
                            }
                        }
                        //Make sale trans detail
                        //1. Detail for SaleServices
                        Long saleTransDetailSaleService = db.getSequence("SALE_TRANS_DETAIL_SEQ", "dbsm");
                        int rsSaleTransDetailSaleServices = db.insertSaleTransDetail(saleTransDetailSaleService, saleTransId, String.valueOf(stockModelId), String.valueOf(priceOfHandsetObj.getPriceId()), "",
                                "", "7", "Handset", stockModel.getAccountingModelCode(), stockModel.getAccountingModelName(), "", "",
                                stockModel.getAccountingModelCode(), stockModel.getAccountingModelName(), "", "17",
                                String.valueOf(priceOfHandset), "", priceOfHandset, 0.0, 1L);
                        if (rsSaleTransDetailSaleServices != 1) {
                            //Insert fail, insert log, send sms
                            db.insertLogMakeSaleTransFail(saleTransId, bn.getIsdnCustomer(), bn.getSimSerial(), Long.valueOf(arrTempId[0]),
                                    Long.valueOf(arrTempId[1]), 0L, 0L,
                                    200351L, saleTransDetailSaleService, 0L, "SALE_TRANS_DETAIL");
                            for (String isdn : arrIsdnReceiveError) {
                                db.sendSms(isdn, "Make saleTransDetail for SaleServices fail, more detail in table: kit_make_sale_trans_fail. saleTransDetailId: " + saleTransDetailSaleService, "86142");
                            }
                        }
                        //Make saleTransSerial
                        Long saleTransSerialId = db.getSequence("SALE_TRANS_SERIAL_SEQ", "dbsm");
                        int rsSaleTransSerial = db.insertSaleTransSerial(saleTransSerialId, saleTransDetailSaleService, stockModelId, bn.getHandsetSerial());
                        if (rsSaleTransSerial != 1) {
                            //Insert fail, insert log, send sms
                            db.insertLogMakeSaleTransFail(0L, bn.getIsdnCustomer(), bn.getSimSerial(), Long.valueOf(arrTempId[0]),
                                    Long.valueOf(arrTempId[1]), 0L, 0L,
                                    200351L, saleTransDetailSaleService, saleTransSerialId, "SALE_TRANS_SERIAL");
                            for (String isdn : arrIsdnReceiveError) {
                                db.sendSms(isdn, "Make saleTransSerial fail, more detail in table: kit_make_sale_trans_fail. saleTransSerialId: " + saleTransSerialId, "86142");
                            }
                        }
//                                Step 4: Minus stockTotal
                        int expStockTotal = db.expStockTotal(staffCode, stockModelId, 1L);
                        if (expStockTotal != 1) {
                            //Insert fail, insert log, send sms
                            for (String isdn : arrIsdnReceiveError) {
                                db.sendSms(isdn, "expStockTotal fail, serialHanset: " + bn.getHandsetSerial()
                                        + ", isdnCustomer: " + bn.getIsdnCustomer(), "86142");
                            }
                        }
//                                Step 5: update stock_hand >> status = 0 >> already sale...
                        int updateSeialExp = db.updateSeialExp(staffCode, stockModelId, bn.getHandsetSerial());
                        if (updateSeialExp != 1) {
                            //Insert fail, insert log, send sms
                            for (String isdn : arrIsdnReceiveError) {
                                db.sendSms(isdn, "updateSeialExp fail, serialHanset: " + bn.getHandsetSerial()
                                        + ", isdnCustomer: " + bn.getIsdnCustomer(), "86142");
                            }
                        }
                    }

                }
                //Make sale trans
                if (totalAmount > 0) {
                    logger.info("Total amount: " + totalAmount + " greater than 0, need to make revenue, isdn: " + bn.getIsdnCustomer());
                    Long saleTransId = db.getSequence("SALE_TRANS_SEQ", "dbsm");
                    String strTempId = db.getShopIdStaffIdByStaffCode(staffCode);
                    String[] arrTempId = strTempId.split("\\|");
                    int rsMakeSaleTrans = db.insertSaleTrans(saleTransId, Long.valueOf(arrTempId[0]), Long.valueOf(arrTempId[1]),
                            0L, 0L, totalAmount.doubleValue(), Long.valueOf(bn.getPkId()),
                            bn.getIsdnCustomer(), reasonId, orgRequestId, bn.getPayMethod(), bn.getBankTranCode(), 0.0, 0, bn.getId());
                    if (rsMakeSaleTrans != 1) {
                        //Insert fail, insert log, send sms
                        db.insertLogMakeSaleTransFail(saleTransId, bn.getIsdnCustomer(), bn.getSimSerial(), Long.valueOf(arrTempId[0]),
                                Long.valueOf(arrTempId[1]), 0L, 0L,
                                reasonId, 0L, 0L, "SALE_TRANS");
                        for (String isdn : arrIsdnReceiveError) {
                            db.sendSms(isdn, "Make saleTrans fail, more detail in table: kit_make_sale_trans_fail. saleTransId: "
                                    + saleTransId, "86142");
                        }
                    }
                    //Make sale trans order if payment by bankTransfer...
                    if (bn.getPayMethod() == 0 && bn.getBankName() != null && bn.getBankName().length() > 0
                            && bn.getBankTranAmount() != null && bn.getBankTranAmount().length() > 0
                            && bn.getBankTranCode() != null && bn.getBankTranCode().length() > 0) {
                        int rsMakeSaleTransOrder = db.insertSaleTransOrder(bn.getBankName(), bn.getBankTranAmount(),
                                bn.getBankTranCode(), saleTransId, "Clear by bankTransfer from change product to STUDENT");
                        if (rsMakeSaleTransOrder != 1) {
                            //Insert fail, insert log, send sms
                            db.insertLogMakeSaleTransFail(saleTransId, bn.getIsdnCustomer(), bn.getSimSerial(), Long.valueOf(arrTempId[0]),
                                    Long.valueOf(arrTempId[1]), 0L, 0L,
                                    reasonId, 0L, 0L, "SALE_TRANS_ORDER");
                            for (String isdn : arrIsdnReceiveError) {
                                db.sendSms(isdn, "Make saleTransOrder fail, more detail in table: kit_make_sale_trans_fail. saleTransId: "
                                        + saleTransId + ", kitBatchId: " + bn.getKitBatchId(), "86142");
                            }
                        }
                    }
                    //Make sale trans detail
                    //1. Detail for SaleServices
                    Long saleTransDetailSaleService = db.getSequence("SALE_TRANS_DETAIL_SEQ", "dbsm");
                    int rsSaleTransDetailSaleServices = db.insertSaleTransDetail(saleTransDetailSaleService, saleTransId, "208309", "520784", "",//stockModelId = 208309 for emola scratch card connect, priceId = 520784 --> For price of emola scratch card
                            "", "", "", "", "", "", "", "", "", "", "17",
                            "1", "", totalAmount.doubleValue(), 0.0, totalAmount);
                    if (rsSaleTransDetailSaleServices != 1) {
                        //Insert fail, insert log, send sms
                        db.insertLogMakeSaleTransFail(saleTransId, bn.getIsdnCustomer(), bn.getSimSerial(), Long.valueOf(arrTempId[0]),
                                Long.valueOf(arrTempId[1]), 0L, 0L,
                                reasonId, saleTransDetailSaleService, 0L, "SALE_TRANS_DETAIL");
                        for (String isdn : arrIsdnReceiveError) {
                            db.sendSms(isdn, "Make saleTransDetail for SaleServices fail, more detail in table: kit_make_sale_trans_fail. saleTransDetailId: " + saleTransDetailSaleService, "86142");
                        }
                    }
                } else {
                    logger.info("Total amount is 0 MT, no need to make revenue for transaction..., isdn: " + bn.getIsdnCustomer());
                }
                String idNo = db.getIdNo(bn.getIsdnCustomer());
                db.updateSubIdNo(bn.getStudentCardCode(), idNo, bn.getPkId());
                //insert action audit
                db.insertActionAudit(bn.getIsdnCustomer(), bn.getIsdnCustomer(), "Change product to STUDENT", bn.getPkId());
                String tel = db.getTelByStaffCode(staffCode);
                String tmpMsg = "", tmpMsgChannel = "";
                if (bn.getVasCode() != null && !bn.getVasCode().isEmpty()) {
                    tmpMsg = kitChangeToStudentSuccess.replace("%XXX%", bn.getVasCode());
                    tmpMsgChannel = kitChangeToStudentSuccessChannel.replace("%XXX%", bn.getVasCode());
                } else {
                    tmpMsg = kitChangeToStudentSuccess.replace("%XXX%", bn.getProductCode());
                    tmpMsgChannel = kitChangeToStudentSuccessChannel.replace("%XXX%", bn.getProductCode());
                }
                tmpMsgChannel = tmpMsgChannel.replace("%ISDN%", bn.getIsdnCustomer());
                db.sendSms(tel, tmpMsgChannel, "86142");
                db.sendSms(bn.getIsdnCustomer(), tmpMsg, "86142");
                bn.setResultCode("0");
                bn.setDescription("Success change product");
                logger.info("isdn " + bn.getIsdnCustomer() + " Message send to channel: " + tmpMsg);
                //Pay money back....
                long diff = new Date().getTime() - bn.getIssueDateTime().getTime();
                long days = TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);
                days++; //add one more day
                if (days <= 7) {
                    String isdnWallet = db.getIsdnWalletByStaffCode(staffCode);
                    if (isdnWallet != null && isdnWallet.length() > 0 && comissionHandset > 0
                            && (bn.getHandsetModel() != null && !bn.getHandsetModel().isEmpty())) {
                        logger.info("start pay money for user connect with handset: " + bn.getHandsetModel()
                                + ", user: " + staffCode + ", isdn: " + bn.getIsdnCustomer());
                        String eWalletResponse = pro.callEwalletV2(bn.getActionAuditId(), bn.getChannelTypeId(), isdnWallet,
                                Math.round(comissionHandset), "615", staffCode, sdf.format(new Date()), 6, db);
                        if ("01".equals(eWalletResponse)) {
                            db.sendSms("258" + tel, "Voce recebeu o total de " + comissionHandset + " MT por vender o telemovel " + bn.getHandsetModel().split("\\|")[0].trim() + ". Obrigado!", "86142");
                        }
                        logger.info("end pay commission for connect with handset, response: " + eWalletResponse + ", isdn: " + bn.getIsdnCustomer());
                    } else {
                        logger.info("don't have isdnWallet, can't paid money when connect with handset: " + bn.getHandsetModel()
                                + ", user: " + staffCode + ", isdn: " + bn.getIsdnCustomer());
                    }
                } else {
                    logger.info("Over 7 days, no need paid money when connect with handset, isdn: " + bn.getIsdnCustomer());
                }

            } else {
                logger.warn("After validate respone code is fail actionId " + bn.getActionAuditId()
                        + " id " + bn.getId() + " isdn: " + bn.getIsdnCustomer()
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
                append("|\tSIM_SERIAL\t|").
                append("|\tCONNECT_KIT_STATUS\t|").
                append("|\tVAS_CODE\t|").
                append("|\tKIT_BATCH_ID\t|").
                append("|\tPREPAID_MONTH\t|").
                append("|\tPAY_METHOD\t|").
                append("|\tBONUS_STATUS\t|");
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
                    append(bn.getSimSerial()).
                    append("||\t").
                    append(bn.getConnectKitStatus()).
                    append("||\t").
                    append(bn.getVasCode()).
                    append("||\t").
                    append(bn.getKitBatchId()).
                    append("||\t").
                    append(bn.getPrepaidMonth()).
                    append("||\t").
                    append(bn.getPayMethod()).
                    append("||\t").
                    append(bn.getBonusStatus());
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
