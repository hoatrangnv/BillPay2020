/*
 * Copyright (C) 2010 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.vas.wsfw.services;

import com.viettel.data.ws.utils.Encrypt;
import com.viettel.vas.util.ExchangeClientChannel;
import com.viettel.vas.wsfw.common.WebserviceAbstract;
import javax.jws.WebMethod;
import javax.jws.WebParam;
import javax.jws.WebService;
import com.viettel.vas.wsfw.common.Common;
import com.viettel.vas.wsfw.common.Vas;
import com.viettel.vas.wsfw.database.DbPost;
import com.viettel.vas.wsfw.database.DbPre;
import com.viettel.vas.wsfw.database.DbProcessor;
import com.viettel.vas.wsfw.object.Subscriber;
import com.viettel.vas.wsfw.object.Topup;
import com.viettel.vas.wsfw.object.TransLog;
import com.viettel.vas.wsfw.object.UserInfo;
import com.viettel.data.ws.utils.Exchange;
import com.viettel.data.ws.utils.Service;
import com.viettel.data.ws.utils.Utils;
import com.viettel.im.database.BO.InvoiceListBean;
import com.viettel.im.database.DAO.InvoiceListDAO;
import com.viettel.smsfw.manager.AppManager;
import com.viettel.vas.wsfw.object.ProductMonthlyFee;
import com.viettel.vas.wsfw.object.ResponseWallet;
import com.viettel.vas.wsfw.object.SubAdslLLPrepaid;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.ResourceBundle;
import org.hibernate.Session;

/**
 *
 * @author Huynq13@viettel.com.vn
 * @since Jun 4, 2013
 * @version 1.0
 */
@WebService
public class TopupHavePreFund extends WebserviceAbstract {

    DbProcessor db;
    DbPre dbPre;
    DbPost dbPost;
    Exchange exch;
    String sRateTopupFee;
    Double rateTopupFee;
    String payType;
    String sStaffIdBillPay;
    String staffCode;
    String groupId;
    long staffIdBillPay;
    long shopIdBillPay;
    String shopId;
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
    Service service;
    SimpleDateFormat sdf2 = new SimpleDateFormat("dd/MM/yyyy");
    String msgFtthNotEnoughMoney;
    String msgFtthFail;
    String msgFtthSuccess;

    public TopupHavePreFund() throws Exception {
        super("TopupHavePreFund");
        try {
            logger.info("Start init webservice TopupHavePreFund");
            sRateTopupFee = ResourceBundle.getBundle("vas").getString("rateTopupFee");
            rateTopupFee = Double.valueOf(sRateTopupFee);
            sStaffIdBillPay = ResourceBundle.getBundle("vas").getString("invoiceStaffIdBillPay");
            staffIdBillPay = Long.valueOf(sStaffIdBillPay);
            shopId = ResourceBundle.getBundle("vas").getString("invoiceShopIdBillPay");
            shopIdBillPay = Long.valueOf(shopId);
            staffCode = ResourceBundle.getBundle("vas").getString("invoiceStaffCode");
            groupId = ResourceBundle.getBundle("vas").getString("invoiceGroupId");
            dbPre = new DbPre("cm_pre", logger);
            dbPost = new DbPost("cm_pos", logger);
            db = new DbProcessor("dbtopup", logger);
            exch = new Exchange(ExchangeClientChannel.getInstance("../etc/exchange_client.cfg").getInstanceChannel(), logger);
            service = new Service(ExchangeClientChannel.getInstance("../etc/service_client.cfg").getInstanceChannel(), logger);
            payType = ResourceBundle.getBundle("vas").getString("payType");
            msgFtthNotEnoughMoney = ResourceBundle.getBundle("vas").getString("msgFtthNotEnoughMoney");
            msgFtthFail = ResourceBundle.getBundle("vas").getString("msgFtthFail");
            msgFtthSuccess = ResourceBundle.getBundle("vas").getString("msgFtthSuccess");
        } catch (Exception e) {
            logger.error("Fail init webservice TopupHavePreFund");
            logger.error(e);
        }
    }

    @WebMethod(operationName = "TopupHavePreFund")
    public String TopupHavePreFund(
            //            @WebParam(header = true, name = "client", targetNamespace = "") String client,
            @WebParam(name = "requestId", targetNamespace = "") String requestId,
            @WebParam(name = "msisdn", targetNamespace = "") String msisdn,
            @WebParam(name = "amount", targetNamespace = "") String amount,
            @WebParam(name = "fundCode", targetNamespace = "") String fundCode,
            @WebParam(name = "signature", targetNamespace = "") String signature,
            @WebParam(name = "partnerCode", targetNamespace = "") String partnerCode,
            @WebParam(name = "sourceId", targetNamespace = "") String sourceId,
            @WebParam(name = "serviceType", targetNamespace = "") String serviceType,
            @WebParam(name = "userName") String wsuser,
            @WebParam(name = "passWord") String wspassword) {
        TransLog tran = new TransLog();
        tran.setClient(wsuser);
        tran.setInput(msisdn + "#" + amount + "#" + fundCode + "#" + requestId + "#" + signature);
        tran.setMoney(amount);
        tran.setTransType(Vas.Topup.TRANS_TYPE_TOPUP_HAVE_PRE_FUND);
        tran.setWsCode("TopupHavePreFund");
        tran.setIsdn(msisdn);
        tran.setFundCode(fundCode);
        tran.setRequestId(requestId);
        long timeStart = System.currentTimeMillis();
        tran.setStartTime(new Timestamp(timeStart));
        tran.setPartnerCode(partnerCode);
        tran.setServiceType(serviceType);
        tran.setSourceId(sourceId);
        tran.setSignature(signature);
        boolean isDeductFund = false; //Add to rollback if have exception
        double topupFee = 0;
        try {
            logger.info("Start process TopupHavePreFund for sub " + msisdn + " client " + wsuser);
//        step 1: validate input
            if (msisdn == null || "".equals(msisdn.trim())
                    || amount == null || "".equals(amount.trim())
                    || wsuser == null || "".equals(wsuser.trim())
                    || wspassword == null || "".equals(wspassword.trim())
                    || requestId == null || "".equals(requestId.trim())
                    || fundCode == null || "".equals(fundCode.trim())
                    || signature == null || "".equals(signature.trim())
                    || partnerCode == null || "".equals(partnerCode.trim())
                    || sourceId == null || "".equals(sourceId.trim())
                    || serviceType == null || "".equals(serviceType.trim())
                    || msisdn.length() > 99
                    || amount.length() > 5
                    || requestId.length() > 99
                    || fundCode.length() > 10
                    || signature.length() > 160
                    || partnerCode.length() > 3
                    || sourceId.length() > 1
                    || serviceType.length() > 1) {
                logger.warn("Invalid input sub " + msisdn + " length " + (msisdn == null ? 0 : msisdn.length())
                        + " amount " + amount + " length " + (amount == null ? 0 : amount.length())
                        + " wsuser " + wsuser + " length " + (wsuser == null ? 0 : wsuser.length())
                        + " length " + (wspassword == null ? 0 : wspassword.length())
                        + " requestId " + requestId + " length " + (requestId == null ? 0 : requestId.length())
                        + " fundCode " + fundCode + " length " + (fundCode == null ? 0 : fundCode.length())
                        + " signature " + signature + " length " + (signature == null ? 0 : signature.length())
                        + " partnerCode " + partnerCode + " length " + (partnerCode == null ? 0 : partnerCode.length())
                        + " sourceId " + sourceId + " length " + (sourceId == null ? 0 : sourceId.length())
                        + " serviceType " + serviceType + " length " + (serviceType == null ? 0 : serviceType.length()));
                tran.setDuration(System.currentTimeMillis() - timeStart);
                tran.setOutput(Vas.Topup.INPUT_ERROR + "|" + "The input is invalid");
                tran.setResultCode(Vas.Topup.INPUT_ERROR);
                db.insertTopupLog(tran);
                return Vas.Topup.INPUT_ERROR + "|The input is invalid";
            }
//        step 2: validate ip
            String ip = getIpClient();
            if (ip == null || "".equals(ip.trim())) {
                logger.warn("Can not get ip for sub " + msisdn);
                tran.setDuration(System.currentTimeMillis() - timeStart);
                tran.setOutput(Vas.Topup.INPUT_ERROR + "|" + "The remote IP is not allowed");
                tran.setResultCode(Vas.Topup.INPUT_ERROR);
                db.insertTopupLog(tran);
                return Vas.Topup.INPUT_ERROR + "|The remote IP is not allowed";
            }
            tran.setIpRemote(ip);
            UserInfo user = authenticate(db, wsuser, wspassword, ip);
            if (user == null || user.getId() < 0) {
                logger.warn("Invalid account " + msisdn);
                tran.setDuration(System.currentTimeMillis() - timeStart);
                tran.setOutput(Vas.Topup.INPUT_ERROR + "|" + "Invalid accoun");
                tran.setResultCode(Vas.Topup.INPUT_ERROR);
                db.insertTopupLog(tran);
                return Vas.Topup.INPUT_ERROR + "|Invalid accoun";
            }
//            Check partner
            if (!partnerCode.equals(user.getPartnerCode())) {
                logger.warn("Invalid partnercode " + msisdn + " partner " + partnerCode);
                tran.setDuration(System.currentTimeMillis() - timeStart);
                tran.setOutput(Vas.Topup.INPUT_ERROR + "|" + "Invalid Partner");
                tran.setResultCode(Vas.Topup.INPUT_ERROR);
                db.insertTopupLog(tran);
                return Vas.Topup.INPUT_ERROR + "|Invalid Partner";
            }
//            Check fundCode
            if (!fundCode.equals(user.getFundCode())) {
                logger.warn("Invalid fundCode " + msisdn + " fundCode " + fundCode);
                tran.setDuration(System.currentTimeMillis() - timeStart);
                tran.setOutput(Vas.Topup.INPUT_ERROR + "|" + "Invalid fundCode");
                tran.setResultCode(Vas.Topup.INPUT_ERROR);
                db.insertTopupLog(tran);
                return Vas.Topup.INPUT_ERROR + "|Invalid fundCode";
            }
            //            Check UAT mode
            String accountUat = ResourceBundle.getBundle("vas").getString("list_account_for_uat");
            if (accountUat != null && accountUat.trim().length() > 0) {
                String[] listAccountUat = accountUat.split("\\|");
                boolean isUatMode = false;
                for (String acc : listAccountUat) {
                    if (acc.trim().equals(msisdn.trim())) {
                        isUatMode = true;
                        break;
                    }
                }
                if (!isUatMode) {
                    logger.warn("Msisdn not in list_account_for_uat " + msisdn + " accountUat " + accountUat);
                    tran.setDuration(System.currentTimeMillis() - timeStart);
                    tran.setOutput(Vas.Topup.INPUT_ERROR + "|" + "The msisdn not in UAT accounts");
                    tran.setResultCode(Vas.Topup.INPUT_ERROR);
                    db.insertTopupLog(tran);
                    return Vas.Topup.INPUT_ERROR + "|The msisdn not in UAT accounts";
                }
            }
//            Check sourceId
            if (!"1".equals(sourceId.trim()) && !"2".equals(sourceId.trim())
                    && !"3".equals(sourceId.trim()) && !"4".equals(sourceId.trim())) {
                logger.warn("Invalid sourceId " + msisdn + ", sourceId " + sourceId);
                tran.setDuration(System.currentTimeMillis() - timeStart);
                tran.setOutput(Vas.Topup.INPUT_ERROR + "|" + "Invalid sourceId");
                tran.setResultCode(Vas.Topup.INPUT_ERROR);
                db.insertTopupLog(tran);
                return Vas.Topup.INPUT_ERROR + "|Invalid sourceId";
            }
//            Check serviceType
            if (!"1".equals(serviceType.trim()) && !"2".equals(serviceType.trim())
                    && !"3".equals(serviceType.trim())) {
                logger.warn("Invalid serviceType " + msisdn + ", serviceType " + serviceType);
                tran.setDuration(System.currentTimeMillis() - timeStart);
                tran.setOutput(Vas.Topup.INPUT_ERROR + "|" + "Invalid serviceType");
                tran.setResultCode(Vas.Topup.INPUT_ERROR);
                db.insertTopupLog(tran);
                return Vas.Topup.INPUT_ERROR + "|Invalid serviceType";
            }
//        Validate money
            int money = 0;
            try {
                money = Integer.valueOf(amount);
                topupFee = money * rateTopupFee;
//                money = Math.abs(money);
                if (money > 100000 || money <= 0 || topupFee <= 0) { //Fix maximum is 50.000 mt
                    logger.warn("Can not recharge because the value is not in 0 - 50000 mt " + msisdn + " " + money);
                    tran.setDuration(System.currentTimeMillis() - timeStart);
                    tran.setOutput(Vas.Topup.INVALID_CHARGING_AMOUNT + "|" + "Money not in 0 - 50000 mt");
                    tran.setResultCode(Vas.Topup.INVALID_CHARGING_AMOUNT);
                    db.insertTopupLog(tran);
                    return Vas.Topup.INVALID_CHARGING_AMOUNT + "|Money not valid";
                }
            } catch (Exception e) {
                logger.warn("Invalid money " + msisdn + " amount " + amount + " " + e.toString());
                tran.setDuration(System.currentTimeMillis() - timeStart);
                tran.setOutput(Vas.Topup.INVALID_CHARGING_AMOUNT + "|" + "Invalid money");
                tran.setResultCode(Vas.Topup.INVALID_CHARGING_AMOUNT);
                db.insertTopupLog(tran);
                return Vas.Topup.INVALID_CHARGING_AMOUNT + "|Money invalid";
            }
//            Check signature
            String hashMsg = Encrypt.hashSHA1(msisdn + amount + requestId);
            if (!hashMsg.equals(signature)) {
                logger.warn("The input data has been modified " + msisdn + " signature " + signature);
                tran.setDuration(System.currentTimeMillis() - timeStart);
                tran.setOutput(Vas.Topup.SIGNATURE_INVALID + "|" + "Signature is invalid");
                tran.setResultCode(Vas.Topup.SIGNATURE_INVALID);
                db.insertTopupLog(tran);
                return Vas.Topup.SIGNATURE_INVALID + "|Signature is invalid";
            }
//            Huynq13 20180827 start add to support pay by ReferenceId for postpaid
            boolean isReference = false;
            if (Utils.checkIsReference(msisdn)) {
                isReference = true;
            } else if (!msisdn.startsWith(Common.config.countryCode)) {
                msisdn = Common.config.countryCode + msisdn;
            }
//            Check duplicate request_id
            if (db.checkSameRequestId(requestId, wsuser)) {
                logger.warn("The request_id already exist before " + msisdn + " requestId " + requestId);
                tran.setDuration(System.currentTimeMillis() - timeStart);
                tran.setOutput(Vas.Topup.FAIL_SAME_REQUESTID + "|" + "same requestId");
                tran.setResultCode(Vas.Topup.FAIL_SAME_REQUESTID);
                db.insertTopupLog(tran);
                return Vas.Topup.FAIL_SAME_REQUESTID + "|same requestId";
            }
            // chan lai  neu chua thanh toan het, tra ve ma loi
            String[] ls = db.checkDebitInWsuser();
            if (ls != null && ls.length > 0 && "1".equalsIgnoreCase(ls[0])) {
                logger.warn("The " + wsuser + " have debit with requestId " + requestId);
                tran.setDuration(System.currentTimeMillis() - timeStart);
                tran.setOutput(Vas.Topup.FAIL_HAVE_DEBIT + "|" + "have debit");
                tran.setResultCode(Vas.Topup.FAIL_HAVE_DEBIT);
                db.insertTopupLog(tran);
                return Vas.Topup.FAIL_HAVE_DEBIT + "|have debit";
            }
// Check postpaid mobile
            if ("2".equals(serviceType)) {
                logger.info(msisdn + " request servicetype for mobile postpaid " + serviceType);
//                Subscriber posSub = dbPost.getSubInfoMobile(msisdn, false);
//                if (posSub == null) {
//                    logger.warn("Fail get post sub " + msisdn);
//                    tran.setDuration(System.currentTimeMillis() - timeStart);
//                    tran.setOutput(Vas.Topup.DATABASE_ERROR + "|" + "Server is too busy");
//                    tran.setResultCode(Vas.Topup.DATABASE_ERROR);
//                    db.insertTopupLog(tran);
//                    return Vas.Topup.DATABASE_ERROR + "|Server is too busy";
//                }
                long contractId = 0;
                if (isReference) {
                    contractId = db.getContractMobileByRefer(msisdn);
                } else {
                    contractId = db.getContractMobile(msisdn, msisdn);
                }
                if (contractId > 0) {
                    logger.info("Start Topup for postpaid sub " + msisdn);
                    tran.setSubType(Vas.Constanst.POSTPAID + "");
//                Deduct fund
//                Integer topupFee = money * rateTopupFee / 100;
                    //        20180509 Huynq add to check payType for supporting pay by Airtime
                    String deductFund = "";
                    String orgEmolaRequestId = "";
                    ResponseWallet emolaResponse = null;
                    if (payType != null && payType.trim().equals("1")) {
                        deductFund = exch.adjustMoney(fundCode, "-" + money, "2000");
                        if (!"0".equals(deductFund)) {
                            logger.warn("Fail deduct emoney fundCode " + fundCode + " sub " + msisdn);
                            tran.setDuration(System.currentTimeMillis() - timeStart);
                            tran.setOutput(Vas.Topup.FAIL_PAID_FUND + "|" + "Fail to deduct fund");
                            tran.setResultCode(Vas.Topup.FAIL_PAID_FUND);
                            db.insertTopupLog(tran);
                            return Vas.Topup.FAIL_PAID_FUND + "|Fail to deduct fund";
                        } else {
                            logger.info("Deduct successfully emoney fundCode " + fundCode + " sub " + msisdn);
                            isDeductFund = true;
                        }
                    } else {
//                        20181122 start BOD approve not charge online Emola, chagne to deduct onetime when finish day and setup in make sale trans process
                        logger.info("Not Deduct Emola on transaction fundCode " + fundCode + " sub " + msisdn + " will be deducted when finish day with sum of amoun");
//                        emolaResponse = exch.chargeEmola(requestId, fundCode, topupFee + "", requestId + fundCode, wsuser, db, msisdn);
//                        if (emolaResponse != null) {
//                            deductFund = emolaResponse.getResponseCode();
//                            orgEmolaRequestId = emolaResponse.getRequestId();
//                        }
//                        if (!"01".equals(deductFund)) {
//                            logger.warn("Fail deduct emoney fundCode " + fundCode + " sub " + msisdn);
//                            tran.setDuration(System.currentTimeMillis() - timeStart);
//                            tran.setOutput(Vas.Topup.FAIL_PAID_FUND + "|" + "Fail to deduct fund");
//                            tran.setResultCode(Vas.Topup.FAIL_PAID_FUND);
//                            db.insertTopupLog(tran);
//                            return Vas.Topup.FAIL_PAID_FUND + "|Fail to deduct fund";
//                        } else {
//                            logger.info("Deduct successfully emoney fundCode " + fundCode + " sub " + msisdn);
//                            isDeductFund = true;
//                        }
                    }
//                    long contractId = db.getContractMobile(msisdn.substring(3), msisdn);
                    Session ss = com.viettel.vas.wsfw.database.IMSessionFactory.getSession();
                    ss.getTransaction().begin();
                    InvoiceListDAO invoiceListUtils = new InvoiceListDAO(ss);
                    List invoiceListList = invoiceListUtils.getAvailableInvoiceList(shopIdBillPay,
                            staffIdBillPay);
                    if (invoiceListList != null && invoiceListList.size() > 0) {
                        InvoiceListBean invoiceList = (InvoiceListBean) invoiceListList.get(0);
                        String blockNo = invoiceListUtils.getBlockNoFormatByBookType(invoiceList.getSerialNo(),
                                invoiceList.getBlockNo(), invoiceList.getCurrInvoiceNo());
                        String invoiceNumber = invoiceListUtils.getInvoiceNoFormatByBookType(invoiceList.getSerialNo(),
                                invoiceList.getBlockNo(), invoiceList.getCurrInvoiceNo());
                        long resultTopup;
//                    20180917 modify to separate collection_staff if payment by Bank, fix collection_staff_id = 1380839, BANK_PAYMENT
                        if (isReference) {
                            resultTopup = db.genBillPay(contractId, (long) money, "9", 1380839l, "2", invoiceList.getSerialNo(),
                                    blockNo, invoiceNumber, 0l, "1.1.1.1", Long.valueOf(groupId),
                                    "BANK_PAYMENT", msisdn);
                        } else {
                            resultTopup = db.genBillPay(contractId, (long) money, "9", staffIdBillPay, "2", invoiceList.getSerialNo(),
                                    blockNo, invoiceNumber, 0l, "1.1.1.1", Long.valueOf(groupId),
                                    staffCode, msisdn);
                        }
                        if (resultTopup == 0) {
                            logger.info("Payment call package pck_pay113 success for sub " + msisdn + " money " + money
                                    + " now update invoice to used, invoice_list_id" + invoiceList.getInvoiceListId());
                            long invoiceUsed = invoiceListUtils.updateInvoiceToUsing(shopIdBillPay, staffIdBillPay,
                                    invoiceList.getSerialNo(), invoiceList.getBlockNo(), invoiceList.getCurrInvoiceNo());
                            ss.getTransaction().commit();
                            ss.flush();
                            ss.close();
                            logger.info("Update invoice success for sub " + msisdn + " invoiceUsed " + invoiceUsed);
                            tran.setInvoiceListId(invoiceList.getInvoiceListId() + "|" + invoiceUsed);
                            tran.setDuration(System.currentTimeMillis() - timeStart);
                            tran.setOutput(Vas.Topup.SUCCESSFUL + "|" + "Payment success");
                            tran.setResultCode(Vas.Topup.SUCCESSFUL);
                            db.insertTopupLog(tran);
                            return Vas.Topup.SUCCESSFUL + "|The transaction was done successfully";
                        } else {
                            logger.warn("Fail to topup for sub " + msisdn);
                            //                Rollback fund                    
                            String rollback = "";
                            if (payType != null && payType.trim().equals("1")) {
                                rollback = exch.adjustMoney(fundCode, "" + money, "2000");
                                if (!"0".equals(rollback)) {
                                    logger.error("Fail rollback emoney fundCode " + fundCode + " sub " + msisdn);
                                    tran.setOutput(Vas.Topup.FAIL_RECHARGE + "|" + "Fail call package pck_pay113, and rollback fail");
                                } else {
                                    logger.info("Rollback successfully emoney fundCode " + fundCode + " sub " + msisdn);
                                    tran.setOutput(Vas.Topup.FAIL_RECHARGE + "|" + "Fail call package pck_pay113, and rollback success");
                                }
                            } else {
                                rollback = exch.revertTransaction(db, wsuser, orgEmolaRequestId, fundCode, money);
                                if (!"01".equals(rollback)) {
                                    logger.error("Fail rollback emoney fundCode " + fundCode + " sub " + msisdn);
                                    tran.setOutput(Vas.Topup.FAIL_RECHARGE + "|" + "Fail call package pck_pay113, and rollback fail");
                                } else {
                                    logger.info("Rollback successfully emoney fundCode " + fundCode + " sub " + msisdn);
                                    tran.setOutput(Vas.Topup.FAIL_RECHARGE + "|" + "Fail call package pck_pay113, and rollback success");
                                }
                            }
                            tran.setDuration(System.currentTimeMillis() - timeStart);
                            tran.setResultCode(Vas.Topup.FAIL_RECHARGE);
                            db.insertTopupLog(tran);
                            return Vas.Topup.FAIL_RECHARGE + "|Fail to topup";
                        }
                    } else {
                        logger.warn("Don't have invoice so can not make bill pay for sub " + msisdn);
                        //                Rollback fund                    
                        String rollback = "";
                        if (payType != null && payType.trim().equals("1")) {
                            rollback = exch.adjustMoney(fundCode, "" + money, "2000");
                            if (!"0".equals(rollback)) {
                                logger.error("Fail rollback emoney fundCode " + fundCode + " sub " + msisdn);
                                tran.setOutput(Vas.Topup.FAIL_RECHARGE + "|" + "Out of invoice, and rollback fail");
                            } else {
                                logger.info("Rollback successfully emoney fundCode " + fundCode + " sub " + msisdn);
                                tran.setOutput(Vas.Topup.FAIL_RECHARGE + "|" + "Out of invoice, and rollback success");
                            }
                        } else {
                            rollback = exch.revertTransaction(db, wsuser, orgEmolaRequestId, fundCode, money);
                            if (!"01".equals(rollback)) {
                                logger.error("Fail rollback emoney fundCode " + fundCode + " sub " + msisdn);
                                tran.setOutput(Vas.Topup.FAIL_RECHARGE + "|" + "Out of invoice, and rollback fail");
                            } else {
                                logger.info("Rollback successfully emoney fundCode " + fundCode + " sub " + msisdn);
                                tran.setOutput(Vas.Topup.FAIL_RECHARGE + "|" + "Out of invoice, and rollback success");
                            }
                        }
                        tran.setDuration(System.currentTimeMillis() - timeStart);
                        tran.setResultCode(Vas.Topup.FAIL_RECHARGE);
                        db.insertTopupLog(tran);
                        return Vas.Topup.FAIL_RECHARGE + "|Out of invoice";
                    }
                } else {
                    logger.warn(msisdn + " Not postpaid sub, while serviceType is for mobile postpaid " + serviceType);
                    tran.setDuration(System.currentTimeMillis() - timeStart);
                    tran.setOutput(Vas.Topup.NOT_EXISTS + "|" + "Not postpaid sub, while serviceType is for mobile postpaid");
                    tran.setResultCode(Vas.Topup.NOT_EXISTS);
                    db.insertTopupLog(tran);
                    return Vas.Topup.NOT_EXISTS + "|Not postpaid sub, while serviceType is for mobile postpaid";
                }
            }
            if ("1".equals(serviceType)) {
                logger.info(msisdn + " request servicetype for mobile prepaid " + serviceType);
                // Lay thong tin thue bao mobile tra truoc
                Subscriber preSub = dbPre.getSubInfoMobile(msisdn, false);
                if (preSub == null) {
                    logger.warn("Fail get pre sub " + msisdn);
                    tran.setDuration(System.currentTimeMillis() - timeStart);
                    tran.setOutput(Vas.Topup.DATABASE_ERROR + "|" + "Server is too busy");
                    tran.setResultCode(Vas.Topup.DATABASE_ERROR);
                    db.insertTopupLog(tran);
                    return Vas.Topup.DATABASE_ERROR + "|Server is too busy";
                }
                if (!preSub.getMsisdn().equals("NO_INFO_SUB")) {
                    //        20180428 Huynq add to check active status on OCS
//                    String activeStatus = exch.checkActiveStatusOnOCS(msisdn);
//                    if (activeStatus == null || activeStatus.trim().equals("1") || activeStatus.trim().equals("5")) {
//                        logger.warn("Not active so not support recharge " + msisdn);
//                        tran.setDuration(System.currentTimeMillis() - timeStart);
//                        tran.setOutput(Vas.ResultCode.INVALID_INPUT + "|" + "Sub not yet active");
//                        tran.setResultCode(Vas.ResultCode.INVALID_INPUT + "");
//                        db.insertTopupLog(tran);
//                        return Vas.Topup.NOT_EXISTS + "|Sub not yet active";
//                    }
                    tran.setSubType(Vas.Constanst.PREPAID + "");
                    logger.info("Topup for prepaid modbile subscriber: " + msisdn);
                    //                Deduct fund
//                    Integer topupFee = money * rateTopupFee / 100;
                    String deductFund = "";
                    String orgEmolaRequestId = "";
                    ResponseWallet emolaResponse = null;
                    if (payType != null && payType.trim().equals("1")) {
                        deductFund = exch.adjustMoney(fundCode, "-" + money, "2000");
                        if (!"0".equals(deductFund)) {
                            logger.warn("Fail deduct emoney fundCode " + fundCode + " sub " + msisdn);
                            tran.setDuration(System.currentTimeMillis() - timeStart);
                            tran.setOutput(Vas.Topup.FAIL_PAID_FUND + "|" + "Fail to deduct fund");
                            tran.setResultCode(Vas.Topup.FAIL_PAID_FUND);
                            db.insertTopupLog(tran);
                            return Vas.Topup.FAIL_PAID_FUND + "|Fail to deduct fund";
                        } else {
                            logger.info("Deduct successfully emoney fundCode " + fundCode + " sub " + msisdn);
                            isDeductFund = true;
                        }
                    } else {
//                        20181122 start BOD approve not charge online Emola, chagne to deduct onetime when finish day and setup in make sale trans process
                        logger.info("Not Deduct Emola on transaction fundCode " + fundCode + " sub " + msisdn + " will be deducted when finish day with sum of amoun");
//                        emolaResponse = exch.chargeEmola(requestId, fundCode, topupFee + "", requestId + fundCode, wsuser, db, msisdn);
//                        if (emolaResponse != null) {
//                            deductFund = emolaResponse.getResponseCode();
//                            orgEmolaRequestId = emolaResponse.getRequestId();
//                        }
//                        if (!"01".equals(deductFund)) {
//                            logger.warn("Fail deduct emoney fundCode " + fundCode + " sub " + msisdn);
//                            tran.setDuration(System.currentTimeMillis() - timeStart);
//                            tran.setOutput(Vas.Topup.FAIL_PAID_FUND + "|" + "Fail to deduct fund");
//                            tran.setResultCode(Vas.Topup.FAIL_PAID_FUND);
//                            db.insertTopupLog(tran);
//                            return Vas.Topup.FAIL_PAID_FUND + "|Fail to deduct fund";
//                        } else {
//                            logger.info("Deduct successfully emoney fundCode " + fundCode + " sub " + msisdn);
//                            isDeductFund = true;
//                        }                        
//                        20181122 end BOD approve not charge online Emola, chagne to deduct onetime when finish day and setup in make sale trans process                        
                    }
                    Topup result = new Topup();
//                    result = exch.topupPrePaid(msisdn, money + "", null);
                    result = exch.topupPrePaidPartner(msisdn, money + "");
                    if (result == null || !"0".equals(result.getErr())) {
                        logger.warn("Topup fail for pre sub " + msisdn + " money " + money);
                        //                Rollback fund                    
                        String rollback = "";
                        if (payType != null && payType.trim().equals("1")) {
                            rollback = exch.adjustMoney(fundCode, "" + money, "2000");
                            if (!"0".equals(rollback)) {
                                logger.error("Fail rollback emoney fundCode " + fundCode + " sub " + msisdn);
                                tran.setOutput(Vas.Topup.FAIL_RECHARGE + "|" + "Topup fail for pre sub, and rollback fail");
                            } else {
                                logger.info("Rollback successfully emoney fundCode " + fundCode + " sub " + msisdn);
                                tran.setOutput(Vas.Topup.FAIL_RECHARGE + "|" + "Topup fail for pre sub, and rollback success");
                            }
                        } else {
                            rollback = exch.revertTransaction(db, wsuser, orgEmolaRequestId, fundCode, money);
                            if (!"01".equals(rollback)) {
                                logger.error("Fail rollback emoney fundCode " + fundCode + " sub " + msisdn);
                                tran.setOutput(Vas.Topup.FAIL_RECHARGE + "|" + "Topup fail for pre sub, and rollback fail");
                            } else {
                                logger.info("Rollback successfully emoney fundCode " + fundCode + " sub " + msisdn);
                                tran.setOutput(Vas.Topup.FAIL_RECHARGE + "|" + "Topup fail for pre sub, and rollback success");
                            }
                        }
                        tran.setDuration(System.currentTimeMillis() - timeStart);
                        tran.setResultCode(Vas.Topup.FAIL_RECHARGE);
                        db.insertTopupLog(tran);
                        return Vas.Topup.FAIL_RECHARGE + "|Could not topup";
                    } else {
                        logger.warn("Topup success for pre sub " + msisdn + " money " + money);
                        tran.setDuration(System.currentTimeMillis() - timeStart);
                        tran.setOutput(Vas.Topup.SUCCESSFUL + "|" + "Topup success for pre sub");
                        tran.setResultCode(Vas.Topup.SUCCESSFUL);
                        db.insertTopupLog(tran);
                        return Vas.Topup.SUCCESSFUL + "|The transaction was done successfully";
                    }
                } else {
                    logger.warn(msisdn + " Not prepaid sub, while serviceType is for mobile prepaid " + serviceType);
                    tran.setDuration(System.currentTimeMillis() - timeStart);
                    tran.setOutput(Vas.Topup.NOT_EXISTS + "|" + "Not prepaid sub, while serviceType is for mobile prepaid");
                    tran.setResultCode(Vas.Topup.NOT_EXISTS);
                    db.insertTopupLog(tran);
                    return Vas.Topup.NOT_EXISTS + "|Not prepaid sub, while serviceType is for mobile prepaid";
                }
            }
            if ("3".equals(serviceType)) {
                logger.info(msisdn + " request servicetype for fixbroadband " + serviceType);
                // Lay thong tin thue bao ADSL
//                Subscriber adslSub = dbPost.getSubInfoADSL(msisdn);
//                if (adslSub == null) {
//                    logger.warn("Fail get adsl sub " + msisdn);
//                    tran.setDuration(System.currentTimeMillis() - timeStart);
//                    tran.setOutput(Vas.Topup.DATABASE_ERROR + "|" + "Server is too busy");
//                    tran.setResultCode(Vas.Topup.DATABASE_ERROR);
//                    db.insertTopupLog(tran);
//                    return Vas.Topup.DATABASE_ERROR + "|Server is too busy";
//                }
                long contractId = 0;
                if (isReference) {
                    contractId = db.getContractFbbByRefer(msisdn);
                } else {
                    contractId = db.getContractFbb(msisdn, msisdn);
                }
                if (contractId > 0) {
                    tran.setSubType(Vas.Constanst.FIX_BROADBAND + "");
//                    Huynq13 20180827 check FTTH prepaid remember if FTTH prepaid must set SUB_TYPE = 1
//                    Is FTTH prepaid if msisdn is account not isReference and normal in sub_adsl_ll_prepaid table
                    SubAdslLLPrepaid subPrepaid;
                    subPrepaid = db.checkFtthPrepaid(contractId, msisdn);
                    if (subPrepaid != null && subPrepaid.getExpireTime() != null) {
                        tran.setSubType(Vas.Constanst.FIX_BROADBAND_PREPAID + "");
                        logger.info(msisdn + " is prepaid FTTH, and now process for modifing expire_time ");
//                                    Calculate money fee range base on product and discount of range prepaid
                        ProductMonthlyFee product = db.getMonthlyFeeFtthPre(amount, subPrepaid.getNewProductCode());
                        if (product == null || product.getMonthlyFee() <= 0) {
                            logger.info(msisdn + " is prepaid FTTH, but can not get product monthly fee");
                            tran.setDuration(System.currentTimeMillis() - timeStart);
                            tran.setOutput(Vas.Topup.INPUT_ERROR + "|" + "is prepaid FTTH, but can not get product monthly fee");
                            tran.setResultCode(Vas.Topup.INPUT_ERROR);
                            db.insertTopupLog(tran);
                            String msg = msgFtthFail.replace("%ACCOUNT%", subPrepaid.getAccount());
                            db.sendSms(subPrepaid.getTelFax(), msg, "866123123");
                            db.insertActionAudit(subPrepaid.getId(), msisdn,
                                    "Extent fail because can not get product monthly fee account " + msisdn + " old time "
                                    + sdf.format(subPrepaid.getExpireTime())
                                    + " money " + money, subPrepaid.getSubId(), ip);
                            return Vas.Topup.INPUT_ERROR + "|is prepaid FTTH, but can not get product monthly fee";
                        }
                        long moneyOneMonth = product.getMonthlyFee();
//                                    Check less than one month fee
                        if (money < moneyOneMonth) {
                            logger.info(msisdn + " is prepaid FTTH, but money less than one month fee");
                            tran.setDuration(System.currentTimeMillis() - timeStart);
                            tran.setOutput(Vas.Topup.INVALID_CHARGING_AMOUNT + "|" + "is prepaid FTTH, but money less than one month fee");
                            tran.setResultCode(Vas.Topup.INVALID_CHARGING_AMOUNT);
                            db.insertTopupLog(tran);
                            String msg = msgFtthNotEnoughMoney.replace("%ACCOUNT%", subPrepaid.getAccount());
                            msg = msg.replace("%MONEY%", (moneyOneMonth - money) + "");
                            db.sendSms(subPrepaid.getTelFax(), msg, "866123123");
                            db.insertActionAudit(subPrepaid.getId(), msisdn,
                                    "Extent fail because money less than one month fee account " + msisdn + " old time "
                                    + sdf.format(subPrepaid.getExpireTime())
                                    + " money " + money, subPrepaid.getSubId(), ip);
                            return Vas.Topup.INVALID_CHARGING_AMOUNT + "|is prepaid FTTH, but money less than one month fee";
                        }
//                                    Get money for 3 month
                        double disCountPercent;
                        double noDiscountNoTax;
                        double amountDiscount;
                        double amountBeforeTax;
                        double tax;
                        double amount3Month;
                        double amount6Month;
                        double amount12Month;
                        long convertMonth = 0;
                        long remainMoney = 0;
                        long convertDay = 0;
                        disCountPercent = Double.parseDouble(product.getMapDiscount().get("3")) / 100;
                        noDiscountNoTax = moneyOneMonth * 3 / 1.17;
                        amountDiscount = noDiscountNoTax * disCountPercent; // Discount after tax
                        amountBeforeTax = noDiscountNoTax - amountDiscount;
                        tax = 0.17 * amountBeforeTax; //fix tax = 17%     
                        amount3Month = amountBeforeTax + tax;
//                                    Check money between one month and three month fee
                        if (money < amount3Month) {
                            convertMonth = money / moneyOneMonth;
                            remainMoney = money - moneyOneMonth * convertMonth;
                            convertDay = (remainMoney * 30) / moneyOneMonth;
                            logger.info(msisdn + " has money between fee of 1 month " + moneyOneMonth
                                    + " and fee of 3 month " + amount3Month + " convertMonth " + convertMonth
                                    + " remainMoney " + remainMoney + " convertDay " + convertDay);
                        } else {
                            disCountPercent = Double.parseDouble(product.getMapDiscount().get("6")) / 100;
                            noDiscountNoTax = moneyOneMonth * 6 / 1.17;
                            amountDiscount = noDiscountNoTax * disCountPercent; // Discount after tax
                            amountBeforeTax = noDiscountNoTax - amountDiscount;
                            tax = 0.17 * amountBeforeTax; //fix tax = 17%     
                            amount6Month = amountBeforeTax + tax;
                            if (money < amount6Month) {
                                convertMonth = 3;
                                remainMoney = money - (long) amount3Month;
                                convertDay = (remainMoney * 90) / (long) amount3Month;
                                logger.info(msisdn + " has money between fee of 3 month " + amount3Month
                                        + " and fee of 6 month " + amount6Month + " convertMonth " + convertMonth
                                        + " remainMoney " + remainMoney + " convertDay " + convertDay);
                            } else {
                                disCountPercent = Double.parseDouble(product.getMapDiscount().get("12")) / 100;
                                noDiscountNoTax = moneyOneMonth * 12 / 1.17;
                                amountDiscount = noDiscountNoTax * disCountPercent; // Discount after tax
                                amountBeforeTax = noDiscountNoTax - amountDiscount;
                                tax = 0.17 * amountBeforeTax; //fix tax = 17%     
                                amount12Month = amountBeforeTax + tax;
                                if (money < amount12Month) {
                                    convertMonth = 6;
                                    remainMoney = money - (long) amount6Month;
                                    convertDay = (remainMoney * 180) / (long) amount6Month;
                                    logger.info(msisdn + " has money between fee of 6 month " + amount6Month
                                            + " and fee of 12 month " + amount12Month + " convertMonth " + convertMonth
                                            + " remainMoney " + remainMoney + " convertDay " + convertDay);
                                } else {
                                    convertMonth = 12;
                                    remainMoney = money - (long) amount12Month;
                                    convertDay = (remainMoney * 360) / (long) amount12Month;
                                    logger.info(msisdn + " has money over fee of 12 month " + amount12Month
                                            + " convertMonth " + convertMonth
                                            + " remainMoney " + remainMoney + " convertDay " + convertDay);
                                }
                            }
                        }
//                                        open sub
                        String center = db.getCenter(msisdn);
                        if (center == null || center.trim().length() <= 0) {
                            logger.warn("Can not get center of account " + msisdn + " so set default center = 1");
                            center = "1";
                        }
                        String deductFund = "";
                        String orgEmolaRequestId = "";
                        ResponseWallet emolaResponse = null;
                        if (payType != null && payType.trim().equals("1")) {
                            deductFund = exch.adjustMoney(fundCode, "-" + money, "2000");
                            if (!"0".equals(deductFund)) {
                                logger.warn("Fail deduct emoney fundCode " + fundCode + " sub " + msisdn);
                                tran.setDuration(System.currentTimeMillis() - timeStart);
                                tran.setOutput(Vas.Topup.FAIL_PAID_FUND + "|" + "Fail to deduct fund");
                                tran.setResultCode(Vas.Topup.FAIL_PAID_FUND);
                                db.insertTopupLog(tran);
                                return Vas.Topup.FAIL_PAID_FUND + "|Fail to deduct fund";
                            } else {
                                logger.info("Deduct successfully emoney fundCode " + fundCode + " sub " + msisdn);
                                isDeductFund = true;
                            }
                        } else {
//                        20181122 start BOD approve not charge online Emola, chagne to deduct onetime when finish day and setup in make sale trans process
                            logger.info("Not Deduct Emola on transaction fundCode " + fundCode + " sub " + msisdn + " will be deducted when finish day with sum of amoun");
//                            emolaResponse = exch.chargeEmola(requestId, fundCode, topupFee + "", requestId + fundCode, wsuser, db, msisdn);
//                            if (emolaResponse != null) {
//                                deductFund = emolaResponse.getResponseCode();
//                                orgEmolaRequestId = emolaResponse.getRequestId();
//                            }
//                            if (!"01".equals(deductFund)) {
//                                logger.warn("Fail deduct emoney fundCode " + fundCode + " sub " + msisdn);
//                                tran.setDuration(System.currentTimeMillis() - timeStart);
//                                tran.setOutput(Vas.Topup.FAIL_PAID_FUND + "|" + "Fail to deduct fund");
//                                tran.setResultCode(Vas.Topup.FAIL_PAID_FUND);
//                                db.insertTopupLog(tran);
//                                return Vas.Topup.FAIL_PAID_FUND + "|Fail to deduct fund";
//                            } else {
//                                logger.info("Deduct successfully emoney fundCode " + fundCode + " sub " + msisdn + " money " + money);
//                                isDeductFund = true;
//                            }
                        }
                        String resActive = service.activeFBB(subPrepaid, center, msisdn);
                        if (resActive == null || !"0".equals(resActive)) {
                            logger.warn("Fail active FBB Prepaid sub " + msisdn);
                            tran.setDuration(System.currentTimeMillis() - timeStart);
                            tran.setResultCode(Vas.Topup.DATABASE_ERROR);
                            //                Rollback fund                    
                            String rollback = "";
                            if (payType != null && payType.trim().equals("1")) {
                                rollback = exch.adjustMoney(fundCode, "" + money, "2000");
                                if (!"0".equals(rollback)) {
                                    logger.error("Fail rollback emoney fundCode " + fundCode + " sub " + msisdn);
                                    tran.setOutput(Vas.Topup.DATABASE_ERROR + "|" + "Can not active for FBB prepaid sub and rollback fail");
                                } else {
                                    logger.info("Rollback successfully emoney fundCode " + fundCode + " sub " + msisdn);
                                    tran.setOutput(Vas.Topup.DATABASE_ERROR + "|" + "Can not active for FBB prepaid sub and rollback success");
                                }
                            } else {
                                rollback = exch.revertTransaction(db, wsuser, orgEmolaRequestId, fundCode, money);
                                if (!"01".equals(rollback)) {
                                    logger.error("Fail rollback emoney fundCode " + fundCode + " sub " + msisdn);
                                    tran.setOutput(Vas.Topup.DATABASE_ERROR + "|" + "Can not active for FBB prepaid sub and rollback fail");
                                } else {
                                    logger.info("Rollback successfully emoney fundCode " + fundCode + " sub " + msisdn);
                                    tran.setOutput(Vas.Topup.DATABASE_ERROR + "|" + "Can not active for FBB prepaid sub and rollback success");
                                }
                            }
                            db.insertTopupLog(tran);
                            String msg = msgFtthFail.replace("%ACCOUNT%", subPrepaid.getAccount());
                            db.sendSms(subPrepaid.getTelFax(), msg, "866123123");
                            db.insertActionAudit(subPrepaid.getId(), msisdn,
                                    "Extent fail because Can not active for account " + msisdn + " old time "
                                    + sdf.format(subPrepaid.getExpireTime())
                                    + " money " + money, subPrepaid.getSubId(), ip);
                            return Vas.Topup.DATABASE_ERROR + "|Can not active for FBB prepaid sub";
                        }
//                                        modify expire_time, block_time
                        Calendar cal = Calendar.getInstance();
                        Date today = new Date();
                        if (subPrepaid.getExpireTime().after(today)) {
                            logger.info("The expire_time after today so calculate time base on expiretime sub " + msisdn);
                            cal.setTime(subPrepaid.getExpireTime());
                        } else {
                            logger.info("The expire_time before today so calculate time base on today sub " + msisdn);
                            cal.setTime(today);
                        }
                        cal.add(Calendar.MONTH, (int) convertMonth);
                        cal.add(Calendar.DATE, (int) convertDay);
                        int resExtend = db.updateExpireFtthPre(subPrepaid.getId(), msisdn, cal.getTime());
                        if (resExtend <= 0) {
                            logger.warn("Fail update expire_time for FBB Prepaid sub " + msisdn);
                            tran.setDuration(System.currentTimeMillis() - timeStart);
                            tran.setResultCode(Vas.Topup.DATABASE_ERROR);
                            //                Rollback fund                    
                            String rollback = "";
                            if (payType != null && payType.trim().equals("1")) {
                                rollback = exch.adjustMoney(fundCode, "" + money, "2000");
                                if (!"0".equals(rollback)) {
                                    logger.error("Fail rollback emoney fundCode " + fundCode + " sub " + msisdn);
                                    tran.setOutput(Vas.Topup.DATABASE_ERROR + "|" + "Fail to extend for FBB prepaid sub and rollback fail");
                                } else {
                                    logger.info("Rollback successfully emoney fundCode " + fundCode + " sub " + msisdn);
                                    tran.setOutput(Vas.Topup.DATABASE_ERROR + "|" + "Fail to extend for FBB prepaid sub and rollback success");
                                }
                            } else {
                                rollback = exch.revertTransaction(db, wsuser, orgEmolaRequestId, fundCode, money);
                                if (!"01".equals(rollback)) {
                                    logger.error("Fail rollback emoney fundCode " + fundCode + " sub " + msisdn);
                                    tran.setOutput(Vas.Topup.DATABASE_ERROR + "|" + "Fail to extend for FBB prepaid sub and rollback fail");
                                } else {
                                    logger.info("Rollback successfully emoney fundCode " + fundCode + " sub " + msisdn);
                                    tran.setOutput(Vas.Topup.DATABASE_ERROR + "|" + "Fail to extend for FBB prepaid sub and rollback success");
                                }
                            }
                            db.insertTopupLog(tran);
                            String msg = msgFtthFail.replace("%ACCOUNT%", subPrepaid.getAccount());
                            db.sendSms(subPrepaid.getTelFax(), msg, "866123123");
                            db.insertActionAudit(subPrepaid.getId(), msisdn,
                                    "Extent fail because Can not update new expire time for account " + msisdn + " old time "
                                    + sdf.format(subPrepaid.getExpireTime())
                                    + " money " + money, subPrepaid.getSubId(), ip);
                            return Vas.Topup.DATABASE_ERROR + "|Fail to extend for FBB prepaid sub";
                        }
//                                        save action audit
                        db.insertActionAudit(subPrepaid.getId(), msisdn,
                                "Extent expire_time of FTTH Pre account " + msisdn + " old time "
                                + sdf.format(subPrepaid.getExpireTime())
                                + " new time " + sdf.format(cal.getTime()) + " money " + money + " addMonth " + convertMonth
                                + " addDay " + convertDay, subPrepaid.getSubId(), ip);
                        db.updateSubAdslLL(subPrepaid.getAccount());
                        tran.setDuration(System.currentTimeMillis() - timeStart);
                        tran.setOutput(Vas.Topup.SUCCESSFUL + "|"
                                + "Extent success for FTTH Pre " + " old time "
                                + sdf.format(subPrepaid.getExpireTime()) + " new time " + sdf.format(cal.getTime()));
                        tran.setResultCode(Vas.Topup.SUCCESSFUL);
                        db.insertTopupLog(tran);
                        String msg = msgFtthSuccess.replace("%ACCOUNT%", subPrepaid.getAccount());
                        msg = msg.replace("%EXPIRE%", sdf2.format(cal.getTime()));
                        db.sendSms(subPrepaid.getTelFax(), msg, "866123123");
                        return Vas.Topup.SUCCESSFUL + "|Extent success for FTTH Pre";
                    } else {
//                        If not prepaid must check have postpaid FBB account
                        if (db.checkFtthPospaid(contractId, msisdn)) {
                            logger.info("Topup for fixbroadband sub " + msisdn);
                            String deductFund = "";
                            String orgEmolaRequestId = "";
                            ResponseWallet emolaResponse = null;
                            if (payType != null && payType.trim().equals("1")) {
                                deductFund = exch.adjustMoney(fundCode, "-" + money, "2000");
                                if (!"0".equals(deductFund)) {
                                    logger.warn("Fail deduct emoney fundCode " + fundCode + " sub " + msisdn);
                                    tran.setDuration(System.currentTimeMillis() - timeStart);
                                    tran.setOutput(Vas.Topup.FAIL_PAID_FUND + "|" + "Fail to deduct fund");
                                    tran.setResultCode(Vas.Topup.FAIL_PAID_FUND);
                                    db.insertTopupLog(tran);
                                    return Vas.Topup.FAIL_PAID_FUND + "|Fail to deduct fund";
                                } else {
                                    logger.info("Deduct successfully emoney fundCode " + fundCode + " sub " + msisdn);
                                    isDeductFund = true;
                                }
                            } else {
//                        20181122 start BOD approve not charge online Emola, chagne to deduct onetime when finish day and setup in make sale trans process
                                logger.info("Not Deduct Emola on transaction fundCode " + fundCode + " sub " + msisdn + " will be deducted when finish day with sum of amoun");
//                                emolaResponse = exch.chargeEmola(requestId, fundCode, money + "", requestId + fundCode, wsuser, db, msisdn);
//                                if (emolaResponse != null) {
//                                    deductFund = emolaResponse.getResponseCode();
//                                    orgEmolaRequestId = emolaResponse.getRequestId();
//                                }
//                                if (!"01".equals(deductFund)) {
//                                    logger.warn("Fail deduct emoney fundCode " + fundCode + " sub " + msisdn);
//                                    tran.setDuration(System.currentTimeMillis() - timeStart);
//                                    tran.setOutput(Vas.Topup.FAIL_PAID_FUND + "|" + "Fail to deduct fund");
//                                    tran.setResultCode(Vas.Topup.FAIL_PAID_FUND);
//                                    db.insertTopupLog(tran);
//                                    return Vas.Topup.FAIL_PAID_FUND + "|Fail to deduct fund";
//                                } else {
//                                    logger.info("Deduct successfully emoney fundCode " + fundCode + " sub " + msisdn);
//                                    isDeductFund = true;
//                                }
                            }
//                    String newMsisdn = adslSub.getMsisdn(); //Huynq 20180810 add to support pay by subId
//                    long contractId = db.getContractMobile(newMsisdn.substring(3), msisdn); //Huynq 20180810 add to support pay by subId
                            Session ss = com.viettel.vas.wsfw.database.IMSessionFactory.getSession();
                            ss.getTransaction().begin();
                            InvoiceListDAO invoiceListUtils = new InvoiceListDAO(ss);
                            List invoiceListList = invoiceListUtils.getAvailableInvoiceList(shopIdBillPay,
                                    staffIdBillPay);
                            if (invoiceListList != null && invoiceListList.size() > 0) {
                                InvoiceListBean invoiceList = (InvoiceListBean) invoiceListList.get(0);
                                String blockNo = invoiceListUtils.getBlockNoFormatByBookType(invoiceList.getSerialNo(),
                                        invoiceList.getBlockNo(), invoiceList.getCurrInvoiceNo());
                                String invoiceNumber = invoiceListUtils.getInvoiceNoFormatByBookType(invoiceList.getSerialNo(),
                                        invoiceList.getBlockNo(), invoiceList.getCurrInvoiceNo());
                                long resultTopup;
//                    20180917 modify to separate collection_staff if payment by Bank, fix collection_staff_id = 1380839, BANK_PAYMENT
                                if (isReference) {
                                    resultTopup = db.genBillPay(contractId, (long) money, "9", 1380839l, "2", invoiceList.getSerialNo(),
                                            blockNo, invoiceNumber, 0l, "1.1.1.1", Long.valueOf(groupId),
                                            "BANK_PAYMENT", msisdn);
                                } else {
                                    resultTopup = db.genBillPay(contractId, (long) money, "9", staffIdBillPay, "2", invoiceList.getSerialNo(),
                                            blockNo, invoiceNumber, 0l, "1.1.1.1", Long.valueOf(groupId),
                                            staffCode, msisdn); //Huynq 20180810 add to support pay by subId
                                }
                                if (resultTopup == 0) {
                                    logger.info("Payment call package pck_pay113 success for sub " + msisdn + " money " + money
                                            + " now update invoice to used, invoice_list_id" + invoiceList.getInvoiceListId());
                                    long invoiceUsed = invoiceListUtils.updateInvoiceToUsing(shopIdBillPay, staffIdBillPay,
                                            invoiceList.getSerialNo(), invoiceList.getBlockNo(), invoiceList.getCurrInvoiceNo());
                                    ss.getTransaction().commit();
                                    ss.flush();
                                    ss.close();
                                    logger.info("Update invoice success for sub " + msisdn + " invoiceUsed " + invoiceUsed);
                                    tran.setInvoiceListId(invoiceList.getInvoiceListId() + "|" + invoiceUsed);
                                    tran.setDuration(System.currentTimeMillis() - timeStart);
                                    tran.setOutput(Vas.Topup.SUCCESSFUL + "|" + "Payment success");
                                    tran.setResultCode(Vas.Topup.SUCCESSFUL);
                                    db.insertTopupLog(tran);
                                    return Vas.Topup.SUCCESSFUL + "|The transaction was done successfully";
                                } else {
                                    logger.warn("Fail to topup for sub " + msisdn);
                                    //                Rollback fund                    
                                    String rollback = "";
                                    if (payType != null && payType.trim().equals("1")) {
                                        rollback = exch.adjustMoney(fundCode, "" + money, "2000");
                                        if (!"0".equals(rollback)) {
                                            logger.error("Fail rollback emoney fundCode " + fundCode + " sub " + msisdn);
                                            tran.setOutput(Vas.Topup.FAIL_RECHARGE + "|" + "Fail call package pck_pay113, and rollback fail");
                                        } else {
                                            logger.info("Rollback successfully emoney fundCode " + fundCode + " sub " + msisdn);
                                            tran.setOutput(Vas.Topup.FAIL_RECHARGE + "|" + "Fail call package pck_pay113, and rollback success");
                                        }
                                    } else {
                                        rollback = exch.revertTransaction(db, wsuser, orgEmolaRequestId, fundCode, money);
                                        if (!"01".equals(rollback)) {
                                            logger.error("Fail rollback emoney fundCode " + fundCode + " sub " + msisdn);
                                            tran.setOutput(Vas.Topup.FAIL_RECHARGE + "|" + "Fail call package pck_pay113, and rollback fail");
                                        } else {
                                            logger.info("Rollback successfully emoney fundCode " + fundCode + " sub " + msisdn);
                                            tran.setOutput(Vas.Topup.FAIL_RECHARGE + "|" + "Fail call package pck_pay113, and rollback success");
                                        }
                                    }
                                    tran.setDuration(System.currentTimeMillis() - timeStart);
                                    tran.setResultCode(Vas.Topup.FAIL_RECHARGE);
                                    db.insertTopupLog(tran);
                                    return Vas.Topup.FAIL_RECHARGE + "|Fail to topup";
                                }
                            } else {
                                logger.warn("Don't have invoice so can not make bill pay for sub " + msisdn);
                                //                Rollback fund                    
                                String rollback = "";
                                if (payType != null && payType.trim().equals("1")) {
                                    rollback = exch.adjustMoney(fundCode, "" + money, "2000");
                                    if (!"0".equals(rollback)) {
                                        logger.error("Fail rollback emoney fundCode " + fundCode + " sub " + msisdn);
                                        tran.setOutput(Vas.Topup.FAIL_RECHARGE + "|" + "Out of invoice, and rollback fail");
                                    } else {
                                        logger.info("Rollback successfully emoney fundCode " + fundCode + " sub " + msisdn);
                                        tran.setOutput(Vas.Topup.FAIL_RECHARGE + "|" + "Out of invoice, and rollback success");
                                    }
                                } else {
                                    rollback = exch.revertTransaction(db, wsuser, orgEmolaRequestId, fundCode, money);
                                    if (!"01".equals(rollback)) {
                                        logger.error("Fail rollback emoney fundCode " + fundCode + " sub " + msisdn);
                                        tran.setOutput(Vas.Topup.FAIL_RECHARGE + "|" + "Out of invoice, and rollback fail");
                                    } else {
                                        logger.info("Rollback successfully emoney fundCode " + fundCode + " sub " + msisdn);
                                        tran.setOutput(Vas.Topup.FAIL_RECHARGE + "|" + "Out of invoice, and rollback success");
                                    }
                                }
                                tran.setDuration(System.currentTimeMillis() - timeStart);
                                tran.setResultCode(Vas.Topup.FAIL_RECHARGE);
                                db.insertTopupLog(tran);
                                return Vas.Topup.FAIL_RECHARGE + "|Out of invoice";
                            }
                        } else {
                            logger.warn(msisdn + " not prepaid and postpaid FBB contractId " + contractId);
                            tran.setDuration(System.currentTimeMillis() - timeStart);
                            tran.setOutput(Vas.Topup.NOT_EXISTS + "|" + "not prepaid and postpaid FBB");
                            tran.setResultCode(Vas.Topup.NOT_EXISTS);
                            db.insertTopupLog(tran);
                            return Vas.Topup.NOT_EXISTS + "|not prepaid and postpaid FBB";
                        }
                    }
                } else {
                    logger.warn(msisdn + " Not adsl, ftth, leaseline sub, while serviceType is for fixbroadband " + serviceType);
                    tran.setDuration(System.currentTimeMillis() - timeStart);
                    tran.setOutput(Vas.Topup.NOT_EXISTS + "|" + "Not fixbroadband sub, while serviceType is for fixbroadband");
                    tran.setResultCode(Vas.Topup.NOT_EXISTS);
                    db.insertTopupLog(tran);
                    return Vas.Topup.NOT_EXISTS + "|Not fixbroadband sub, while serviceType is for fixbroadband";
                }
            }
            logger.info("No infomation for charge " + msisdn);
            tran.setDuration(System.currentTimeMillis() - timeStart);
            tran.setOutput(Vas.Topup.NOT_EXISTS + "|" + "The isdn does not exists");
            tran.setResultCode(Vas.Topup.NOT_EXISTS);
            db.insertTopupLog(tran);
            return Vas.Topup.NOT_EXISTS + "|The isdn does not exists";
        } catch (Exception e) {
            logger.error("[!!!] Error topup for sub " + msisdn, e);
            logger.error(AppManager.logException(timeStart, e));
            if (isDeductFund) {
                try {
                    //                Rollback fund                    
                    String rollback = "";
                    if (payType != null && payType.trim().equals("1")) {
                        rollback = exch.adjustMoney(fundCode, amount, "2000");
                        if (!"0".equals(rollback)) {
                            logger.error("Fail rollback emoney fundCode " + fundCode + " sub " + msisdn);
                            tran.setOutput(Vas.Topup.EXCEPTION + "|" + "Exception, and rollback fail");
                        } else {
                            logger.error("Rollback successfully emoney fundCode " + fundCode + " sub " + msisdn);
                            tran.setOutput(Vas.Topup.EXCEPTION + "|" + "Exception, and rollback success");
                        }
                    } else {
//                        rollback = exch.revertTransaction(db, wsuser, orgEmolaRequestId, fundCode, Long.valueOf(amount));
                        if (!"01".equals(rollback)) {
                            logger.error("Can not rollback emoney fundCode " + fundCode + " sub " + msisdn);
                            tran.setOutput(Vas.Topup.EXCEPTION + "|" + "Exception, and rollback fail");
                        } else {
                            logger.error("Rollback successfully emoney fundCode " + fundCode + " sub " + msisdn);
                            tran.setOutput(Vas.Topup.EXCEPTION + "|" + "Exception, and rollback success");
                        }
                    }
                } catch (Exception ex) {
                    logger.error("Exception when rollback fund " + fundCode + " for sub " + msisdn, ex);
                    tran.setOutput(Vas.Topup.EXCEPTION + "|" + "Exception when rollback, and rollback fail");
                }
            } else {
                tran.setOutput(Vas.Topup.EXCEPTION + "|" + "Exception, and no need rollback");
            }
            tran.setDuration(System.currentTimeMillis() - timeStart);
            tran.setResultCode(Vas.Topup.EXCEPTION);
            db.insertTopupLog(tran);
            return Vas.Topup.EXCEPTION + "|Unexpected exception";
        }
    }

//      return status|desc|money
//      - status (mandatory): 0 success, 1 fail, 2 not exist
//      - description (optional) 
//      - money (optional)
    @WebMethod(operationName = "viewTransHis")
    public String viewTransHis(
            @WebParam(name = "requestId") String requestId,
            @WebParam(name = "userName") String userName,
            @WebParam(name = "passWord") String passWord) throws Exception {
        logger.info("Start viewTransHis for requestId " + requestId + " userName " + userName);
//        step 1: validate input
        if (requestId == null || "".equals(requestId.trim())
                || userName == null || "".equals(userName.trim())
                || passWord == null || "".equals(passWord.trim())) {
            logger.warn("Invalid input to viewTransHis requestId " + requestId);
            return "3|Input is invalid|0";
        }
//        step 2: validate ip
        String ip = getIpClient();
        if (ip == null || "".equals(ip.trim())) {
            logger.warn("Can not get ip for requestId " + requestId);
            return "4|Ip is invalid|0";
        }
        UserInfo user = authenticate(db, userName, passWord, ip);
        if (user == null || user.getId() < 0) {
            logger.warn("Invalid account requestId " + requestId);
            return "5|Account is invalid|0";
        }
        String result = db.checkTopupHis(requestId);
        logger.warn("Result for requestId " + result);
        return result;
    }

    @WebMethod(operationName = "viewBalance")
    public String viewBalance(
            @WebParam(name = "fundCode") String fundCode,
            @WebParam(name = "userName") String userName,
            @WebParam(name = "passWord") String passWord) throws Exception {
        logger.info("Start viewBalance for userName " + userName);
//        step 1: validate input
        if (fundCode == null || "".equals(fundCode.trim())
                || userName == null || "".equals(userName.trim())
                || passWord == null || "".equals(passWord.trim())) {
            logger.warn("Invalid input to viewBalance userName " + userName);
            return "Input is invalid";
        }
//        step 2: validate ip
        String ip = getIpClient();
        if (ip == null || "".equals(ip.trim())) {
            logger.warn("Can not get ip for userName " + userName);
            return "Ip is invalid";
        }
//        UserInfo user = authenticate(db, userName, passWord, ip);
//        if (user == null || user.getId() < 0) {
//            logger.warn("Invalid account userName " + userName);
//            return "Account is invalid";
//        }
        String result = exch.viewEpay(fundCode);
        logger.warn("Result for requestId " + result);
        return result;
    }

    @WebMethod(operationName = "makeSaleTrans")
    public String makeSaleTrans(
            @WebParam(name = "client") String client,
            @WebParam(name = "money") String money,
            @WebParam(name = "custName") String custName,
            @WebParam(name = "address") String address,
            @WebParam(name = "nuit") String nuit,
            @WebParam(name = "userName") String userName,
            @WebParam(name = "passWord") String passWord) throws Exception {
        logger.info("Start makeSaleTrans for userName " + userName + " client " + client + " money " + money + " custName " + custName + " address " + address + " nuit " + nuit);
//        step 1: validate input
        if (client == null || "".equals(client.trim())
                || money == null || "".equals(money.trim())
                || userName == null || "".equals(userName.trim())
                || passWord == null || "".equals(passWord.trim())) {
            logger.warn("Invalid input to makeSaleTrans userName " + userName + " client " + client + " money " + money);
            return "Input is invalid";
        }
//        step 2: validate ip
        String ip = getIpClient();
        if (ip == null || "".equals(ip.trim())) {
            logger.warn("Can not get ip for userName " + userName);
            return "Ip is invalid";
        }
//        UserInfo user = authenticate(db, userName, passWord, ip);
//        if (user == null || user.getId() < 0) {
//            logger.warn("Invalid account userName " + userName);
//            return "Account is invalid";
//        }
//        int result = db.insertMakeTransInput(client, money);
        int result = db.insertMakeTransInputWithName(client, money, custName, address, nuit);
        logger.warn("Result for requestId " + result);
        return result + "";
    }
}