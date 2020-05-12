/*
 * Copyright (C) 2010 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.vas.wsfw.services;

import com.viettel.vas.util.ExchangeClientChannel;
import com.viettel.vas.wsfw.common.WebserviceAbstract;
import javax.jws.WebMethod;
import javax.jws.WebParam;
import javax.jws.WebService;
import com.viettel.vas.wsfw.common.Common;
import com.viettel.vas.wsfw.common.Vas;
import com.viettel.vas.wsfw.database.DbPre;
import com.viettel.vas.wsfw.database.DbProcessor;
import com.viettel.vas.wsfw.object.ResponseRecharge;
import com.viettel.vas.wsfw.object.Subscriber;
import com.viettel.vas.wsfw.object.Topup;
import com.viettel.vas.wsfw.object.UserInfo;
import com.viettel.vas.wsfw.object.TransLog;
import com.viettel.data.ws.utils.Exchange;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

/**
 *
 * @author minhnh@viettel.com.vn
 * @since Jun 4, 2013
 * @version 1.0
 */
@WebService
public class RechargeWithCard extends WebserviceAbstract {

    private Exchange exch;
    DbProcessor db;
    DbPre dbPre;
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

    public RechargeWithCard() {
        super("Recharge");
        try {
            db = new DbProcessor("dbtopup", logger);
            dbPre = new DbPre("cm_pre", logger);
            exch = new Exchange(ExchangeClientChannel.getInstance("../etc/exchange_client.cfg").getInstanceChannel(), logger);
        } catch (Exception ex) {
            logger.error("Fail init webservice Recharge");
            logger.error(ex);
        }
    }

    @WebMethod(operationName = "RechargeWithCard")
    public ResponseRecharge Recharge(
            @WebParam(name = "pincode") String pincode,
            @WebParam(name = "isdn") String isdn,
            @WebParam(name = "userName") String wsuser,
            @WebParam(name = "passWord") String wspassword) throws Exception {
        ResponseRecharge response = new ResponseRecharge();
//        pincode is money because meuway application use invalid this name, so don want to rename now, continue use this name.
        logger.info("Start process get RechargeWithCard for sub " + isdn + " client " + wsuser + " pin " + pincode);
        long timeStart = System.currentTimeMillis();
        TransLog tran = new TransLog();
        tran.setClient(wsuser);
        tran.setIsdn(isdn);
        tran.setPinCode(pincode);
        tran.setSubType(Vas.Topup.SUB_TYPE_MOBILE_PRE);
        tran.setTransType(Vas.Topup.TRANS_TYPE_RECHARGE);
        tran.setWsCode("RechargeWithCard");
        tran.setInput(isdn + "|" + pincode);
        tran.setStartTime(new Timestamp(timeStart));
//        step 1: validate input
        if (isdn == null || "".equals(isdn.trim())
                || pincode == null || "".equals(pincode.trim())
                || wsuser == null || "".equals(wsuser.trim())
                || wspassword == null || "".equals(wspassword.trim())) {
            logger.warn("Invalid input sub " + isdn);
            response.setErrorCode(Vas.ResultCode.INVALID_INPUT);
            response.setDescription("INVALID_INPUT");
            tran.setDuration(System.currentTimeMillis() - timeStart);
            tran.setOutput(Vas.ResultCode.INVALID_INPUT + "|" + "INVALID_INPUT");
            tran.setResultCode(Vas.ResultCode.INVALID_INPUT + "");
            db.insertTopupLog(tran);
            return response;
        }
//        step 2: validate ip
        String ip = getIpClient();
        if (ip == null || "".equals(ip.trim())) {
            logger.warn("Can not get ip for sub " + isdn);
            response.setErrorCode(Vas.ResultCode.FAIL_GET_IP);
            response.setDescription("FAIL_GET_IP");
            tran.setDuration(System.currentTimeMillis() - timeStart);
            tran.setOutput(Vas.ResultCode.FAIL_GET_IP + "|" + "FAIL_GET_IP");
            tran.setResultCode(Vas.ResultCode.FAIL_GET_IP + "");
            db.insertTopupLog(tran);
            return response;
        }
        tran.setIpRemote(ip);
        UserInfo user = authenticate(db, wsuser, wspassword, ip);
        if (user == null || user.getId() < 0) {
            logger.warn("Invalid account " + isdn);
            response.setErrorCode(Vas.ResultCode.WRONG_ACCOUNT_IP);
            response.setDescription("WRONG_ACCOUNT_IP");
            tran.setDuration(System.currentTimeMillis() - timeStart);
            tran.setOutput(Vas.ResultCode.WRONG_ACCOUNT_IP + "|" + "WRONG_ACCOUNT_IP");
            tran.setResultCode(Vas.ResultCode.WRONG_ACCOUNT_IP + "");
            db.insertTopupLog(tran);
            return response;
        }
        // Lay thong tin thue bao mobile tra truoc
        Subscriber preSub = dbPre.getSubInfoMobile(isdn, false);
        if (preSub == null) {
            logger.warn("Fail get pre sub " + isdn);
            response.setErrorCode(Vas.ResultCode.INVALID_INPUT);
            response.setDescription("Fail get pre sub");
            tran.setDuration(System.currentTimeMillis() - timeStart);
            tran.setOutput(Vas.ResultCode.INVALID_INPUT + "|" + "Fail get pre sub");
            tran.setResultCode(Vas.ResultCode.INVALID_INPUT + "");
            db.insertTopupLog(tran);
            return response;
        }
        if (preSub.getMsisdn().equals("NO_INFO_SUB")) {
            logger.warn("NO_INFO_SUB " + isdn);
            response.setErrorCode(Vas.ResultCode.INVALID_INPUT);
            response.setDescription("NO_INFO_SUB");
            tran.setDuration(System.currentTimeMillis() - timeStart);
            tran.setOutput(Vas.ResultCode.INVALID_INPUT + "|" + "NO_INFO_SUB");
            tran.setResultCode(Vas.ResultCode.INVALID_INPUT + "");
            db.insertTopupLog(tran);
            return response;
        }
        //        20180428 Huynq add to check active status on OCS
        String activeStatus = exch.checkActiveStatusOnOCS(isdn);
        if (activeStatus == null || activeStatus.trim().equals("1") || activeStatus.trim().equals("5")) {
            logger.warn("Not active so not support recharge " + isdn);
            response.setErrorCode(Vas.ResultCode.INVALID_INPUT);
            response.setDescription("Sub not yet active");
            tran.setDuration(System.currentTimeMillis() - timeStart);
            tran.setOutput(Vas.ResultCode.INVALID_INPUT + "|" + "Sub not yet active");
            tran.setResultCode(Vas.ResultCode.INVALID_INPUT + "");
            db.insertTopupLog(tran);
            return response;
        }
        tran.setSubType(Vas.Constanst.PREPAID + "");
//        Lock card
        String resultLock = exch.lockCard(isdn, pincode);
        if (resultLock == null || resultLock.trim().length() <= 0) {
            logger.warn("Lock card fail for sub " + isdn + " pincode " + pincode);
            response.setErrorCode(Vas.ResultCode.INVALID_CARD);
            response.setDescription("INVALID_CARD");
            tran.setDuration(System.currentTimeMillis() - timeStart);
            tran.setOutput(Vas.ResultCode.INVALID_CARD + "|" + "Fail to lock card");
            tran.setResultCode(Vas.ResultCode.INVALID_CARD + "");
            db.insertTopupLog(tran);
            return response;
        }
        tran.setMoney(resultLock);
        try {
            Double amount = 0d;
            amount = Double.parseDouble(resultLock);
            amount = Math.abs(amount);
            if (amount > 501) { //Fix maximum is 1000 mt
                logger.warn("Invalid card, value greater the maximum card " + isdn + " " + amount);
                response.setErrorCode(Vas.ResultCode.INVALID_CARD);
                response.setDescription("INVALID_CARD");
                tran.setDuration(System.currentTimeMillis() - timeStart);
                tran.setOutput(Vas.ResultCode.INVALID_CARD + "|" + "Card invalid, greater maximum card");
                tran.setResultCode(Vas.ResultCode.INVALID_CARD + "");
                db.insertTopupLog(tran);
                return response;
            }
        } catch (Exception e) {
            logger.warn("Invalid card, can not convert to get money of card " + isdn + " " + e.toString());
            response.setErrorCode(Vas.ResultCode.INVALID_CARD);
            response.setDescription("INVALID_INPUT");
            tran.setDuration(System.currentTimeMillis() - timeStart);
            tran.setOutput(Vas.ResultCode.INVALID_CARD + "|" + "Invalid card, can not convert to get money of card");
            tran.setResultCode(Vas.ResultCode.INVALID_CARD + "");
            db.insertTopupLog(tran);
            return response;
        }
        Topup topup = exch.topupPrePaid(isdn, resultLock, null);
        if (topup.getErr().equals("0")) {
            logger.info("RechargeWithCard success for sub " + isdn + " money " + topup.getBalance());
            response.setBalance(topup.getBalance());
            response.setFaceValue(resultLock);
            response.setErrorCode(Common.ErrCode.SUCCESS);
            response.setDescription("SUCCESS");
            tran.setDuration(System.currentTimeMillis() - timeStart);
            tran.setOutput(Vas.ResultCode.SUCCESS + "|" + "RECHARGE SUCCESS");
            tran.setResultCode(Vas.ResultCode.SUCCESS + "");
            db.insertTopupLog(tran);
        } else if (topup.getErr().equals("102010690") || topup.getErr().equals("102010671")) {
            logger.info("Rechage fail, invalid card " + isdn);
            response.setErrorCode(Common.ErrCode.INVALID_CARD);
            response.setDescription("INVALID CARD");
            tran.setDuration(System.currentTimeMillis() - timeStart);
            tran.setOutput(Vas.ResultCode.INVALID_CARD + "|" + "INVALID CARD");
            tran.setResultCode(Vas.ResultCode.INVALID_CARD + "");
            db.insertTopupLog(tran);
        } else {
            logger.error("Recharge fail " + isdn);
            response.setErrorCode(Common.ErrCode.INVALID_CARD);
            response.setDescription("RECHARGE FAIL");
            tran.setDuration(System.currentTimeMillis() - timeStart);
            tran.setOutput(Vas.ResultCode.INVALID_CARD + "|" + "RECHARGE FAIL, let check log");
            tran.setResultCode(Vas.ResultCode.INVALID_CARD + "");
            db.insertTopupLog(tran);
        }
        return response;
    }
}
