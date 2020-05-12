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
import java.util.ResourceBundle;

/**
 *
 * @author minhnh@viettel.com.vn
 * @since Jun 4, 2013
 * @version 1.0
 */
@WebService
public class RechargeNoCard extends WebserviceAbstract {

    private Exchange exch;
    DbProcessor db;
    DbPre dbPre;
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

    public RechargeNoCard() {
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

    @WebMethod(operationName = "Recharge")
    public ResponseRecharge Recharge(
            @WebParam(name = "pincode") String pincode,
            @WebParam(name = "isdn") String isdn,
            @WebParam(name = "userName") String wsuser,
            @WebParam(name = "passWord") String wspassword) throws Exception {
        ResponseRecharge response = new ResponseRecharge();
//        pincode is money because meuway application use invalid this name, so don want to rename now, continue use this name.
        logger.info("Start process get Recharge for sub " + isdn + " client " + wsuser);
        long timeStart = System.currentTimeMillis();
        TransLog tran = new TransLog();
        tran.setClient(wsuser);
        tran.setIsdn(isdn);
        tran.setMoney(pincode);
        tran.setSubType(Vas.Topup.SUB_TYPE_MOBILE_PRE);
        tran.setTransType(Vas.Topup.TRANS_TYPE_RECHARGE);
        tran.setWsCode("Recharge");
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
//        Validate money
        try {
            Double amount = 0d;
            amount = Double.parseDouble(pincode);
            amount = Math.abs(amount);
            if (amount > 10000) { //Fix maximum is 10.000 mt
                logger.warn("Can not recharge due to the value is too big " + isdn + " " + amount);
                response.setErrorCode(Vas.ResultCode.INVALID_INPUT);
                response.setDescription("INVALID_INPUT");
                tran.setDuration(System.currentTimeMillis() - timeStart);
                tran.setOutput(Vas.ResultCode.INVALID_INPUT + "|" + "Money too big");
                tran.setResultCode(Vas.ResultCode.INVALID_INPUT + "");
                db.insertTopupLog(tran);
                return response;
            }
        } catch (Exception e) {
            logger.warn("Invalid money " + isdn + " " + e.toString());
            response.setErrorCode(Vas.ResultCode.INVALID_INPUT);
            response.setDescription("INVALID_INPUT");
            tran.setDuration(System.currentTimeMillis() - timeStart);
            tran.setOutput(Vas.ResultCode.INVALID_INPUT + "|" + "Invalid money");
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
        tran.setSubType(Vas.Constanst.PREPAID + "");
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
//                            20180206 Huynq13 change to make exception for topup get special promotion
        String command = null;
        try {
            command = ResourceBundle.getBundle("vas").getString(wsuser.toLowerCase());
        } catch (Exception e) {
            logger.info("User " + wsuser + " do not get special promotion when topup " + isdn + " " + e.toString());
            command = null;
        }
        Topup topup = exch.topupPrePaid(isdn, pincode, command);
        if (topup.getErr().equals("0")) {
            logger.info("Recharge success for sub " + isdn + " money " + topup.getBalance());
            response.setBalance(topup.getBalance());
            response.setFaceValue(topup.getFaceVakue());
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
            logger.info("Recharge fail " + isdn);
            response.setErrorCode(Common.ErrCode.INVALID_CARD);
            response.setDescription("RECHARGE FAIL");
            tran.setDuration(System.currentTimeMillis() - timeStart);
            tran.setOutput(Vas.ResultCode.INVALID_CARD + "|" + "RECHARGE FAIL");
            tran.setResultCode(Vas.ResultCode.INVALID_CARD + "");
            db.insertTopupLog(tran);
        }

        return response;
    }
}
