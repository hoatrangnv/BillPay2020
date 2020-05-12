/*
 * Copyright (C) 2010 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.vas.wsfw.services;

import com.viettel.vas.util.SmsWsManager;
import com.viettel.vas.wsfw.common.WebserviceAbstract;
import com.viettel.vas.wsfw.common.WebserviceManager;
import javax.jws.WebMethod;
import javax.jws.WebParam;
import javax.jws.WebService;
import com.viettel.vas.wsfw.common.Common;
import com.viettel.vas.wsfw.common.Vas;
import com.viettel.vas.wsfw.database.DbProcessor;
import com.viettel.vas.wsfw.object.ResponseSendMT;
import com.viettel.vas.wsfw.object.UserInfo;

/**
 *
 * @author minhnh@viettel.com.vn
 * @since Jun 4, 2013
 * @version 1.0
 */
@WebService
public class SendMT extends WebserviceAbstract {

    private SmsWsManager smsWsManager;
    DbProcessor db;

    public SendMT() throws Exception {
        super("SendMT");
        smsWsManager = SmsWsManager.getInstance();
        db = new DbProcessor("dbtopup", logger);
    }

    @WebMethod(operationName = "SendMT")
    public ResponseSendMT SendMT(
            @WebParam(name = "message") String message,
            @WebParam(name = "isdn") String isdn,
            @WebParam(name = "shortcode") String shortCode,
            @WebParam(name = "userName") String wsuser,
            @WebParam(name = "passWord") String wspassword) throws Exception {
        ResponseSendMT response = new ResponseSendMT();
        logger.info("Start process SendMt for sub " + isdn + " client " + wsuser);
        //        step 1: validate input
        if (isdn == null || "".equals(isdn.trim()) || "?".equals(isdn.trim())
                || message == null || "".equals(message.trim()) || message.length() > 160 || "?".equals(message.trim())
                || message.length() == 0
                || wsuser == null || "".equals(wsuser.trim())
                || wspassword == null || "".equals(wspassword.trim())) {
            logger.warn("Invalid input sub " + isdn);
            response.setErrorCode(Vas.ResultCode.INVALID_INPUT);
            response.setDescription("INVALID_INPUT");
            return response;
        }
//        step 2: validate ip
        String ip = getIpClient();
        if (ip == null || "".equals(ip.trim())) {
            logger.warn("Can not get ip for sub " + isdn);
            response.setErrorCode(Vas.ResultCode.FAIL_GET_IP);
            response.setDescription("FAIL_GET_IP");
            return response;
        }
        UserInfo user = authenticate(db, wsuser, wspassword, ip);
        if (user == null || user.getId() < 0) {
            logger.warn("Invalid account " + isdn);
            response.setErrorCode(Vas.ResultCode.WRONG_ACCOUNT_IP);
            response.setDescription("WRONG_ACCOUNT_IP");
            return response;
        }
        int result;
        if (!isdn.startsWith(Common.config.countryCode)) {
            isdn = Common.config.countryCode + isdn;
        }
        if (shortCode != null && !"".equals(shortCode.trim()) && !"?".equals(shortCode.trim())) {
            result = SmsWsManager.getWebservice().sendSmsAll("movitel", message, isdn,
                    shortCode, logger);
            logger.info("Send sms for sub " + isdn + " content " + message + " result " + result + ", now insert log.");
            db.insertSendSmsHis(isdn, message, shortCode, wsuser, result + "");
        } else {
            result = SmsWsManager.getWebservice().sendSmsAll("movitel", message, isdn,
                    WebserviceManager.shortCode, logger);
            logger.info("Send sms for sub " + isdn + " content " + message + " result " + result + ", now insert log.");
            db.insertSendSmsHis(isdn, message, WebserviceManager.shortCode, wsuser, result + "");
        }
        if (result == 0) {
            response.setErrorCode(Common.ErrCode.SUCCESS);
            response.setDescription("Send Message Success");
        } else {
            response.setErrorCode(Common.ErrCode.FAIL);
            response.setDescription("Send Message fail");
        }
        return response;
    }
}
