/*
 * Copyright (C) 2010 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.vas.wsfw.services;

import com.viettel.vas.wsfw.common.WebserviceAbstract;
import javax.jws.WebMethod;
import javax.jws.WebParam;
import javax.jws.WebService;
import com.viettel.vas.wsfw.common.Common;
import com.viettel.vas.wsfw.database.DbPre;
import com.viettel.vas.wsfw.database.DbProcessor;
import com.viettel.vas.wsfw.object.ResponseBalance;
import com.viettel.data.ws.utils.Exchange;
import java.text.SimpleDateFormat;
import com.viettel.vas.util.ExchangeClientChannel;
import com.viettel.vas.wsfw.common.Vas;
import com.viettel.vas.wsfw.object.UserInfo;
import java.util.Calendar;
import java.util.Date;
import java.util.ResourceBundle;

/**
 *
 * @author minhnh@viettel.com.vn
 * @since Jun 4, 2013
 * @version 1.0
 */
@WebService
public class AddMoneyDataForKeeto extends WebserviceAbstract {

    Exchange exch;
    DbPre dbPre;
    DbProcessor db;
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
    String moneyId;
    String dataMonthlyId;
    String moneyMonthlyValue;
    String dataMBMonthlyValue;
    String expireDay;

    public AddMoneyDataForKeeto() {
        super("AddMoneyDataForKeeto");
        try {
            dbPre = new DbPre("cm_pre", logger);
            db = new DbProcessor("dbtopup", logger);
            exch = new Exchange(ExchangeClientChannel.getInstance("../etc/exchange_client.cfg").getInstanceChannel(), logger);
            moneyMonthlyValue = ResourceBundle.getBundle("vas").getString("moneyMonthlyValue");
            dataMBMonthlyValue = ResourceBundle.getBundle("vas").getString("dataMBMonthlyValue");
            expireDay = ResourceBundle.getBundle("vas").getString("expireDay");
            moneyId = ResourceBundle.getBundle("vas").getString("moneyId");
            dataMonthlyId = ResourceBundle.getBundle("vas").getString("dataMonthlyId");
        } catch (Exception ex) {
            logger.error("Fail init webservice AddMoneyDataForKeeto");
            logger.error(ex);
        }
    }

    @WebMethod(operationName = "addMoneyData")
    public ResponseBalance ShowAccInfo(
            @WebParam(name = "misdn") String msisdn,
            @WebParam(name = "userName") String wsuser,
            @WebParam(name = "passWord") String wspassword) throws Exception {
        ResponseBalance response = new ResponseBalance();
        logger.info("Start process addPrice for sub " + msisdn + " client " + wsuser);
//        step 1: validate input
        if (msisdn == null || "".equals(msisdn.trim())
                || wsuser == null || "".equals(wsuser.trim())
                || wspassword == null || "".equals(wspassword.trim())) {
            logger.warn("Invalid input sub " + msisdn);
            response.setErrorCode(Vas.ResultCode.INVALID_INPUT);
            response.setDescription("INVALID_INPUT");
            return response;
        }
//        step 2: validate ip
        String ip = getIpClient();
        if (ip == null || "".equals(ip.trim())) {
            logger.warn("Can not get ip for sub " + msisdn);
            response.setErrorCode(Vas.ResultCode.FAIL_GET_IP);
            response.setDescription("FAIL_GET_IP");
            return response;
        }
        UserInfo user = authenticate(db, wsuser, wspassword, ip);
        if (user == null || user.getId() < 0) {
            logger.warn("Invalid account " + msisdn);
            response.setErrorCode(Vas.ResultCode.WRONG_ACCOUNT_IP);
            response.setDescription("WRONG_ACCOUNT_IP");
            return response;
        }
        if (msisdn.startsWith(Common.config.countryCode)) {
            msisdn = msisdn.substring(Common.config.countryCode.length());
        }

//      Check msisdn already get promotion
        if (dbPre.checkAlreadyBonues(msisdn)) {
            logger.warn("Isdn already have bounes whith isdn " + msisdn);
            response.setErrorCode(Vas.ResultCode.ALREADY_HAVE_BOUNES);
            response.setDescription("Isdn already have bounes whith isdn " + msisdn);
            return response;
        }
        // change Date
        Calendar calSysDate = Calendar.getInstance();
        calSysDate.setTime(new Date());
        calSysDate.add(Calendar.DATE, Integer.parseInt(expireDay));
        String expireTime = sdf.format(calSysDate.getTime());

//        Add money
        String resultChargeMoney = exch.modifyMoney(msisdn, moneyMonthlyValue, moneyId, expireTime);
        if (!"0".equals(resultChargeMoney)) {
            logger.warn("Fail add money " + msisdn + " moneyMonthlyValue " + moneyMonthlyValue + " errcode chargemoney " + resultChargeMoney);
            response.setErrorCode(Vas.ResultCode.NOT_ENOUGH_MONEY);
            response.setDescription("The system now is busy, please try later.");
            return response;
        }

//        change data
        String error = exch.addSmsDataVoice(msisdn, dataMBMonthlyValue, dataMonthlyId, expireTime);
        if (!"0".equals(error)) {
            logger.warn("Fail charge addData msisdn " + msisdn + " moneyMonthlyValue " + moneyMonthlyValue + " errcode chargemoney " + error);
            response.setErrorCode(Vas.ResultCode.FALSE_ADD_DATA);
            response.setDescription("Fail charge addData msisdn " + msisdn + " errcode chargemoney " + error);
            return response;
        }

        logger.info("Start create histor for sub " + msisdn);
        try {
            dbPre.insertIsdnAddMoneyDataForKeeto(msisdn, dataMBMonthlyValue, moneyMonthlyValue, moneyId, dataMonthlyId);
        } catch (Exception e) {
            logger.warn("Fail to insertIsdnAddMoneyDataForKeeto " + msisdn + " dataMBMonthlyValue " + dataMBMonthlyValue);
        }
        logger.info("addMoneyData success for sub " + msisdn + " dataMBMonthlyValue : " + dataMBMonthlyValue
                + " money " + moneyMonthlyValue);
        response.setErrorCode(Common.ErrCode.SUCCESS);
        response.setDescription("SUCCESS");
        return response;
    }
}
