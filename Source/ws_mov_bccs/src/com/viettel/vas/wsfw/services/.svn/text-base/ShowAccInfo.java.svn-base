/*
 * Copyright (C) 2010 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.vas.wsfw.services;

import com.viettel.vas.wsfw.common.WebserviceAbstract;
import com.viettel.vas.wsfw.common.WebserviceManager;
import javax.jws.WebMethod;
import javax.jws.WebParam;
import javax.jws.WebService;
import com.viettel.vas.wsfw.common.Common;
import com.viettel.vas.wsfw.database.DbPost;
import com.viettel.vas.wsfw.database.DbPre;
import com.viettel.vas.wsfw.database.DbProcessor;
import com.viettel.vas.wsfw.object.AccountInfo;
import com.viettel.vas.wsfw.object.Balance;
import com.viettel.vas.wsfw.object.ListBalance;
import com.viettel.vas.wsfw.object.ResponseBalance;
import com.viettel.vas.wsfw.object.Subscriber;
import com.viettel.data.ws.utils.Exchange;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import com.viettel.vas.util.ExchangeClientChannel;
import com.viettel.vas.wsfw.common.Vas;
import com.viettel.vas.wsfw.object.UserInfo;

/**
 *
 * @author minhnh@viettel.com.vn
 * @since Jun 4, 2013
 * @version 1.0
 */
@WebService
public class ShowAccInfo extends WebserviceAbstract {

    Exchange exch;
    DbPre dbPre;
    DbPost dbPost;
    DbProcessor db;
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
    private String accountBalance;
    private String accountList;

    public ShowAccInfo() {
        super("ShowAccInfo");
        try {
            dbPre = new DbPre("cm_pre", logger);
            dbPost = new DbPost("cm_pos", logger);
            db = new DbProcessor("dbtopup", logger);
            exch = new Exchange(ExchangeClientChannel.getInstance("../etc/exchange_client.cfg").getInstanceChannel(), logger);
            accountList = WebserviceManager.accountList;
        } catch (Exception ex) {
            logger.error("Fail init webservice ShowBalance");
            logger.error(ex);
        }
    }

    @WebMethod(operationName = "ShowAccInfo")
    public ResponseBalance ShowAccInfo(
            @WebParam(name = "misdn") String msisdn,
            @WebParam(name = "userName") String wsuser,
            @WebParam(name = "passWord") String wspassword) throws Exception {
        ResponseBalance response = new ResponseBalance();
        logger.info("Start process get account info for sub " + msisdn + " client " + wsuser);
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
        logger.info("Check prepaid sub " + msisdn);
        Subscriber subscriber = dbPre.getSubInfoMobile(msisdn, false);
        if (subscriber == null || subscriber.getSubId() == null || "".equals(subscriber.getSubId())) {
            //ko tim thay tren CM PRE, thuc hien tim tren CM POST
            logger.info("Not pre, check postpaid sub " + msisdn);
            subscriber = dbPost.getSubInfoMobile(msisdn, false);
            if (subscriber == null || subscriber.getSubId() == null || "".equals(subscriber.getSubId())) {
                logger.info("Not postpaid mobile, check homephone pre sub " + msisdn);
                subscriber = dbPre.getSubInfoHomephone(msisdn, false);
                if (subscriber == null || subscriber.getSubId() == null || "".equals(subscriber.getSubId())) {
                    logger.info("Not prepaid homephone, check postpaid homephone " + msisdn);
                    subscriber = dbPost.getSubInfoHomephone(msisdn, false);
                }
            }
        }
        //loi he thong CM
        if (subscriber == null) {
            logger.info("Not Movitel sub " + msisdn);
            response.setErrorCode(Vas.ResultCode.INVALID_INPUT);
            response.setDescription("SUBSCRIBER_INVALID");
            return response;
        }

        //neu khong co thong tin 
        if (subscriber.getSubId() == null || "".equals(subscriber.getSubId().trim())) {
            logger.info("Can not get sub info " + msisdn);
            response.setErrorCode(Vas.ResultCode.INVALID_INPUT);
            response.setDescription("SUBSCRIBER_INVALID");
            return response;
        }

        /**
         * TYPE : PRE/POST | STATUS | ACT STAUS | PRODUCT CODE | DATE TIME
         */
        String infoCM = "";
        infoCM += subscriber.getServiceType() + "|" + subscriber.getStatus() + "|" + subscriber.getActStatus()
                + "|" + subscriber.getProductCode() + "|" + subscriber.getActiveTime();

        if (subscriber.getServiceType() == 0) {
//            accountBalance = Common.config.accountBalance;
            accountBalance = accountList;
            AccountInfo listBalances = exch.viewAccInfo(msisdn);
            ListBalance listBalance = new ListBalance();
            List<Balance> lBal = new ArrayList<Balance>();
            String[] arrBalance = accountBalance.split("&");
            for (String bal : arrBalance) {
                Balance balance = new Balance();
                balance.setBalanceId(bal);
                balance.setBalanceValue(listBalances.getAccountView(bal));
                balance.setExpDate(listBalances.getAccountExpire(bal));
                lBal.add(balance);
                listBalance.setBalance(lBal);
            }
            response.setListBalance(listBalance);
        } else {
            logger.warn("Sub not prepaid mobile " + msisdn);
        }
        logger.info("Get info success for sub " + msisdn);
        response.setInfo(infoCM);
        response.setErrorCode(Common.ErrCode.SUCCESS);
        response.setDescription("SUCCESS");
        return response;
    }
}
