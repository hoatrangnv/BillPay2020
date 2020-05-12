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
import com.viettel.vas.wsfw.object.Subscriber;
import com.viettel.data.ws.utils.Exchange;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import com.viettel.vas.util.ExchangeClientChannel;
import com.viettel.vas.wsfw.common.Vas;
import com.viettel.vas.wsfw.object.UserInfo;
import java.util.Arrays;
import java.util.HashMap;
import java.util.ResourceBundle;

/**
 *
 * @author minhnh@viettel.com.vn
 * @since Jun 4, 2013
 * @version 1.0
 */
@WebService
public class AddPrice extends WebserviceAbstract {

    Exchange exch;
    DbPre dbPre;
    DbProcessor db;
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
    String ucmPrice;
    String ucmFee;
    String ucmPackage;
    ArrayList<String> listUcmPackage;
    HashMap<String, String> mapPrice;
    HashMap<String, String> mapFee;
    ArrayList<String> listErrNotEnoughBalance;

    public AddPrice() {
        super("AddPrice");
        try {
            dbPre = new DbPre("cm_pre", logger);
            db = new DbProcessor("dbtopup", logger);
            exch = new Exchange(ExchangeClientChannel.getInstance("../etc/exchange_client.cfg").getInstanceChannel(), logger);
            ucmPrice = ResourceBundle.getBundle("vas").getString("listUCMPrice");
            ucmFee = ResourceBundle.getBundle("vas").getString("listUCMFee");
            ucmPackage = ResourceBundle.getBundle("vas").getString("listUCMPackage");
            listUcmPackage = new ArrayList<String>();
            String[] lstPck = ucmPackage.split("\\|");
            for (String lp : lstPck) {
                logger.info("Add package " + lp + " to list");
                listUcmPackage.add(lp.trim().toUpperCase());
            }
            String[] temp = ucmPrice.split("\\|");
            mapPrice = new HashMap();
            for (String t : temp) {
                String[] temp2 = t.split(":");
                logger.info("Add price to map key " + temp2[0].trim().toUpperCase() + " value " + temp2[1].trim().toUpperCase());
                mapPrice.put(temp2[0].trim().toUpperCase(), temp2[1].trim().toUpperCase());
            }
            String[] tempFee = ucmFee.split("\\|");
            mapFee = new HashMap();
            for (String t : tempFee) {
                String[] tempFee2 = t.split(":");
                logger.info("Add fee to map key " + tempFee2[0].trim().toUpperCase() + " value " + tempFee2[1].trim().toUpperCase());
                mapFee.put(tempFee2[0].trim().toUpperCase(), tempFee2[1].trim().toUpperCase());
            }
            listErrNotEnoughBalance = new ArrayList<String>(Arrays.asList(ResourceBundle.getBundle("vas").getString("ERR_BALANCE_NOT_ENOUGH").split("\\|")));
        } catch (Exception ex) {
            logger.error("Fail init webservice AddPrice");
            logger.error(ex);
        }
    }

    @WebMethod(operationName = "addPrice")
    public ResponseBalance ShowAccInfo(
            @WebParam(name = "misdn") String msisdn,
            @WebParam(name = "userName") String wsuser,
            @WebParam(name = "passWord") String wspassword,
            @WebParam(name = "packageName") String packageName) throws Exception {
        ResponseBalance response = new ResponseBalance();
        logger.info("Start process addPrice for sub " + msisdn + " client " + wsuser + " packageName " + packageName);
//        step 1: validate input
        if (msisdn == null || "".equals(msisdn.trim())
                || wsuser == null || "".equals(wsuser.trim())
                || wspassword == null || "".equals(wspassword.trim())
                || packageName == null || "".equals(packageName.trim())) {
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
            logger.info("Not pre, so not support sub " + msisdn);
            response.setErrorCode(Vas.ResultCode.NOT_SUPPORT_POSTPAID);
            response.setDescription("Your current product's not supported.");
            return response;
        }
        if (subscriber.getStatus() == null || "".equals(subscriber.getStatus())
                || !subscriber.getStatus().equals("2")) {
            //ko tim thay tren CM PRE, thuc hien tim tren CM POST
            logger.info("Not active, so not support sub " + msisdn);
            response.setErrorCode(Vas.ResultCode.NOT_ACTIVE);
            response.setDescription("Your subscriber hasn't not actived, please active first.");
            return response;
        }
        if (subscriber.getProductCode() == null || "".equals(subscriber.getProductCode())) {
            logger.info("ProductCode is null or empty, so not support sub " + msisdn + " product " + subscriber.getProductCode());
            response.setErrorCode(Vas.ResultCode.INVALID_PRODUCT);
            response.setDescription("Your current product's not supported.");
            return response;
        }
        if (!listUcmPackage.contains(subscriber.getProductCode().trim().toUpperCase())) {
            logger.info("Not UCM product, so not support sub " + msisdn + " product " + subscriber.getProductCode());
            response.setErrorCode(Vas.ResultCode.INVALID_PRODUCT);
            response.setDescription("Your current product's not supported.");
            return response;
        }
        String price = mapPrice.get(packageName.trim().toUpperCase());
        if (price == null || price.trim().length() <= 0) {
            logger.info("Do not have price, so not support sub " + msisdn + " product " + subscriber.getProductCode());
            response.setErrorCode(Vas.ResultCode.INVALID_PRODUCT);
            response.setDescription("Your current product's not supported.");
            return response;
        }
        String fee = mapFee.get(packageName.trim().toUpperCase());
        if (fee == null || fee.trim().length() <= 0) {
            logger.info("Do not have money fee, so not support sub " + msisdn + " product " + subscriber.getProductCode());
            response.setErrorCode(Vas.ResultCode.INVALID_PRODUCT);
            response.setDescription("The system now is busy, please try later.");
            return response;
        }
//        Charge money
        String resultChargeMoney = exch.adjustMoney(msisdn, "-" + fee, "2000");
        if (listErrNotEnoughBalance.contains(resultChargeMoney)) {
            logger.warn("Not enough money " + msisdn + " fee " + fee + " errcode chargemoney " + resultChargeMoney);
            response.setErrorCode(Vas.ResultCode.NOT_ENOUGH_MONEY);
            response.setDescription("Your current balance is not enough, please refill money and try again.");
            return response;
        } else if (!"0".equals(resultChargeMoney)) {
            logger.warn("Fail charge money " + msisdn + " free " + fee + " errcode chargemoney " + resultChargeMoney);
            response.setErrorCode(Vas.ResultCode.NOT_ENOUGH_MONEY);
            response.setDescription("The system now is busy, please try later.");
            return response;
        }
        logger.info("Start add Price sub " + msisdn + " price " + price);
        String resultAddPrice = exch.addPrice(msisdn, price, "", "");
        if (!"0".equals(resultAddPrice)) {
            logger.info("Fail to add price " + msisdn + " price " + price);
            response.setErrorCode(Common.ErrCode.FAIL);
            response.setDescription("Fail to add price");
            return response;
        }
        try {
            dbPre.insertSubRelProduct(msisdn, subscriber.getSubId(), subscriber.getProductCode(), packageName);
        } catch (Exception e) {
            logger.warn("Fail to insert sub rel product for sub " + msisdn + " product " + subscriber.getProductCode());
        }
        logger.info("Add price success for sub " + msisdn);
        response.setErrorCode(Common.ErrCode.SUCCESS);
        response.setDescription("SUCCESS");
        return response;
    }
}
