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
import com.viettel.vas.wsfw.common.Vas;
import com.viettel.vas.wsfw.database.DbPost;
import com.viettel.vas.wsfw.database.DbPre;
import com.viettel.vas.wsfw.database.DbProcessor;
import com.viettel.vas.wsfw.object.Subscriber;
import com.viettel.vas.wsfw.object.UserInfo;
import com.viettel.data.ws.utils.Exchange;
import com.viettel.smsfw.manager.AppManager;

/**
 *
 * @author minhnh@viettel.com.vn
 * @since Jun 4, 2013
 * @version 1.0
 */
@WebService
public class ValidateAccount extends WebserviceAbstract {

    DbProcessor db;
    DbPre dbPre;
    DbPost dbPost;
    Exchange exch;
//    String sRateTopupFee;
//    Integer rateTopupFee;
    String sStaffIdBillPay;
    String staffCode;
    String groupId;
    long staffIdBillPay;
    long shopIdBillPay;
    String shopId;

    public ValidateAccount() throws Exception {
        super("ValidateAccount");
        try {
            logger.info("Start init webservice ValidateAccount");
            dbPre = new DbPre("cm_pre", logger);
            dbPost = new DbPost("cm_pos", logger);
            db = new DbProcessor("dbtopup", logger);
        } catch (Exception e) {
            logger.error("Fail init webservice ValidateAccount");
            logger.error(e);
        }
    }

    @WebMethod(operationName = "ValidateAccount")
    public String ValidateAccount(
            @WebParam(name = "msisdn", targetNamespace = "") String msisdn,
            @WebParam(name = "userName") String wsuser,
            @WebParam(name = "passWord") String wspassword) {
        long timeStart = System.currentTimeMillis();
        try {
            logger.info("Start process ValidateAccount for sub " + msisdn + " client " + wsuser);
//        step 1: validate input
            if (msisdn == null || "".equals(msisdn.trim())
                    || wsuser == null || "".equals(wsuser.trim())
                    || wspassword == null || "".equals(wspassword.trim())
                    || msisdn.length() > 99) {
                logger.warn("Invalid input sub " + msisdn + " length " + (msisdn == null ? 0 : msisdn.length())
                        + " wsuser " + wsuser + " length " + (wsuser == null ? 0 : wsuser.length())
                        + " wspassword " + wspassword + " length " + (wspassword == null ? 0 : wspassword.length()));
                return Vas.Topup.INPUT_ERROR + "|The input is invalid";
            }
//        step 2: validate ip
            String ip = getIpClient();
            if (ip == null || "".equals(ip.trim())) {
                logger.warn("Can not get ip for sub " + msisdn);
                return Vas.Topup.INPUT_ERROR + "|The remote IP is not allowed";
            }
            UserInfo user = authenticate(db, wsuser, wspassword, ip);
            if (user == null || user.getId() < 0) {
                logger.warn("Invalid account " + msisdn);
                return Vas.Topup.INPUT_ERROR + "|Invalid accoun";
            }
//            Check signature            
            if (!msisdn.startsWith(Common.config.countryCode)) {
                msisdn = Common.config.countryCode + msisdn;
            }
// Check postpaid mobile
            logger.info(msisdn + " start checking mobile postpaid ");
            Subscriber posSub = dbPost.getSubInfoMobile(msisdn, false);
            if (posSub == null) {
                logger.warn("Fail get post sub " + msisdn);
                return Vas.Topup.DATABASE_ERROR + "|Server is too busy";
            }
            if (!posSub.getMsisdn().equals("NO_INFO_SUB")) {
                logger.info("Sub is active postpaid mobile " + msisdn);
                return Vas.Topup.SUCCESSFUL + "|pos";
            } else {
                // Check prepaid mobile
                logger.info(msisdn + " start checking mobile prepaid");
                Subscriber preSub = dbPre.getSubInfoMobile(msisdn, false);
                if (preSub == null) {
                    logger.warn("Fail get pre sub " + msisdn);
                    return Vas.Topup.DATABASE_ERROR + "|Server is too busy";
                }
                if (!preSub.getMsisdn().equals("NO_INFO_SUB")) {
                    logger.info("Sub is active prepaid mobile " + msisdn);
                    return Vas.Topup.SUCCESSFUL + "|pre";
                } else {
                    // Check fixbroadband
                    logger.info(msisdn + " start checking fixbroadband ");
                    // Lay thong tin thue bao ADSL, FTTH, Leaseline, not support whiteleaseline
                    Subscriber adslSub = dbPost.getSubInfoADSL(msisdn);
                    if (adslSub == null) {
                        logger.warn("Fail get adsl sub " + msisdn);
                        return Vas.Topup.DATABASE_ERROR + "|Server is too busy";
                    }
                    if (!adslSub.getMsisdn().equals("NO_INFO_SUB")) {
                        logger.info("Sub is active adsl " + msisdn);
                        return Vas.Topup.SUCCESSFUL + "|fbb";
                    } else {
                        logger.info("No infomation for sub " + msisdn);
                        return Vas.Topup.NOT_EXISTS + "|No infomation";
                    }
                }
            }
        } catch (Exception e) {
            logger.error("[!!!] Error ValidateAccount for sub " + msisdn, e);
            logger.error(AppManager.logException(timeStart, e));
            return Vas.Topup.EXCEPTION + "|Unexpected exception";
        }
    }
}
