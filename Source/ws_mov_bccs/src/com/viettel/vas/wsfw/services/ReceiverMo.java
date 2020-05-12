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
import com.viettel.vas.wsfw.database.DbProcessor;

/**
 *
 * @author minhnh@viettel.com.vn
 * @since Jun 4, 2013
 * @version 1.0
 */
@WebService
public class ReceiverMo extends WebserviceAbstract {

    DbProcessor db;

    public ReceiverMo() throws Exception {
        super("ReceiverMo");
        db = new DbProcessor("dbVote", logger);
    }

    @WebMethod(operationName = "moRequest")
    public String SendMT(
            @WebParam(name = "username") String user,
            @WebParam(name = "password") String pass,
            @WebParam(name = "source") String msisdn,
            @WebParam(name = "dest") String shortcode,
            @WebParam(name = "content") String content) throws Exception {
        logger.info("Start process moRequest for sub " + msisdn + " client " + user + " content " + content);
        //        step 1: validate input
        if (msisdn == null || "".equals(msisdn.trim()) || "?".equals(msisdn.trim())
                || content == null || "".equals(content.trim()) || content.length() > 160 || "?".equals(content.trim())
                || user == null || "".equals(user.trim())
                || pass == null || "".equals(pass.trim())) {
            logger.warn("Invalid input sub " + msisdn);
            return "200";
        }
        if (!"voteVas".equals(user) || !"voteVas@2019".equals(pass)) {
            logger.warn("Invalid account " + msisdn);
            return "201";
        }
        if (!msisdn.startsWith(Common.config.countryCode)) {
            msisdn = Common.config.countryCode + msisdn;
        }
        if (!content.trim().toUpperCase().startsWith("VOT")
                && !content.trim().toUpperCase().startsWith("COD")
                && !content.trim().toUpperCase().startsWith("AJU")
                && !content.trim().toUpperCase().startsWith("RES")) {
            db.insertGp(msisdn, content, "dbGp");
            logger.info("Receive MO and forward success " + msisdn);
            return "1";
        } else {
            int result = 0;
            String[] cmdparam = content.split("\\s+");
            if (cmdparam.length > 1) {
                result = db.insertMO(msisdn, cmdparam[1], "dbVote", "86156", "79", cmdparam[0]);
            } else {
                result = db.insertMO(msisdn, "", "dbVote", "86156", "79", cmdparam[0]);
            }
            if (result == 0) {
                logger.info("Receive MO success " + msisdn);
                return "1";
            } else {
                logger.warn("Receive MO success " + msisdn);
                return "0";
            }
        }
    }
}
