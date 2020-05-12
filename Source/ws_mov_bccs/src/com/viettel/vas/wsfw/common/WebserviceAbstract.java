/*
 * Copyright (C) 2010 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.vas.wsfw.common;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.viettel.data.ws.utils.Encrypt;
import com.viettel.smsfw.manager.AppManager;
import com.viettel.vas.wsfw.object.UserInfo;
import java.util.List;
import java.util.Set;
import javax.annotation.Resource;
import javax.jws.WebMethod;
import javax.jws.WebParam;
import javax.jws.WebService;
import javax.xml.ws.WebServiceContext;
import javax.xml.ws.handler.MessageContext;
import org.apache.log4j.Logger;
import com.viettel.vas.wsfw.database.DbProcessor;
import java.util.HashMap;
import java.util.regex.Pattern;

/**
 *
 * @author minhnh@viettel.com.vn
 * @since Jun 4, 2013
 * @version 1.0
 */
@WebService
public abstract class WebserviceAbstract {

    public Logger logger;
    //
    public static int USER_NOT_FOUND = -1;
    public static int WRONG_PASSWORD = -2;
    public static int NOT_ALLOW = -3;
    public static int MSISDN_NOT_VALID = -4;
    public static int PARAM_NOT_ENOUGH = -5;
    public static int EXCEPTION = -6;
    private static String rexIp = "[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}";
    protected HashMap<String, UserInfo> mapUser; //Huynq13 20180827 add to load users to mem
    //
    @Resource
    WebServiceContext wsContext;

    public WebserviceAbstract(String logName) {
        logger = Logger.getLogger(logName);
    }

    public String getIpClient() {
        String ipAddress = "unknown";
        MessageContext msgCtxt = wsContext.getMessageContext();
        HttpExchange httpEx = (HttpExchange) msgCtxt.get("com.sun.xml.ws.http.exchange");
//        return httpEx.getRemoteAddress().getAddress().getHostAddress();
        if (httpEx == null) {
            httpEx = (HttpExchange) msgCtxt.get("com.sun.xml.internal.ws.http.exchange");
        }

        Headers header = httpEx.getRequestHeaders();
        Set<String> keySet = header.keySet();
        if (keySet == null || keySet.isEmpty()) {
            logger.debug("Header keyset is null or empty");
        }

        StringBuilder br = new StringBuilder();
        for (String key : keySet) {
            br.append(key).append(":");
            List<String> list = header.get(key);
            if (key.toLowerCase().equals("x-forwarded-for")) {
                for (String string : list) {
                    br.append(string).append(";");
                    if (string.matches(rexIp)) {
                        ipAddress = string;
                        break;
                    }
                }
            } else {
                for (String string : list) {
                    br.append(string).append(";");
                }
            }

            logger.debug(br);
            br.setLength(0);
        }

        if (ipAddress.equals("unknown")) {
            ipAddress = httpEx.getRemoteAddress().getAddress().getHostAddress();
        }

        return ipAddress;
    }

    public UserInfo authenticate(DbProcessor db, String userName, String password, String ipAddress) throws Exception {
        UserInfo userInfo = null;
        long timeStart = System.currentTimeMillis();
        try {
//            String decUserName = com.viettel.security.PassTranformer.decrypt(userName);
//            String decPassword = com.viettel.security.PassTranformer.decrypt(password);
            if (mapUser == null || mapUser.size() <= 0) {
                mapUser = db.getUserInfo();
            }            
            userInfo = mapUser.get(userName);                   
            if (userInfo == null) {
                logger.info("Wrong account or ip: " + Vas.ResultCode.USER_NOT_FOUND);
                userInfo = new UserInfo();
                userInfo.setId(Vas.ResultCode.USER_NOT_FOUND);
                return userInfo;
            }
            userInfo.setId(0);
            if (userInfo.getIp() != null && userInfo.getIp().length() > 0) {
                // Check IP
                String ip[] = userInfo.getIp().split(",");
                boolean pass = false;
                for (String ipConfig : ip) {
                    if (pair2(ipAddress, ipConfig.trim())) {
                        pass = true;
                        break;
                    }
                }
                if (!pass) {
                    logger.info("Wrong account or ip: " + Vas.ResultCode.IP_NOT_ALLOW + " the ip: " + ipAddress);
                    userInfo.setId(Vas.ResultCode.IP_NOT_ALLOW);
                    return userInfo;
                }
            }
            // Check password
            if (userInfo.getPassword() != null || userInfo.getPassword().trim().length() > 0) {
                if (password.equals(userInfo.getPassword())) {
                    return userInfo;
                }

                String passEncript = Encrypt.MD5(password);
                if (passEncript.equals(userInfo.getPassword())) {
                    return userInfo;
                }
                logger.info("Wrong account or ip: " + Vas.ResultCode.WRONG_PASSWORD);
                userInfo.setId(Vas.ResultCode.WRONG_PASSWORD);
            }
//            if (userInfo.getChannel() == null || userInfo.getChannel().trim().length() > 0) {
//            }
        } catch (Exception ex) {
            logger.info("Exception validate user: " + ex.getMessage());
            logger.error(AppManager.logException(timeStart, ex));
        }
        return userInfo;
    }

    public boolean pair(String ipClient, String ipConfig) {
        if (ipClient == null || ipClient.equals("") || ipConfig == null || ipConfig.equals("")) {
            return false;
        }
        ipConfig = ipConfig.replaceAll("x", "\\\\d+");
        return ipClient.matches(ipConfig);
    }

    private boolean pair2(String ip, String allowIP) {
        String[] listIP = allowIP.split("\\|");
        Pattern singleIp = Pattern.compile("^(?:[0-9]{1,3}\\.){3}[0-9]{1,3}$");
        Pattern rangeDIp = Pattern.compile("^(?:[0-9]{1,3}\\.){3}\\*$");
        for (int i = 0; i < listIP.length; i++) {
            if (singleIp.matcher(listIP[i]).matches()) {
                if (ip.equals(listIP[i])) {
                    return true;
                }
            } else if (rangeDIp.matcher(listIP[i]).matches()) {
                if (ip.substring(0, ip.lastIndexOf(".")).equals(
                        listIP[i].substring(0, listIP[i].lastIndexOf(".")))) {
                    return true;
                }
            }
        }
        return false;
    }

    @WebMethod(operationName = "testWebService")
    public String testWebService(
            @WebParam(name = "input") String input) {
        return "Test Webservice Success!!, return input: " + input;
    }
}
