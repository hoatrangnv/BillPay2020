/*
 * Copyright (C) 2010 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.vas.wsfw.services;

import com.google.gson.Gson;
import com.viettel.smsfw.manager.AppManager;
import com.viettel.vas.wsfw.common.WebserviceAbstract;
import com.viettel.vas.wsfw.database.DbChannelProcessor;
import com.viettel.vas.wsfw.object.ListRevenue;
import com.viettel.vas.wsfw.object.ResponseRevenue;
import com.viettel.vas.wsfw.object.UssdObj;
import javax.jws.WebMethod;
import javax.jws.WebParam;
import javax.jws.WebService;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author minhnh@viettel.com.vn
 * @since Jun 4, 2013
 * @version 1.0
 */
@WebService
public class UssdProcess extends WebserviceAbstract {

    DbChannelProcessor db;
    public UssdProcess() {
        super("UssdProcess");
        try {
            db = new DbChannelProcessor("dbsm", logger);
        } catch (Exception ex) {
            logger.error("Fail init webservice UssdProcess");
            logger.error(ex);
        }
    }

//    @WebMethod(operationName = "synRevenueBccs")
//    public ResponseRevenue synRevenueBccs(
//            @WebParam(name = "user") String user,
//            @WebParam(name = "pass") String pass) throws Exception {
//        logger.info("Start process synRevenueBccs user " + user);
//        ResponseRevenue response = new ResponseRevenue();
//        try {
//            response.setErrorCode(0);
//            response.setDescription("Demo api synchonize BCCS Revenue");
//            ListRevenue listRev = new ListRevenue();
//            listRev.setRevenues(db.getSaleRevenue(user));
//            response.setListRevenue(listRev);
//        } catch (Throwable e) {
//            logger.warn("Had exception " + e.toString());
//            return response;
//        } finally {
//            logger.info("Finish synRevenueBccs");
//            return response;
//        }
//
//    }

    @WebMethod(operationName = "ussdws")
    public String ussdws(
            @WebParam(name = "user") String user,
            @WebParam(name = "msg") String msg,
            @WebParam(name = "pass") String pass,
            @WebParam(name = "msisdn") String msisdn,
            @WebParam(name = "imsi") String imsi,
            @WebParam(name = "session") String session,
            @WebParam(name = "transactionid") String transactionid,
            @WebParam(name = "type") String type,
            @WebParam(name = "id") String id) throws Exception {
        logger.info("Start process ussd message msg " + msg);
        try {
            this.callHttp(msisdn, transactionid, id);
        } catch (Throwable e) {
            logger.warn("Had exception " + e.toString());
            return "Had exception " + e.toString();
        } finally {
            logger.info("Finish ussdws");
            return "success";
        }

    }

    public String callHttp(String msisdn, String transId, String gwId) {
        long timeSt = System.currentTimeMillis();
        String result = "";
        Reader in = null;
        try {
            String request = "";
            String response = "";
            String urlString = "http://127.0.0.1:7892";
            URL url = new URL(urlString);
            UssdObj ussd = new UssdObj();
            ussd.setMsg("Hello wold");
            ussd.setMsisdn(msisdn);
            ussd.setTransactionid(transId);
            ussd.setUssdgw_id(gwId);
            ussd.setRequestType("202");
            ussd.setUser("ewallet");
            ussd.setPass("ewallet");
            Gson gson = new Gson();
            request = gson.toJson(ussd, UssdObj.class);
            logger.info("Request: " + request);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
//            conn.setRequestProperty("Authorization", "Basic TW92aXRlbDp6b3Awc3QhQCM=");
            conn.setRequestProperty("Accept", "application/json");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setConnectTimeout(60000);
            conn.setReadTimeout(60000);
            conn.setDoOutput(true);
            String soapRequest = "<?xml version=\"1.0\" encoding=\"utf-8\"?><soap:Envelope xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\">   "
                    + "<soap:Body>"
                    + "<ussdrequest>"
                    + "<requestType>"
                    + "202"
                    + "</requestType>"
                    + "<transactionid>"
                    + transId
                    + "</transactionid>"
                    + "<ussdgw_id>"
                    + gwId
                    + "</ussdgw_id>"
                    + "<msg>"
                    + "Hello World"
                    + "</msg>"
                    + "<msisdn>"
                    + msisdn
                    + "</msisdn>"
                    + "<user>"
                    + "ewallet"
                    + "</user>"
                    + "<pass>"
                    + "ewallet"
                    + "</pass>"
                    + "</ussdrequest>"
                    + "</soap:Body>"
                    + "</soap:Envelope>";
            conn.getOutputStream().write(soapRequest.getBytes());
            logger.info(" Response code when open Connection: "
                    + conn.getResponseCode() + "\nResponse message:" + conn.getResponseMessage());
            in = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));
            StringBuilder sbJSON = new StringBuilder();
            for (int c;
                    (c = in.read()) >= 0;) {
                sbJSON.append((char) c);
            }
            response = sbJSON.toString();
            logger.info("Response Wallet: " + response);
            String responseCode = getValue("errorCode", response, "");
            logger.info("Finish ussdws error code: " + responseCode);
        } catch (Exception e) {
            logger.error("callHttp had exception isdn " + msisdn + " duration "
                    + (System.currentTimeMillis() - timeSt) + " detail " + e.toString());
            logger.error(AppManager.logException(timeSt, e));
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (Exception ex) {
                    logger.error("Failt to close inputstream " + ex.toString());
                }
            }
            return result;
        }
    }

    private String getValue(String tag, String content, String partern) /*     */ {
        String expression = "<" + partern + tag + ">";
        Pattern pattern = Pattern.compile(expression);
        Matcher matcher = pattern.matcher(content);
        int start = -1;
        int end = -1;

        if (matcher.find()) {
            start = matcher.end();
        }
        expression = "</" + partern + tag + ">";
        pattern = Pattern.compile(expression);
        matcher = pattern.matcher(content);
        if (matcher.find()) {
            end = matcher.start();
        }

        if ((end > start) && (start >= 0)) {
            return content.substring(start, end);
        }
        return "-1";
    }

    public static void main(String[] args) {
//        UssdProcess test = new UssdProcess();
//        String result = test.callHttp("D:\\STUDY\\Project\\Movitel\\MOV_WS\\test.jpg");
//        System.out.println("Result " + result);
    }
}
