/*
 * Copyright 2012 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.paybonus.service;

import com.viettel.common.ViettelService;
import com.viettel.threadfw.manager.AppManager;
import com.viettel.vas.util.obj.ExchangeChannel;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import org.apache.log4j.Logger;
import com.viettel.common.ViettelMsg;
import com.viettel.paybonus.database.DbConnectKitProcessor;
import com.viettel.paybonus.database.DbKitBatchConnectProcessor;
import com.viettel.threadfw.database.DbProcessorAbstract;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.RequestEntity;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;
import org.jdom.Element;

/**
 *
 * @author kdvt_tungtt8
 * @version x.x
 * @since Dec 28, 2012
 */
public class Service {

    private Logger logger;
    private String loggerLabel = Service.class.getSimpleName() + ": ";
    private ExchangeChannel channel;
//    private DbProcessor dbProcessor;
    private StringBuffer br = new StringBuffer();
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
    private SimpleDateFormat sdf2 = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
    //
    public static final long REQUEST_TIME_OUT = 30000;
//    private static final String module = "PROVISIONING";

    public Service(ExchangeChannel channel, Logger logger) throws IOException {
        this.logger = logger;
        this.channel = channel;
//        this.dbProcessor = dbProcessor;
        try {
            logger.info(loggerLabel + "Connect Exchange Client-" + channel.getId());
        } catch (Exception ex) {
            logger.error(loggerLabel + "ERROR connect Exchange Client-" + channel.getId(), ex);
        }
    }

    public double checkMoney(String msisdn, String balanceId) {
        double balance = -9999;
        ViettelService request = new ViettelService();
        ViettelService response = new ViettelService();
        long start = System.currentTimeMillis();
        try {
            request.setMessageType("1900");
            request.setProcessCode("TEST");
            request.set("MSISDN", msisdn);

            response = (ViettelService) channel.sendAll(request, REQUEST_TIME_OUT, false);
            logTime("Time to checkMoney", start);

            String prRespondCode = response.get("responseCode").toString();
            if (("0".equals(prRespondCode)) || ("405000000".equals(prRespondCode))) {
                String balanceIdKey = "BALANCE_TYPE_IDS";
                String balanceValueKey = "BALANCES";

                String[] typeIds = ((String) response.get(balanceIdKey)).split("&");
                String[] balances = ((String) response.get(balanceValueKey)).split("&");

                balance = 0;

                for (int i = 0; i < typeIds.length; i++) {
                    if (typeIds[i].equals(balanceId)) {
                        balance += Double.parseDouble(balances[i]);
                    }
                }
                logger.info("\r\nThe Balance: " + balance);
                return balance;
            }
            logger.info("\r\nERROR Pro get balance --> ");
            logger.info("Thong tin gui sang Provisioning:\n" + request);
            logger.info("Thong tin tra ve tu Provisioning:\n" + response);
            return balance;
        } catch (Exception e) {
            logger.error("Error checkMoney", e);
            logger.info("Thong tin gui sang Provisioning:\n" + request);
            logger.info("Thong tin tra ve tu Provisioning:\n" + response);
        } finally {
            return balance;
        }
    }

    public void logTime(String strLog, long timeSt) {
        long timeEx = System.currentTimeMillis() - timeSt;
        if (timeEx >= AppManager.minTimePro && AppManager.loggerProMap != null) {
            br.setLength(0);
            br.append(loggerLabel).
                    append(AppManager.getTimeLevelOcs(timeEx)).append(": ").
                    append(strLog).
                    append(": ").
                    append(timeEx).
                    append(" ms");

            logger.warn(br);
        } else {
            br.setLength(0);
            br.append(loggerLabel).
                    append(strLog).
                    append(": ").
                    append(timeEx).
                    append(" ms");

            logger.info(br);
        }
    }

    public String addBalance(String msisdn, long money, String balanceId) {
        long timeSt = System.currentTimeMillis();
        ViettelMsg response = null;
        String err = "";
        try {
            logger.info("Start add Balance for sub " + msisdn + " value " + money + " accountid " + balanceId);
            ViettelService request = new ViettelService();
            request.setMessageType("1900");
            request.setProcessCode("000064");
            request.set("MSISDN", msisdn);
            request.set("ACCOUNT_TYPE", balanceId);
            request.set("AMOUNT", String.valueOf(money));
            request.set("VALIDITY_INCREMENT", "0");
            response = channel.sendAll(request, REQUEST_TIME_OUT, true);
            err = (String) response.get("responseCode");
            logger.info("End addBalance isdn " + msisdn + " amount " + money + " result " + err);
            return err;
        } catch (Exception ex) {
            logger.error("Had exception addBalance isdn " + msisdn + " amount " + money);
            logger.error(AppManager.logException(timeSt, ex));
            return err;
        }
    }

    public String addMbFree(String msisdn, String addDay, String amount, String balanceId, Date expireTime) {
        long timeSt = System.currentTimeMillis();
        ViettelMsg response = null;
        String err = "";
        int iAddDay;
        long lAmoung;
        try {
            Calendar cal = Calendar.getInstance();
            iAddDay = Integer.valueOf(addDay);
            lAmoung = Long.valueOf(amount);
            if (iAddDay >= 1000) {
                logger.warn("addDay too max over 1000 " + msisdn + " " + addDay);
                cal.add(Calendar.DATE, 1);
            } else {
                cal.add(Calendar.DATE, iAddDay);
            }
            if (lAmoung <= 0) {
                logger.warn("Amount must greater 0 " + msisdn);
                return err;
            }
            ViettelService request = new ViettelService();
            request.setMessageType("1900");
            request.setProcessCode("400002");
            request.set("MSISDN", msisdn);
            request.set("ACCOUNT_TYPE", balanceId);
            if (expireTime == null) {
                request.set("EXPIRE_DATE", sdf.format(cal.getTime()));
            } else {
                request.set("EXPIRE_DATE", sdf.format(expireTime));
            }
            request.set("AMOUNT", amount);
            request.set("VALIDITY_INCREMENT", "0");
            response = channel.sendAll(request, REQUEST_TIME_OUT, true);
            err = (String) response.get("responseCode");
            logger.info("End addMbFree isdn " + msisdn + " amount " + amount + " result " + err);
            return err;
        } catch (Exception ex) {
            logger.error("Had exception addMbFree isdn " + msisdn + " amount " + amount);
            logger.error(AppManager.logException(timeSt, ex));
            return err;
        }
    }

    public String blockFBB(String msisdn, String center) {
        long timeSt = System.currentTimeMillis();
        ViettelMsg response = null;
        String err = "";
        try {
            logger.info("Start blockFBB for sub " + msisdn + " center " + center);
            ViettelService request = new ViettelService();
            request.setMessageType("1900");
            request.setProcessCode("240011");
            request.set("MSISDN", msisdn);
            request.set("ACT_STATUS", "000"); //use 100 KHYC because Rating not accept to open by this reason from other systems
            request.set("NUM_WAY", "1");
            request.set("BLOCK_TYPE", "KHYC");
            request.set("USERNAME", msisdn);
            request.set("CENTER", center);
            Calendar cal = Calendar.getInstance();
            cal.add(Calendar.MINUTE, 10); // to be greater the time on server
            request.set("EFFECT_DATE", sdf2.format(cal.getTime()));
            response = channel.sendAll(request, REQUEST_TIME_OUT, true);
            err = (String) response.get("responseCode");
            logger.info("End blockFBB isdn " + msisdn + " center " + center + " result " + err + " request " + request);
            return err;
        } catch (Exception ex) {
            logger.error("Had exception blockFBB isdn " + msisdn + " center " + center);
            logger.error(AppManager.logException(timeSt, ex));
            return "";
        }
    }
    //LinhNBV 20180607: Add method connectKit

    public String connectKit(String mainProduct, String productCode, String msisdn, String serial, String imsi,
            String eki, DbProcessorAbstract db, String shopCode, String staffCode, String k4sNo, String tplId) {
        long timeSt = System.currentTimeMillis();
        ViettelMsg response = null;
        String err = "";
        DbConnectKitProcessor dbConnectKit = null;
        DbKitBatchConnectProcessor dbKitBatch = null;
        if (db.getClass() == DbConnectKitProcessor.class) {
            dbConnectKit = (DbConnectKitProcessor) db;
        } else if (db.getClass() == DbKitBatchConnectProcessor.class) {
            dbKitBatch = (DbKitBatchConnectProcessor) db;
        }
        try {
            logger.info("Start connectKit for sub " + msisdn + " serial " + serial);
            ViettelService request = new ViettelService();
            request.setMessageType("1900");
            request.setProcessCode("000045");
            request.set("LANG", "1");
            request.set("SERIAL", serial);
            request.set("TPLID_GPRS", "23");
            request.set("MAIN_PRODUCT", mainProduct);
            request.set("productId", productCode);
            request.set("K4SNO", k4sNo);
            request.set("MSISDN", msisdn);
            request.set("PAID_MODE", "0");
            request.set("START_MONEY", "0");
            request.set("FTN", "25886154");
            request.set("IMSI", imsi);
            request.set("EKI", eki);
            request.set("TPLID", tplId);
            response = channel.sendAll(request, REQUEST_TIME_OUT, true);
            logger.info("Response: " + response);
            err = (String) response.get("responseCode");
            logger.info("End connectKit isdn " + msisdn + " serial " + serial + " result " + err + " request " + request);
            if (dbConnectKit != null) {
                dbConnectKit.insertActionLogPr(msisdn, serial, shopCode, staffCode, request.toString(), response.toString(), err);
            } else if (dbKitBatch != null) {
                dbKitBatch.insertActionLogPr(msisdn, serial, shopCode, staffCode, request.toString(), response.toString(), err);
            }
            return err;
        } catch (Exception ex) {
            logger.error("Had exception connectKit isdn " + msisdn + " serial " + serial);
            logger.error(AppManager.logException(timeSt, ex));
            if (dbConnectKit != null) {
                dbConnectKit.insertActionLogPr(msisdn, serial, shopCode, staffCode, "Exception when connect KIT", ex.getMessage(), "ERR");
            } else if (dbKitBatch != null) {
                dbKitBatch.insertActionLogPr(msisdn, serial, shopCode, staffCode, "Exception when connect KIT", ex.getMessage(), "ERR");
            }

            return "";
        }
    }

    public String registerMCA(String msisdn, String url) {
        logger.info("Start registerMCA sub " + msisdn);
        PostMethod post = new PostMethod(url);
        String soapResponse = "";
        String result = "";
        long start = System.currentTimeMillis();
        try {
            String request = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:proc=\"http://Process.com/\">"
                    + " <soapenv:Header/> "
                    + "   <soapenv:Body> "
                    + "      <proc:subscribevip> \n"
                    + "         <msisdn>" + msisdn + "</msisdn> \n"
                    + "      </proc:subscribevip> "
                    + "   </soapenv:Body> "
                    + " </soapenv:Envelope>";
            RequestEntity entity = new StringRequestEntity(request, "text/xml", "UTF-8");
            post.setRequestEntity(entity);
            MultiThreadedHttpConnectionManager conMgr = new MultiThreadedHttpConnectionManager();
            conMgr.setMaxConnectionsPerHost(20000);
            conMgr.setMaxTotalConnections(20000);
            HttpClient httpTransport = new HttpClient(conMgr);
            HttpConnectionManager conMgr1 = httpTransport.getHttpConnectionManager();
            HttpConnectionManagerParams conPars = conMgr1.getParams();
            conPars.setMaxTotalConnections(2000);
            conPars.setConnectionTimeout(30000); //timeout ket noi : ms
            conPars.setSoTimeout(60000); //timeout doc ket qua : ms
            httpTransport.executeMethod(post);
            soapResponse = post.getResponseBodyAsString(609600000);
            //            Parse reponse
            XmlConfig soapMsg = new XmlConfig();
            soapMsg.load(soapResponse, logger);
            Element root = soapMsg.getDocument().getRootElement();
            Element ele = XmlUtil.findElement(root, "return");
            if (ele != null) {
                result = ele.getText().split("\\|")[0];
                if (!"0".equals(result)) {
                    result = "ERR";
                }
            } else {
                result = "ERR";
            }
            logTime("Time to registerMCA msisdn: " + msisdn + " result: " + result, start);
            return result;
        } catch (Exception ex) {
            logTime("Exception to registerMCA msisdn:  " + msisdn, start);
            logger.error(AppManager.logException(start, ex));
            return "ERR";
        } catch (Throwable e) {
            logTime("Exception to registerMCA msisdn:  " + msisdn, start);
            logger.error(AppManager.logException(start, e));
            return "ERR";
        } finally {
            post.releaseConnection();
        }

    }

    public String renewMCA(String msisdn, String url) {
        PostMethod post = new PostMethod(url);
        String soapResponse = "";
        String result = "";
        long start = System.currentTimeMillis();
        try {
            String request = "<?xml version=\"1.0\" encoding=\"UTF-8\"?> <soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:proc=\"http://Process.com/\"> \n"
                    + "<soapenv:Header /> \n"
                    + "<soapenv:Body> \n"
                    + "	<proc:updateexpiredate> \n"
                    + "		<msisdn>" + msisdn + "</msisdn> \n"
                    + "	</proc:updateexpiredate> \n"
                    + "</soapenv:Body> \n"
                    + "</soapenv:Envelope>";
            RequestEntity entity = new StringRequestEntity(request, "text/xml", "UTF-8");
            post.setRequestEntity(entity);
            MultiThreadedHttpConnectionManager conMgr = new MultiThreadedHttpConnectionManager();
            conMgr.setMaxConnectionsPerHost(20000);
            conMgr.setMaxTotalConnections(20000);
            HttpClient httpTransport = new HttpClient(conMgr);
            HttpConnectionManager conMgr1 = httpTransport.getHttpConnectionManager();
            HttpConnectionManagerParams conPars = conMgr1.getParams();
            conPars.setMaxTotalConnections(2000);
            conPars.setConnectionTimeout(30000); //timeout ket noi : ms
            conPars.setSoTimeout(60000); //timeout doc ket qua : ms
            httpTransport.executeMethod(post);
            soapResponse = post.getResponseBodyAsString(609600000);
            //            Parse reponse
            XmlConfig soapMsg = new XmlConfig();
            soapMsg.load(soapResponse, logger);
            Element root = soapMsg.getDocument().getRootElement();
            Element ele = XmlUtil.findElement(root, "return");
            if (ele != null) {
                result = ele.getText().split("\\|")[0];
                if (!"0".equals(result)) {
                    result = "ERR";
                }
            } else {
                result = "ERR";
            }
            logTime("Time to renewMCA msisdn: " + msisdn + " result: " + result, start);
            return result;
        } catch (Exception ex) {
            logTime("Exception to renewMCA msisdn:  " + msisdn, start);
            logger.error(AppManager.logException(start, ex));
            return "ERR";
        } finally {
            post.releaseConnection();
        }

    }

    public String registerCRBT(String msisdn, String url) {
        long start = System.currentTimeMillis();
        try {
            PostMethod post = new PostMethod(url);
            String soapResponse = "";
            String result = "";

            String request = "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:ws=\"http://elcom.com.vn/crbt/ws\">\n"
                    + "   <soapenv:Header/>\n"
                    + "   <soapenv:Body>\n"
                    + "      <ws:registerTrial>\n"
                    + "         <!--Optional:-->\n"
                    + "         <msisdn>" + msisdn + "</msisdn>\n"
                    + "      </ws:registerTrial>\n"
                    + "   </soapenv:Body>\n"
                    + "</soapenv:Envelope>";
            RequestEntity entity = new StringRequestEntity(request, "text/xml", "UTF-8");
            post.setRequestEntity(entity);
            MultiThreadedHttpConnectionManager conMgr = new MultiThreadedHttpConnectionManager();
            conMgr.setMaxConnectionsPerHost(20000);
            conMgr.setMaxTotalConnections(20000);
            HttpClient httpTransport = new HttpClient(conMgr);
            HttpConnectionManager conMgr1 = httpTransport.getHttpConnectionManager();
            HttpConnectionManagerParams conPars = conMgr1.getParams();
            conPars.setMaxTotalConnections(2000);
            conPars.setConnectionTimeout(30000); //timeout ket noi : ms
            conPars.setSoTimeout(60000); //timeout doc ket qua : ms
            httpTransport.executeMethod(post);
            soapResponse = post.getResponseBodyAsString(609600000);
            //            Parse reponse
            XmlConfig soapMsg = new XmlConfig();
            soapMsg.load(soapResponse, logger);
            Element root = soapMsg.getDocument().getRootElement();
            logger.info("Root: " + root);
            Element ele = XmlUtil.findElement(root, "return");
            logger.info("Element: " + ele.getText());
            Element resultCode = XmlUtil.findElement(ele, "result_code");
            if (resultCode != null) {
                result = resultCode.getText();
                if (!"0".equals(result)) {
                    result = "ERR";
                }
            } else {
                result = "ERR";
            }
            logTime("Time to registerCRBT msisdn: " + msisdn + " result: " + result, start);
            post.releaseConnection();
            return result;
        } catch (Exception ex) {
            logTime("Exception to registerCRBT msisdn:  " + msisdn, start);
            logger.error(AppManager.logException(start, ex));
            return "ERR";
        } finally {
        }

    }

    public String renewCRBT(String msisdn, String url) {

        String result = "";
        long start = System.currentTimeMillis();
        try {
            PostMethod post = new PostMethod(url);
            String soapResponse = "";
            String request = "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:ws=\"http://elcom.com.vn/crbt/ws\">\n"
                    + "   <soapenv:Header/>\n"
                    + "   <soapenv:Body>\n"
                    + "      <ws:continueTrial>\n"
                    + "         <!--Optional:-->\n"
                    + "         <msisdn>" + msisdn + "</msisdn>\n"
                    + "         <!--Optional:-->\n"
                    + "         <status>1</status>\n"
                    + "      </ws:continueTrial>\n"
                    + "   </soapenv:Body>\n"
                    + "</soapenv:Envelope>";
            RequestEntity entity = new StringRequestEntity(request, "text/xml", "UTF-8");
            post.setRequestEntity(entity);
            MultiThreadedHttpConnectionManager conMgr = new MultiThreadedHttpConnectionManager();
            conMgr.setMaxConnectionsPerHost(20000);
            conMgr.setMaxTotalConnections(20000);
            HttpClient httpTransport = new HttpClient(conMgr);
            HttpConnectionManager conMgr1 = httpTransport.getHttpConnectionManager();
            HttpConnectionManagerParams conPars = conMgr1.getParams();
            conPars.setMaxTotalConnections(2000);
            conPars.setConnectionTimeout(30000); //timeout ket noi : ms
            conPars.setSoTimeout(60000); //timeout doc ket qua : ms
            httpTransport.executeMethod(post);
            soapResponse = post.getResponseBodyAsString(609600000);
            //            Parse reponse
            XmlConfig soapMsg = new XmlConfig();
            soapMsg.load(soapResponse, logger);
            Element root = soapMsg.getDocument().getRootElement();
            logger.info("Root: " + root);
            Element ele = XmlUtil.findElement(root, "return");
            logger.info("Element: " + ele.getText());
            Element resultCode = XmlUtil.findElement(ele, "result_code");
            if (resultCode != null) {
                result = resultCode.getText();
                if (!"0".equals(result)) {
                    result = "ERR";
                }
            } else {
                result = "ERR";
            }
            logTime("Time to renewCRBT msisdn: " + msisdn + " result: " + result, start);
            post.releaseConnection();
            return result;
        } catch (Exception ex) {
            logTime("Exception to renewCRBT msisdn:  " + msisdn, start);
            logger.error(AppManager.logException(start, ex));
            return "ERR";
        } finally {
        }

    }

    public String callPaymentGw(String requestId, String msisdn, String amount, String signature,
            String partnerCode, String sourceId, String serviceType, String userName, String passWord, String url) {
        PostMethod post = new PostMethod(url);
        String soapResponse = "";
        String result = "";
        long start = System.currentTimeMillis();
        try {
            String request = "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:ser=\"http://services.wsfw.vas.viettel.com/\">\n"
                    + "   <soapenv:Header/>\n"
                    + "   <soapenv:Body>\n"
                    + "      <ser:TopupHavePosFund>\n"
                    + "         <requestId>" + requestId + "</requestId>\n"
                    + "         <msisdn>" + msisdn + "</msisdn>\n"
                    + "         <amount>" + amount + "</amount>\n"
                    + "         <signature>" + signature + "</signature>\n"
                    + "         <partnerCode>" + partnerCode + "</partnerCode>\n"
                    + "         <creditAccountNumber>" + sourceId + "</creditAccountNumber>\n"
                    + "         <sourceId>" + sourceId + "</sourceId>\n"
                    + "         <serviceType>" + serviceType + "</serviceType>\n"
                    + "         <userName>" + userName + "</userName>\n"
                    + "         <passWord>" + passWord + "</passWord>\n"
                    + "      </ser:TopupHavePosFund>\n"
                    + "   </soapenv:Body>\n"
                    + "</soapenv:Envelope>";
            RequestEntity entity = new StringRequestEntity(request, "text/xml", "UTF-8");
            post.setRequestEntity(entity);
            MultiThreadedHttpConnectionManager conMgr = new MultiThreadedHttpConnectionManager();
            conMgr.setMaxConnectionsPerHost(20000);
            conMgr.setMaxTotalConnections(20000);
            HttpClient httpTransport = new HttpClient(conMgr);
            HttpConnectionManager conMgr1 = httpTransport.getHttpConnectionManager();
            HttpConnectionManagerParams conPars = conMgr1.getParams();
            conPars.setMaxTotalConnections(2000);
            conPars.setConnectionTimeout(30000); //timeout ket noi : ms
            conPars.setSoTimeout(60000); //timeout doc ket qua : ms
            httpTransport.executeMethod(post);
            soapResponse = post.getResponseBodyAsString(609600000);
            //            Parse reponse
            XmlConfig soapMsg = new XmlConfig();
            soapMsg.load(soapResponse, logger);
            Element root = soapMsg.getDocument().getRootElement();
            Element ele = XmlUtil.findElement(root, "return");
            if (ele != null) {
                result = ele.getText().split("\\|")[0];
            } else {
                result = "ERR";
            }
            logTime("Time to callPaymentGw msisdn: " + msisdn + " result: " + result, start);
        } catch (Exception ex) {
            logTime("Exception to callPaymentGw msisdn:  " + msisdn, start);
            logger.error(AppManager.logException(start, ex));
            return "ERR";
        } finally {
            post.releaseConnection();
            return result;
        }
    }

    /**
     * Block or Activate subscriber
     *
     * @param msisdn
     * @param services
     * @param isBlock true = block; false=activate
     * @return
     */
    public String blockOpenFraudSubscriber(String msisdn, String services, boolean isBlock) {
        logger.info("Begin " + (isBlock ? " block " : "active") + " fraud subscriber isdn " + msisdn);
        if ((msisdn != null) && (!"".equals(msisdn.trim()))
                && (!msisdn.startsWith("258"))) {
            msisdn = "258" + msisdn;
        }
        ViettelService request = new ViettelService();
        ViettelService response = new ViettelService();
        long start = System.currentTimeMillis();
        try {
            //processCode=000041-LOCK;000042-UNLOCK
            String processCode = isBlock ? "000041" : "000042";
            request.setMessageType("1900");
            request.setProcessCode(processCode);
            request.set("MSISDN", msisdn);
            request.set("CURRENT_SERVICES", services);
            request.set("NUM_WAY", "1");

            response = (ViettelService) this.channel.sendAll(request, 30000L, false);
            logTime("Time to blockPhoneFraud", start);
            logger.info("End blockPhoneFraud " + response.getError() + ":" + response.getDescription());
            return response.get("responseCode").toString();
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("ERROR blockPhoneFraud " + msisdn, e);
            return "";
        }
    }

    public String exportOrder(String xmlExport) {
        PostMethod post = new PostMethod("http://10.229.43.75:8804/WSMOV/IMWS?wsdl");//Test: http://10.229.41.35:8014/WSMOV/IMWS?wsdl
        String soapResponse = "";
        String result = "";
        long start = System.currentTimeMillis();
        try {
            String request = "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:ws=\"http://ws.smartphonev2.bss.viettel.com/\">\n"
                    + "   <soapenv:Header/>\n"
                    + "   <soapenv:Body>\n"
                    + "      <ws:exportOrder>\n"
                    + xmlExport
                    + "      </ws:exportOrder>\n"
                    + "   </soapenv:Body>\n"
                    + "</soapenv:Envelope>";
            RequestEntity entity = new StringRequestEntity(request, "text/xml", "UTF-8");
            post.setRequestEntity(entity);
            MultiThreadedHttpConnectionManager conMgr = new MultiThreadedHttpConnectionManager();
            conMgr.setMaxConnectionsPerHost(20000);
            conMgr.setMaxTotalConnections(20000);
            HttpClient httpTransport = new HttpClient(conMgr);
            HttpConnectionManager conMgr1 = httpTransport.getHttpConnectionManager();
            HttpConnectionManagerParams conPars = conMgr1.getParams();
            conPars.setMaxTotalConnections(2000);
            conPars.setConnectionTimeout(120000); //timeout ket noi : ms
            conPars.setSoTimeout(120000); //timeout doc ket qua : ms
            httpTransport.executeMethod(post);
            soapResponse = post.getResponseBodyAsString(609600000);
            //            Parse reponse
            XmlConfig soapMsg = new XmlConfig();
            soapMsg.load(soapResponse, logger);
            Element root = soapMsg.getDocument().getRootElement();
            Element eleErrorCode = XmlUtil.findElement(root, "errorCode");
            Element eleDesc = XmlUtil.findElement(root, "errorDescription");
            result = eleErrorCode.getText() + "|" + eleDesc.getText();
            logTime("Time to exportOrder result: " + result, start);
        } catch (Exception ex) {
            ex.printStackTrace();
            logTime("Exception to exportOrder:  ", start);
            logger.error(AppManager.logException(start, ex));
            return "ERR";
        } finally {
            post.releaseConnection();
            return result;
        }
    }

    public String callTopUpNotFundWS(String requestId, String msisdn, String amount, String userName, String passWord, String url) {
        PostMethod post = new PostMethod(url);
        String soapResponse = "";
        String result = "";
        long start = System.currentTimeMillis();
        try {
            String request = "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:ser=\"http://services.wsfw.vas.viettel.com/\">\n"
                    + "   <soapenv:Header/>\n"
                    + "   <soapenv:Body>\n"
                    + "      <ser:topup>\n"
                    + "         <requestId>" + requestId + "</requestId>\n"
                    + "         <msisdn>" + msisdn + "</msisdn>\n"
                    + "         <ammount>" + amount + "</ammount>\n"
                    + "         <userName>" + userName + "</userName>\n"
                    + "         <passWord>" + passWord + "</passWord>\n"
                    + "      </ser:topup>\n"
                    + "   </soapenv:Body>\n"
                    + "</soapenv:Envelope>";
            RequestEntity entity = new StringRequestEntity(request, "text/xml", "UTF-8");
            post.setRequestEntity(entity);
            MultiThreadedHttpConnectionManager conMgr = new MultiThreadedHttpConnectionManager();
            conMgr.setMaxConnectionsPerHost(20000);
            conMgr.setMaxTotalConnections(20000);
            HttpClient httpTransport = new HttpClient(conMgr);
            HttpConnectionManager conMgr1 = httpTransport.getHttpConnectionManager();
            HttpConnectionManagerParams conPars = conMgr1.getParams();
            conPars.setMaxTotalConnections(2000);
            conPars.setConnectionTimeout(30000); //timeout ket noi : ms
            conPars.setSoTimeout(60000); //timeout doc ket qua : ms
            httpTransport.executeMethod(post);
            soapResponse = post.getResponseBodyAsString(609600000);
            //            Parse reponse
            XmlConfig soapMsg = new XmlConfig();
            soapMsg.load(soapResponse, logger);
            Element root = soapMsg.getDocument().getRootElement();
            Element ele = XmlUtil.findElement(root, "return");
            if (ele != null) {
                result = ele.getText().split("\\|")[0];
            } else {
                result = "ERR";
            }
            logTime("Time to callTopUpNotFundWS msisdn: " + msisdn + " result: " + result, start);
        } catch (Exception ex) {
            logTime("Exception to callTopUpNotFundWS msisdn:  " + msisdn, start);
            logger.error(AppManager.logException(start, ex));
            return "ERR";
        } finally {
            post.releaseConnection();
            return result;
        }
    }
}
