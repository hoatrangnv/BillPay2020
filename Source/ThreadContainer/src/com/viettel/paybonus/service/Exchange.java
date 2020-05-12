/*
 * Copyright 2012 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.paybonus.service;

import com.viettel.common.ExchMsg;
import com.viettel.threadfw.manager.AppManager;
import com.viettel.vas.util.obj.ExchangeChannel;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.ResourceBundle;
import org.apache.log4j.Logger;
import com.google.gson.Gson;
import com.viettel.paybonus.database.DbBonus4GChangeSim;
import com.viettel.paybonus.database.DbBonusConnectKit;
import com.viettel.paybonus.database.DbBonusConnectPostpaid;
import com.viettel.paybonus.database.DbConnectKitProcessor;
import com.viettel.paybonus.database.DbDebitRollback;
import com.viettel.paybonus.database.DbEwalletDeductDaily;
import com.viettel.paybonus.database.DbKitBatchConnectProcessor;
import com.viettel.paybonus.database.DbKitBatchRetryPayComission;
import com.viettel.paybonus.database.DbKitBatchSubProcessor;
import com.viettel.paybonus.database.DbKitChangeToStudentProcessor;
import com.viettel.paybonus.database.DbMakeSaleTranCug;
import com.viettel.paybonus.database.DbMobileShopChannel;
import com.viettel.paybonus.database.DbPayBonusAgentVipProcessor;
import com.viettel.paybonus.database.DbPayBonusSecond;
import com.viettel.paybonus.database.DbPromotionScaner;
import com.viettel.paybonus.database.DbSubProfileProcessor;
import com.viettel.paybonus.database.DbSubProfileUpdateProcessor;
import com.viettel.paybonus.obj.AccountInfo;
import com.viettel.paybonus.obj.Customer;
import com.viettel.paybonus.obj.EmolaBean;
import com.viettel.paybonus.obj.EpayEmolaBean;
import com.viettel.paybonus.obj.EwalletDebitLog;
import com.viettel.paybonus.obj.EwalletLog;
import com.viettel.paybonus.obj.IntegrationProduct;
import com.viettel.paybonus.obj.Offer;
import com.viettel.paybonus.obj.ResponseWallet;
import com.viettel.paybonus.obj.RevertTransaction;
import com.viettel.paybonus.obj.SubscriberEmola;
import com.viettel.paybonus.obj.WalletBean;
import com.viettel.threadfw.database.DbProcessorAbstract;
import org.apache.http.params.HttpParams;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.HttpResponse;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.RequestEntity;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.jdom.Element;
import java.util.Scanner;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.soap.MessageFactory;
import javax.xml.soap.SOAPBody;
import javax.xml.soap.SOAPBodyElement;
import javax.xml.soap.SOAPConstants;
import javax.xml.soap.SOAPEnvelope;
import javax.xml.soap.SOAPMessage;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 *
 * @author kdvt_tungtt8
 * @version x.x
 * @since Dec 28, 2012
 */
public class Exchange {

    private Logger logger;
    private String loggerLabel = Exchange.class.getSimpleName() + ": ";
    private ExchangeChannel channel;
//    private DbProcessor dbProcessor;
    private StringBuffer br = new StringBuffer();
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
    //
    public static final long REQUEST_TIME_OUT = 30000;
//    private static final String module = "PROVISIONING";

    public Exchange(ExchangeChannel channel, Logger logger) throws IOException {
        this.logger = logger;
        this.channel = channel;
//        this.dbProcessor = dbProcessor;
        try {
            logger.info(loggerLabel + "Connect Exchange Client-" + channel.getId());
        } catch (Exception ex) {
            logger.error(loggerLabel + "ERROR connect Exchange Client-" + channel.getId(), ex);
        }
    }

    public String activeSub(String isdn) {
        ExchMsg request = new ExchMsg();
        ExchMsg response = null;
        long start = System.currentTimeMillis();
        try {
            request.setCommand("OCSHW_ACTIVEFIRST");
            request.set("ISDN", "258" + isdn);
            request.set("ClientTimeout", "25000");
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, false);
            logTime("Time to active sub " + isdn, start);
            return response.getError();
        } catch (Exception e) {
            logTime("Exception to active sub " + isdn, start);
            logger.error(AppManager.logException(start, e));
            return "";
        }
    }

    public String addProduct(String isdn) {
        ExchMsg request = new ExchMsg();
        ExchMsg response = null;
        long start = System.currentTimeMillis();
        try {
            request.setCommand("OCSHW_SUBSCRIBEPRODUCT");
            request.set("MSISDN", "258" + isdn);
            request.set("PRODUCTID", "10225033");
            request.set("EFFECTIVE_DATE", sdf.format(new Date()));
            Calendar cal = Calendar.getInstance();
            cal.setTime(new Date());
            cal.add(Calendar.DATE, 180);
            request.set("EXPIRE_DATE", sdf.format(cal.getTime()));
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, false);
            logTime("Time to addProduct " + isdn, start);
            return response.getError();
        } catch (Exception e) {
            logTime("Exception to addProduct " + isdn, start);
            logger.error(AppManager.logException(start, e));
            return "";
        }
    }

    public String addFellow(String isdna, String isdnb) {
        ExchMsg request = new ExchMsg();
        ExchMsg response = null;
        long start = System.currentTimeMillis();
        try {
            request.setCommand("OCSHW_ADDMANSUBFAMILYNO");
            request.set("MSISDN", "258" + isdna);
            request.set("GROUP_TYPE", "1");
            request.set("FAMILY_NUMBER", "258" + isdnb);
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, false);
            logTime("Time to addFellow isdna " + isdna + ", isdnb " + isdnb, start);
            return response.getError();
        } catch (Exception e) {
            logTime("Exception to addFellow " + isdna + ", isdnb " + isdnb, start);
            logger.error(AppManager.logException(start, e));
            return "";
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

    public void createEmolaForCustomer(Customer customer, String isdn, DbProcessorAbstract db) {
        String request = "";
        String content = "";
        DbSubProfileProcessor dbSubProfile = null;
        DbConnectKitProcessor dbKit = null;
        try {
            if (db.getClass() == DbConnectKitProcessor.class) {
                dbKit = (DbConnectKitProcessor) db;
            } else if (db.getClass() == DbSubProfileProcessor.class) {
                dbSubProfile = (DbSubProfileProcessor) db;
            }

            String BASE_URL = ResourceBundle.getBundle("configPayBonus").getString("CreateWalletForCust_wsdlUrl");
            String API = ResourceBundle.getBundle("configPayBonus").getString("api");
            String userNameString = ResourceBundle.getBundle("configPayBonus").getString("username");
            String pasString = ResourceBundle.getBundle("configPayBonus").getString("password");
            String funString = "CustomerSync";
            SubscriberEmola sub = new SubscriberEmola();
            sub.setCustId("" + customer.getCustId());
            sub.setUserName(customer.getSubName());
            sub.setMobile(isdn);
            sub.setCustomerName(customer.getSubName());
            sub.setGender(customer.getGender());
            sub.setDoB(customer.getBirthDate());
            sub.setIdType("1");
            sub.setIdNo(customer.getIdNo());
            sub.setAddress("");
            sub.setProgressType("1");//... 1>> Create new eMola
            Gson gson = new Gson();
            request = gson.toJson(sub, SubscriberEmola.class);
            logger.info("Request Wallet:" + request);
            //HttpClient client = new DefaultHttpClient();
            // set the connection timeout value to 60 seconds (60000 milliseconds)
            final HttpParams httpParams = new BasicHttpParams();
            HttpConnectionParams.setConnectionTimeout(httpParams, 60000);
            HttpConnectionParams.setSoTimeout(httpParams, 60000);
            HttpClient client = new DefaultHttpClient(httpParams);
            HttpPost post = new HttpPost(BASE_URL + API);
            List nameValuePairs = new ArrayList();
            TextSecurity sec = TextSecurity.getInstance();
            String str = pasString + "|" + customer.getCustId();
            String passEncrypt = sec.Encrypt(str);
            //System.out.println("passEncrypt:" + passEncrypt);
            nameValuePairs.add(new BasicNameValuePair("Username", userNameString));
            nameValuePairs.add(new BasicNameValuePair("Password", passEncrypt));
            nameValuePairs.add(new BasicNameValuePair("RequestId", customer.getCustId().toString()));
            nameValuePairs.add(new BasicNameValuePair("FunctionName", funString));

            nameValuePairs.add(new BasicNameValuePair("FunctionParams", request));
            post.setEntity(new UrlEncodedFormEntity(nameValuePairs));
            HttpResponse response = client.execute(post);
            logger.info("NameValuePairs:" + nameValuePairs);
            logger.info("Post response:" + post);
            BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
            StringBuilder sb = new StringBuilder();

            String strOutput;
            while ((strOutput = rd.readLine()) != null) {
                sb.append(strOutput);
            }
            content = sb.toString();

            logger.info("response createCustomerEmolaAccount isdn " + isdn + ": " + content);
            ResponseWallet responseWallet = gson.fromJson(content, ResponseWallet.class);
            String[] byPassEmolaErr = ResourceBundle.getBundle("configPayBonus").getString("ByPassEmolaErrorCode").split(";");
            ArrayList<String> listByPassEmolaErr = new ArrayList();
            listByPassEmolaErr.addAll(Arrays.asList(byPassEmolaErr));
            if (responseWallet != null && responseWallet.getResponseCode() != null) {
                if (listByPassEmolaErr.contains(responseWallet.getResponseCode())) {
                    logger.info("Create EMola customer account for isdn " + isdn + " is successful");
                } else {
                    logger.info("Create EMola customer account for isdn " + isdn + " is unsuccessful");
                    if (dbKit != null) {
                        dbKit.insertLogCallWsWallet(isdn, customer.getCustId(), content, customer.getSubName(),
                                customer.getIdNo(), "", "");
                    } else if (dbSubProfile != null) {
                        dbSubProfile.insertLogCallWsWallet(isdn, customer.getCustId(), content, customer.getSubName(),
                                customer.getIdNo(), "", "");
                    }

                }
            } else {
                logger.info("Reponse wallet is null. isdn " + isdn);
                if (dbKit != null) {
                    dbKit.insertLogCallWsWallet(isdn, customer.getCustId(), content, customer.getSubName(),
                            customer.getIdNo(), "", "");
                } else if (dbSubProfile != null) {
                    dbSubProfile.insertLogCallWsWallet(isdn, customer.getCustId(), content, customer.getSubName(),
                            customer.getIdNo(), "", "");
                }
            }
        } catch (Exception e) {
            logger.info("Have exception when Create EMola customer account for isdn " + isdn);
            if (dbKit != null) {
                dbKit.insertLogCallWsWallet(isdn, customer.getCustId(), content, customer.getSubName(),
                        customer.getIdNo(), "", "");
            } else if (dbSubProfile != null) {
                dbSubProfile.insertLogCallWsWallet(isdn, customer.getCustId(), content, customer.getSubName(),
                        customer.getIdNo(), "", "");
            }
        }
    }

    public boolean createEmolaForCustomerV2(Customer customer, String isdn, DbProcessorAbstract db) {
        String request = "";
        String content = "";
        boolean createResult = false;
        DbBonus4GChangeSim dbBonusChangeSim = null;
        try {
            if (db.getClass() == DbBonus4GChangeSim.class) {
                dbBonusChangeSim = (DbBonus4GChangeSim) db;
            }
            String BASE_URL = ResourceBundle.getBundle("configPayBonus").getString("CreateWalletForCust_wsdlUrl");
            String API = ResourceBundle.getBundle("configPayBonus").getString("api");
            String userNameString = ResourceBundle.getBundle("configPayBonus").getString("username");
            String pasString = ResourceBundle.getBundle("configPayBonus").getString("password");
            String funString = "CustomerSync";
            SubscriberEmola sub = new SubscriberEmola();
            sub.setCustId("" + customer.getCustId());
            sub.setUserName(customer.getSubName());
            sub.setMobile(isdn);
            sub.setCustomerName(customer.getSubName());
            sub.setGender(customer.getGender());
            sub.setDoB(customer.getBirthDate());
            sub.setIdType("1");
            sub.setIdNo(customer.getIdNo());
            sub.setAddress("");
            sub.setProgressType("1");//... 1>> Create new eMola
            Gson gson = new Gson();
            request = gson.toJson(sub, SubscriberEmola.class);
            logger.info("Request Wallet:" + request);
            //HttpClient client = new DefaultHttpClient();
            // set the connection timeout value to 60 seconds (60000 milliseconds)
            final HttpParams httpParams = new BasicHttpParams();
            HttpConnectionParams.setConnectionTimeout(httpParams, 60000);
            HttpConnectionParams.setSoTimeout(httpParams, 60000);
            HttpClient client = new DefaultHttpClient(httpParams);
            HttpPost post = new HttpPost(BASE_URL + API);
            List nameValuePairs = new ArrayList();
            TextSecurity sec = TextSecurity.getInstance();
            String str = pasString + "|" + customer.getCustId();
            String passEncrypt = sec.Encrypt(str);
            //System.out.println("passEncrypt:" + passEncrypt);
            nameValuePairs.add(new BasicNameValuePair("Username", userNameString));
            nameValuePairs.add(new BasicNameValuePair("Password", passEncrypt));
            nameValuePairs.add(new BasicNameValuePair("RequestId", customer.getCustId().toString()));
            nameValuePairs.add(new BasicNameValuePair("FunctionName", funString));

            nameValuePairs.add(new BasicNameValuePair("FunctionParams", request));
            post.setEntity(new UrlEncodedFormEntity(nameValuePairs));
            HttpResponse response = client.execute(post);
            logger.info("NameValuePairs:" + nameValuePairs);
            logger.info("Post response:" + post);
            BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
            StringBuilder sb = new StringBuilder();

            String strOutput;
            while ((strOutput = rd.readLine()) != null) {
                sb.append(strOutput);
            }
            content = sb.toString();

            logger.info("response createCustomerEmolaAccount isdn " + isdn + ": " + content);
            ResponseWallet responseWallet = gson.fromJson(content, ResponseWallet.class);
            String[] byPassEmolaErr = ResourceBundle.getBundle("configPayBonus").getString("ByPassEmolaErrorCode").split(";");
            ArrayList<String> listByPassEmolaErr = new ArrayList();
            listByPassEmolaErr.addAll(Arrays.asList(byPassEmolaErr));
            if (responseWallet != null && responseWallet.getResponseCode() != null) {
                if (listByPassEmolaErr.contains(responseWallet.getResponseCode())) {
                    logger.info("Create EMola customer account for isdn " + isdn + " is successful");
                    createResult = true;
                } else {
                    logger.info("Create EMola customer account for isdn " + isdn + " is failed");
                }
            } else {
                logger.info("Reponse wallet is null. isdn " + isdn);
            }
            if (dbBonusChangeSim != null) {
                dbBonusChangeSim.insertLogCallWsWallet(isdn, customer.getCustId(), content, customer.getSubName(),
                        customer.getIdNo(), "", "");
            }
            return createResult;
        } catch (Exception e) {
            logger.info("Have exception when Create EMola customer account for isdn " + isdn);
            if (dbBonusChangeSim != null) {
                dbBonusChangeSim.insertLogCallWsWallet(isdn, customer.getCustId(), content, customer.getSubName(),
                        customer.getIdNo(), "", "");
            }
            return createResult;
        }
    }

    public String callEwallet(long aciontAuditId, int channelTypeId, String mobile, long amount,
            String actionCode, String staffCode, String transId, DbProcessorAbstract db) throws Exception {

        if (amount <= 0) {
            logger.info("The value is not greater than 0 so default return success isdn " + mobile + " amount " + amount);
            return "01";
        }
        String request = "";
        String errorCode = "";
        String description = "";
        long timeSt = System.currentTimeMillis();
        EwalletLog eLog = new EwalletLog();
        DbSubProfileProcessor dbBonusFirst = null;
        DbPayBonusSecond dbBonusSecond = null;
        DbMakeSaleTranCug dbCug = null;
        DbConnectKitProcessor dbKit = null;
        DbBonusConnectKit dbBonusKit = null;
        //        LinhNBV 20181015: Add DbPayBonusAgentVip
        DbPayBonusAgentVipProcessor dbBonusAgentVip = null;
        DbMobileShopChannel dbMobileShop = null;
        DbPromotionScaner dbBranchScaner = null;
        //Bacnx 20190122: Add bonus for channel when change sim 4G
        DbBonus4GChangeSim dbBonusChannelSim4G = null;
        //Bacnx 20190320: Add bonus for update profile
        DbSubProfileUpdateProcessor dbSubProfileUpdateProcessor = null;
        DbKitBatchConnectProcessor dbKitBatchConnectProcessor = null;
        DbKitBatchSubProcessor dbKitBatchProccesser = null;
        DbBonusConnectPostpaid bonusConnectPostpaid = null;
        try {
            logger.info("Start call Ewallet isdn " + mobile + " amount " + amount);
            if (db.getClass() == DbSubProfileProcessor.class) {
                dbBonusFirst = (DbSubProfileProcessor) db;
            } else if (db.getClass() == DbPayBonusSecond.class) {
                dbBonusSecond = (DbPayBonusSecond) db;
            } else if (db.getClass() == DbMakeSaleTranCug.class) {
                dbCug = (DbMakeSaleTranCug) db;
            } else if (db.getClass() == DbConnectKitProcessor.class) {
                dbKit = (DbConnectKitProcessor) db;
            } else if (db.getClass() == DbPayBonusAgentVipProcessor.class) {//LinhNBV 20181015: Init dbBonusAgentVip to save log ewallet
                dbBonusAgentVip = (DbPayBonusAgentVipProcessor) db;
            } else if (db.getClass() == DbMobileShopChannel.class) {
                dbMobileShop = (DbMobileShopChannel) db;
            } else if (db.getClass() == DbBonusConnectKit.class) {
                dbBonusKit = (DbBonusConnectKit) db;
            } else if (db.getClass() == DbBonus4GChangeSim.class) {
                dbBonusChannelSim4G = (DbBonus4GChangeSim) db;
            } else if (db.getClass() == DbPromotionScaner.class) {
                dbBranchScaner = (DbPromotionScaner) db;
            } else if (db.getClass() == DbSubProfileUpdateProcessor.class) {
                dbSubProfileUpdateProcessor = (DbSubProfileUpdateProcessor) db;
            } else if (db.getClass() == DbKitBatchConnectProcessor.class) {
                dbKitBatchConnectProcessor = (DbKitBatchConnectProcessor) db;
            } else if (db.getClass() == DbKitBatchSubProcessor.class) {
                dbKitBatchProccesser = (DbKitBatchSubProcessor) db;
            } else if (db.getClass() == DbBonusConnectPostpaid.class) {
                bonusConnectPostpaid = (DbBonusConnectPostpaid) db;
            }

            String content = null;
            String url = ResourceBundle.getBundle("configPayBonus").getString("ewallet_url");
            String api = ResourceBundle.getBundle("configPayBonus").getString("api");
            String user = ResourceBundle.getBundle("configPayBonus").getString("username");
            String pass = ResourceBundle.getBundle("configPayBonus").getString("password");
            String funcName = ResourceBundle.getBundle("configPayBonus").getString("functionName");
            String transIdIsdn = transId + mobile;
            eLog.setMobile(mobile);
            eLog.setUrl(url);
            eLog.setUserName(user);
            eLog.setFunctionName(funcName);
            eLog.setActionCode(actionCode);
            eLog.setAmount(amount);
            eLog.setAtionAuditId(aciontAuditId);
            eLog.setChannelTypeId(channelTypeId);
            eLog.setTransId(transIdIsdn);
            eLog.setStaffCode(staffCode);
            WalletBean eWallet = new WalletBean();
            eWallet.setMobile(mobile);
            eWallet.setAmount(amount + "");
            eWallet.setActionCode(actionCode);
            eWallet.setTransID(transIdIsdn);
            Gson gson = new Gson();
            request = gson.toJson(eWallet, WalletBean.class);
            eLog.setRequest(request);
            // set the connection timeout value to 60 seconds (60000 milliseconds)
            final HttpParams httpParams = new BasicHttpParams();
            HttpConnectionParams.setConnectionTimeout(httpParams, 60000);
            HttpConnectionParams.setSoTimeout(httpParams, 60000);

            HttpClient client = new DefaultHttpClient(httpParams);
            HttpPost post = new HttpPost(url + api);
            List nameValuePairs = new ArrayList();
            TextSecurity sec = TextSecurity.getInstance();
            String str = pass + "|" + transIdIsdn;
            String passEncrypt = sec.Encrypt(str);
            nameValuePairs.add(new BasicNameValuePair("Username", user));
            nameValuePairs.add(new BasicNameValuePair("Password", passEncrypt));
            nameValuePairs.add(new BasicNameValuePair("FunctionName", funcName));
            nameValuePairs.add(new BasicNameValuePair("RequestId", transIdIsdn));
            nameValuePairs.add(new BasicNameValuePair("FunctionParams", request));
            post.setEntity(new UrlEncodedFormEntity(nameValuePairs));
            HttpResponse response = client.execute(post);
            BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
            StringBuilder sb = new StringBuilder();
            String output;
            while ((output = rd.readLine()) != null) {
                sb.append(output);
            }
            content = sb.toString();
            eLog.setRespone(content);
            Gson responseGson = new Gson();
            ResponseWallet responseWallet = responseGson.fromJson(content, ResponseWallet.class);
            if (responseWallet != null && responseWallet.getResponseCode() != null) {
                errorCode = responseWallet.getResponseCode();
                description = responseWallet.getResponseMessage();
                if ("01".equals(errorCode)) {
                    logger.info("Call eWallet success isdn " + mobile + " amount " + amount);
                } else {
                    logger.error("Call eWallet fail isdn " + mobile + " amount " + amount + " desc " + description);
                }
            } else {
                logger.error("Call eWallet error isdn " + mobile + " amount " + amount);
            }
            eLog.setErrorCode(errorCode);
            eLog.setDescription(description);
            eLog.setDuration(System.currentTimeMillis() - timeSt);
            if (dbBonusFirst != null) {
                dbBonusFirst.insertEwalletLog(eLog);
            } else if (dbBonusSecond != null) {
                dbBonusSecond.insertEwalletLog(eLog);
            } else if (dbCug != null) {
                dbCug.insertEwalletLog(eLog);
            } else if (dbKit != null) {
                dbKit.insertEwalletLog(eLog);
            } else if (dbBonusAgentVip != null) {
                dbBonusAgentVip.insertEwalletLog(eLog);
            } else if (dbMobileShop != null) {
                dbMobileShop.insertEwalletLog(eLog);
            } else if (dbBonusKit != null) {
                dbBonusKit.insertEwalletLog(eLog);
            } else if (dbBonusChannelSim4G != null) {
                dbBonusChannelSim4G.insertEwalletLog(eLog);
            } else if (dbBranchScaner != null) {
                dbBranchScaner.insertEwalletLog(eLog);
            } else if (dbSubProfileUpdateProcessor != null) {
                dbSubProfileUpdateProcessor.insertEwalletLog(eLog);
            } else if (dbKitBatchConnectProcessor != null) {
                dbKitBatchConnectProcessor.insertEwalletLog(eLog);
            } else if (dbKitBatchProccesser != null) {
                dbKitBatchProccesser.insertEwalletLog(eLog);
            } else if (bonusConnectPostpaid != null) {
                bonusConnectPostpaid.insertEwalletLog(eLog);
            }
            return errorCode;
        } catch (Exception ex) {
            logger.error("Had exception call eWallet isdn " + mobile + " amount " + amount);
            try {
                eLog.setErrorCode(errorCode);
                eLog.setDescription(description);
                eLog.setDuration(System.currentTimeMillis() - timeSt);
                if (dbBonusFirst != null) {
                    dbBonusFirst.insertEwalletLog(eLog);
                } else if (dbBonusSecond != null) {
                    dbBonusSecond.insertEwalletLog(eLog);
                } else if (dbCug != null) {
                    dbCug.insertEwalletLog(eLog);
                } else if (dbKit != null) {
                    dbKit.insertEwalletLog(eLog);
                } else if (dbBonusAgentVip != null) {
                    dbBonusAgentVip.insertEwalletLog(eLog);
                } else if (dbMobileShop != null) {
                    dbMobileShop.insertEwalletLog(eLog);
                } else if (dbBonusKit != null) {
                    dbBonusKit.insertEwalletLog(eLog);
                } else if (dbBonusChannelSim4G != null) {
                    dbBonusChannelSim4G.insertEwalletLog(eLog);
                } else if (dbBranchScaner != null) {
                    dbBranchScaner.insertEwalletLog(eLog);
                } else if (dbSubProfileUpdateProcessor != null) {
                    dbSubProfileUpdateProcessor.insertEwalletLog(eLog);
                } else if (dbKitBatchConnectProcessor != null) {
                    dbKitBatchConnectProcessor.insertEwalletLog(eLog);
                } else if (dbKitBatchProccesser != null) {
                    dbKitBatchProccesser.insertEwalletLog(eLog);
                }
            } catch (Exception e) {
                logger.error("Try insert log eWallet_Log had exception isdn " + mobile);
                logger.error(AppManager.logException(timeSt, ex));
            }
            return errorCode;
        }
    }

    public String callEwalletV2(long aciontAuditId, int channelTypeId, String mobile, long amount,
            String actionCode, String staffCode, String transId, long bonusType, DbProcessorAbstract db) throws Exception {

        if (amount <= 0) {
            logger.info("The value is not greater than 0 so default return success isdn " + mobile + " amount " + amount);
            return "01";
        }
        String request = "";
        String errorCode = "";
        String description = "";
        long timeSt = System.currentTimeMillis();
        EwalletLog eLog = new EwalletLog();
        DbSubProfileProcessor dbBonusFirst = null;
        DbPayBonusSecond dbBonusSecond = null;
        DbMakeSaleTranCug dbCug = null;
        DbConnectKitProcessor dbKit = null;
        DbBonusConnectKit dbBonusKit = null;
        //        LinhNBV 20181015: Add DbPayBonusAgentVip
        DbPayBonusAgentVipProcessor dbBonusAgentVip = null;
        DbMobileShopChannel dbMobileShop = null;
        DbPromotionScaner dbBranchScaner = null;
        //Bacnx 20190122: Add bonus for channel when change sim 4G
        DbBonus4GChangeSim dbBonusChannelSim4G = null;
        //Bacnx 20190320: Add bonus for update profile
        DbSubProfileUpdateProcessor dbSubProfileUpdateProcessor = null;
        DbKitBatchConnectProcessor dbKitBatchConnectProcessor = null;
        DbKitBatchSubProcessor dbKitBatchProccesser = null;
        DbKitChangeToStudentProcessor dbKitChangeToStudent = null;
        try {
            logger.info("Start call Ewallet isdn " + mobile + " amount " + amount);
            if (db.getClass() == DbSubProfileProcessor.class) {
                dbBonusFirst = (DbSubProfileProcessor) db;
            } else if (db.getClass() == DbPayBonusSecond.class) {
                dbBonusSecond = (DbPayBonusSecond) db;
            } else if (db.getClass() == DbMakeSaleTranCug.class) {
                dbCug = (DbMakeSaleTranCug) db;
            } else if (db.getClass() == DbConnectKitProcessor.class) {
                dbKit = (DbConnectKitProcessor) db;
            } else if (db.getClass() == DbPayBonusAgentVipProcessor.class) {//LinhNBV 20181015: Init dbBonusAgentVip to save log ewallet
                dbBonusAgentVip = (DbPayBonusAgentVipProcessor) db;
            } else if (db.getClass() == DbMobileShopChannel.class) {
                dbMobileShop = (DbMobileShopChannel) db;
            } else if (db.getClass() == DbBonusConnectKit.class) {
                dbBonusKit = (DbBonusConnectKit) db;
            } else if (db.getClass() == DbBonus4GChangeSim.class) {
                dbBonusChannelSim4G = (DbBonus4GChangeSim) db;
            } else if (db.getClass() == DbPromotionScaner.class) {
                dbBranchScaner = (DbPromotionScaner) db;
            } else if (db.getClass() == DbSubProfileUpdateProcessor.class) {
                dbSubProfileUpdateProcessor = (DbSubProfileUpdateProcessor) db;
            } else if (db.getClass() == DbKitBatchConnectProcessor.class) {
                dbKitBatchConnectProcessor = (DbKitBatchConnectProcessor) db;
            } else if (db.getClass() == DbKitBatchSubProcessor.class) {
                dbKitBatchProccesser = (DbKitBatchSubProcessor) db;
            } else if (db.getClass() == DbKitChangeToStudentProcessor.class) {
                dbKitChangeToStudent = (DbKitChangeToStudentProcessor) db;
            }

            String content = null;
            String url = ResourceBundle.getBundle("configPayBonus").getString("ewallet_url");
            String api = ResourceBundle.getBundle("configPayBonus").getString("api");
            String user = ResourceBundle.getBundle("configPayBonus").getString("username");
            String pass = ResourceBundle.getBundle("configPayBonus").getString("password");
            String funcName = ResourceBundle.getBundle("configPayBonus").getString("functionName");
            String transIdIsdn = transId + mobile;
            eLog.setMobile(mobile);
            eLog.setUrl(url);
            eLog.setUserName(user);
            eLog.setFunctionName(funcName);
            eLog.setActionCode(actionCode);
            eLog.setAmount(amount);
            eLog.setAtionAuditId(aciontAuditId);
            eLog.setChannelTypeId(channelTypeId);
            eLog.setTransId(transIdIsdn);
            eLog.setStaffCode(staffCode);
            eLog.setBonusType(bonusType);
            WalletBean eWallet = new WalletBean();
            eWallet.setMobile(mobile);
            eWallet.setAmount(amount + "");
            eWallet.setActionCode(actionCode);
            eWallet.setTransID(transIdIsdn);
            Gson gson = new Gson();
            request = gson.toJson(eWallet, WalletBean.class);
            eLog.setRequest(request);
            // set the connection timeout value to 60 seconds (60000 milliseconds)
            final HttpParams httpParams = new BasicHttpParams();
            HttpConnectionParams.setConnectionTimeout(httpParams, 60000);
            HttpConnectionParams.setSoTimeout(httpParams, 60000);

            HttpClient client = new DefaultHttpClient(httpParams);
            HttpPost post = new HttpPost(url + api);
            List nameValuePairs = new ArrayList();
            TextSecurity sec = TextSecurity.getInstance();
            String str = pass + "|" + transIdIsdn;
            String passEncrypt = sec.Encrypt(str);
            nameValuePairs.add(new BasicNameValuePair("Username", user));
            nameValuePairs.add(new BasicNameValuePair("Password", passEncrypt));
            nameValuePairs.add(new BasicNameValuePair("FunctionName", funcName));
            nameValuePairs.add(new BasicNameValuePair("RequestId", transIdIsdn));
            nameValuePairs.add(new BasicNameValuePair("FunctionParams", request));
            post.setEntity(new UrlEncodedFormEntity(nameValuePairs));
            HttpResponse response = client.execute(post);
            BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
            StringBuilder sb = new StringBuilder();
            String output;
            while ((output = rd.readLine()) != null) {
                sb.append(output);
            }
            content = sb.toString();
            eLog.setRespone(content);
            Gson responseGson = new Gson();
            ResponseWallet responseWallet = responseGson.fromJson(content, ResponseWallet.class);
            if (responseWallet != null && responseWallet.getResponseCode() != null) {
                errorCode = responseWallet.getResponseCode();
                description = responseWallet.getResponseMessage();
                if ("01".equals(errorCode)) {
                    logger.info("Call eWallet success isdn " + mobile + " amount " + amount);
                } else {
                    logger.error("Call eWallet fail isdn " + mobile + " amount " + amount + " desc " + description);
                }
            } else {
                logger.error("Call eWallet error isdn " + mobile + " amount " + amount);
            }
            eLog.setErrorCode(errorCode);
            eLog.setDescription(description);
            eLog.setDuration(System.currentTimeMillis() - timeSt);
            if (dbBonusFirst != null) {
                dbBonusFirst.insertEwalletLog(eLog);
            } else if (dbBonusSecond != null) {
                dbBonusSecond.insertEwalletLog(eLog);
            } else if (dbCug != null) {
                dbCug.insertEwalletLog(eLog);
            } else if (dbKit != null) {
                dbKit.insertEwalletLog(eLog);
            } else if (dbBonusAgentVip != null) {
                dbBonusAgentVip.insertEwalletLog(eLog);
            } else if (dbMobileShop != null) {
                dbMobileShop.insertEwalletLog(eLog);
            } else if (dbBonusKit != null) {
                dbBonusKit.insertEwalletLog(eLog);
            } else if (dbBonusChannelSim4G != null) {
                dbBonusChannelSim4G.insertEwalletLog(eLog);
            } else if (dbBranchScaner != null) {
                dbBranchScaner.insertEwalletLog(eLog);
            } else if (dbSubProfileUpdateProcessor != null) {
                dbSubProfileUpdateProcessor.insertEwalletLog(eLog);
            } else if (dbKitBatchConnectProcessor != null) {
                dbKitBatchConnectProcessor.insertEwalletLog(eLog);
            } else if (dbKitBatchProccesser != null) {
                dbKitBatchProccesser.insertEwalletLog(eLog);
            } else if (dbKitChangeToStudent != null) {
                dbKitChangeToStudent.insertEwalletLog(eLog);
            }
            return errorCode;
        } catch (Exception ex) {
            logger.error("Had exception call eWallet isdn " + mobile + " amount " + amount);
            try {
                eLog.setErrorCode(errorCode);
                eLog.setDescription(description);
                eLog.setDuration(System.currentTimeMillis() - timeSt);
                if (dbBonusFirst != null) {
                    dbBonusFirst.insertEwalletLog(eLog);
                } else if (dbBonusSecond != null) {
                    dbBonusSecond.insertEwalletLog(eLog);
                } else if (dbCug != null) {
                    dbCug.insertEwalletLog(eLog);
                } else if (dbKit != null) {
                    dbKit.insertEwalletLog(eLog);
                } else if (dbBonusAgentVip != null) {
                    dbBonusAgentVip.insertEwalletLog(eLog);
                } else if (dbMobileShop != null) {
                    dbMobileShop.insertEwalletLog(eLog);
                } else if (dbBonusKit != null) {
                    dbBonusKit.insertEwalletLog(eLog);
                } else if (dbBonusChannelSim4G != null) {
                    dbBonusChannelSim4G.insertEwalletLog(eLog);
                } else if (dbBranchScaner != null) {
                    dbBranchScaner.insertEwalletLog(eLog);
                } else if (dbSubProfileUpdateProcessor != null) {
                    dbSubProfileUpdateProcessor.insertEwalletLog(eLog);
                } else if (dbKitBatchConnectProcessor != null) {
                    dbKitBatchConnectProcessor.insertEwalletLog(eLog);
                } else if (dbKitBatchProccesser != null) {
                    dbKitBatchProccesser.insertEwalletLog(eLog);
                } else if (dbKitChangeToStudent != null) {
                    dbKitChangeToStudent.insertEwalletLog(eLog);
                }
            } catch (Exception e) {
                logger.error("Try insert log eWallet_Log had exception isdn " + mobile);
                logger.error(AppManager.logException(timeSt, ex));
            }
            return errorCode;
        }
    }

    public String addSmsDataVoice(String isdn, String value, String balanceId, String expireTime) {
        long timeSt = System.currentTimeMillis();
        ExchMsg request = new ExchMsg();
        ExchMsg response = null;
        String err = "";
        try {
            logger.info("Start addSmsDataVoice for sub " + isdn + " value " + value + " accountid " + balanceId);
            request.setCommand("OCSHW_ADJUST_SMS_DATA");
            if (!isdn.startsWith("258")) {
                isdn = "258" + isdn;
            }
            request.set("ISDN", isdn.trim());
            request.set("ACCOUNT_TYPE", balanceId.trim());
            request.set("VALIDITY_INCREMENT", "0");
            request.set("AMOUNT", value.trim());
            if (expireTime != null && !"".endsWith(expireTime)) {
                request.set("EXPIRE_DATE", expireTime);
            }
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            err = response.getError();
            logger.info("End addSmsDataVoice isdn " + isdn + " amount " + value + " balanceid " + balanceId + " result " + err);
            return err;
        } catch (Exception ex) {
            logger.error("Had exception addSmsDataVoice isdn " + isdn + " amount " + value);
            logger.error(AppManager.logException(timeSt, ex));
            return err;
        }
    }

    public String addMoney(String isdn, String money, String balanceId) {
        long timeSt = System.currentTimeMillis();
        ExchMsg request = new ExchMsg();
        ExchMsg response = null;
        String err = "";
        try {
            logger.info("Start addMoney for sub " + isdn + " value " + money + " accountid " + balanceId);
            request.setCommand("OCSHW_ADJUSTACCOUNT");
            if (!isdn.startsWith("258")) {
                request.set("ISDN", "258" + isdn.trim());
            } else {
                request.set("ISDN", isdn.trim());
            }
            request.set("ACCOUNT_TYPE", balanceId.trim());
            request.set("AMOUNT", money.trim());
            request.set("VALIDITY_INCREMENT", "0");
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            err = response.getError();
            logger.info("End addMoney isdn " + isdn + " amount " + money + " balanceid " + balanceId + " result " + err);
            return err;
        } catch (Exception ex) {
            logger.error("Had exception addMoney isdn " + isdn + " amount " + money);
            logger.error(AppManager.logException(timeSt, ex));
            return err;
        }
    }

    public String topupPrePaid(String msisdn, String money) {
        long timeSt = System.currentTimeMillis();
        ExchMsg response = null;
        String err = "";
        try {
            logger.info("start topupPrePaid for sub " + msisdn + " money " + money);
            ExchMsg request = new ExchMsg();
            request.setCommand("OCSHW_PAYMENT");
            request.set("ISDN", msisdn);
            request.set("AMOUNT", money);
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            if (response == null) {
                logger.error("ERROR topupPrePaid, response is null sub " + msisdn + " money " + money);
            }
            err = response.getError();
            if ("0".equals(err)) {
                logger.info("topupPrePaid success for sub " + msisdn + " money " + money);
            } else {
                logger.error("ERROR topupPrePaid msisdn " + msisdn + " money " + money + " detail respone:\n" + response);
            }
        } catch (Exception ex) {
            StringBuilder br = new StringBuilder();
            br.setLength(0);
            br.append("ERROR TopupMoney msisdn ");
            br.append(msisdn);
            br.append(" money ");
            br.append(money);
            if (response != null) {
                br.append(" RESPONSE:\n").append(response);
            } else {
                br.append(" Response is null");
            }
            logger.error(br, ex);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            return err;
        }
    }

    public String addPrice(String msisdn, String planId, String effectDate, String addDay) {
        ExchMsg request = new ExchMsg();
        long timeSt = System.currentTimeMillis();
        ExchMsg response = null;
        String err = "";
        try {
            request.setCommand("OCSHW_SUBSCRIBEPRODUCT");
            if (msisdn.startsWith("258")) {
                request.set("MSISDN", msisdn);
            } else {
                request.set("MSISDN", "258" + msisdn);
            }
            request.set("PRODUCTID", planId);
            if (!"".equals(effectDate)) {
                request.set("EFFECTIVE_DATE", effectDate);
            }
            if (!"".equals(addDay)) {
                Calendar cal = Calendar.getInstance();
                cal.setTime(new Date());
                cal.add(Calendar.DATE, Integer.valueOf(addDay) + 1); //20180807 add one more day because OCS trunc to begining of day
                request.set("EXPIRE_DATE", sdf.format(cal.getTime()));
            }
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            logger.info(response != null ? response.toString() : " response null");
            err = response.getError();
            logger.info("End addPrice isdn " + msisdn + " planId " + planId + " result " + err);
            return err;
        } catch (Exception ex) {
            logger.error("Had exception addPrice isdn " + msisdn + " planId " + planId);
            logger.error(AppManager.logException(timeSt, ex));
            return err;
        }
    }

    public String addPriceV4(String msisdn, String planId, String effectDate, String expireTime) {
        ExchMsg request = new ExchMsg();
        long timeSt = System.currentTimeMillis();
        ExchMsg response = null;
        String err = "";
        try {
            request.setCommand("OCSHW_SUBSCRIBEPRODUCT");
            if (msisdn.startsWith("258")) {
                request.set("MSISDN", msisdn);
            } else {
                request.set("MSISDN", "258" + msisdn);
            }
            request.set("PRODUCTID", planId);
            if (!"".equals(effectDate)) {
                request.set("EFFECTIVE_DATE", effectDate);
            }
            if (!"".equals(expireTime)) {
                request.set("EXPIRE_DATE", expireTime);
            }
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            err = response.getError();
            logger.info("End addPrice isdn " + msisdn + " planId " + planId + " result " + err);
            return err;
        } catch (Exception ex) {
            logger.error("Had exception addPrice isdn " + msisdn + " planId " + planId);
            logger.error(AppManager.logException(timeSt, ex));
            return err;
        }
    }

//LinhNBV 20180815: edit method addPrice
    public String addPriceV2(String msisdn, String planId, String effectDate, String expireDate) {
        ExchMsg request = new ExchMsg();
        long timeSt = System.currentTimeMillis();
        ExchMsg response = null;
        String err = "";
        try {
            request.setCommand("OCSHW_SUBSCRIBEPRODUCT3");
            if (msisdn.startsWith("258")) {
                request.set("MSISDN", msisdn);
            } else {
                request.set("MSISDN", "258" + msisdn);
            }
            request.set("PRODUCTID", planId);
            if (!"".equals(effectDate)) {
                request.set("EFFECTIVE_DATE", effectDate);
            }
            if (!"".equals(expireDate)) {
                request.set("EXPIRE_DATE", expireDate);
            }
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            err = response.getError();
            logger.info("End addPrice isdn " + msisdn + " planId " + planId + " result " + err);
            return err;
        } catch (Exception ex) {
            logger.error("Had exception addPrice isdn " + msisdn + " planId " + planId);
            logger.error(AppManager.logException(timeSt, ex));
            return err;
        }
    }

    public String activeFlagSABLCK(String msisdn) {
        ExchMsg request = new ExchMsg();
        ExchMsg response = null;
        long start = System.currentTimeMillis();
        try {
            request.setCommand("HLR_HW_MODI_SUB_SABLCK");
            request.set("MSISDN", msisdn);
            request.set("OC", "FALSE");
            request.set("ClientTimeout", "25000");
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            logTime("Time to activeFlagSABLCK sub " + msisdn + " result " + response.getError(), start);
            return response.getError();
        } catch (Exception e) {
            logTime("Exception to activeFlagSABLCK sub " + msisdn, start);
            AppManager.logException(start, e);
            return "";
        }
    }

    public String activeFlagBAOC(String msisdn) {
        ExchMsg request = new ExchMsg();
        ExchMsg response = null;
        long start = System.currentTimeMillis();
        try {
            request.setCommand("HLR_HW_MOD_BAOC");
            request.set("MSISDN", msisdn);
            request.set("PROVISION", "FALSE");
            request.set("ClientTimeout", "25000");
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            logTime("Time to activeFlagBAOC sub " + msisdn + " result " + response.getError(), start);
            return response.getError();
        } catch (Exception e) {
            logTime("Exception to activeFlagBAOC sub " + msisdn, start);
            AppManager.logException(start, e);
            return "";
        }
    }
//    Lamnt inactive BAOC

    public String inActiveFlagBAOC(String msisdn) {
        ExchMsg request = new ExchMsg();
        ExchMsg response = null;
        long start = System.currentTimeMillis();
        try {
            request.setCommand("HLR_HW_MOD_BAOC");
            request.set("MSISDN", msisdn);
            request.set("PROVISION", "TRUE");
            request.set("ClientTimeout", "25000");
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            logTime("Time to inActiveFlagBAOC sub " + msisdn + " result " + response.getError(), start);
            return response.getError();
        } catch (Exception e) {
            logTime("Exception to inActiveFlagBAOC sub " + msisdn, start);
            AppManager.logException(start, e);
            return "";
        }
    }
//    Lamnt commit BAOC

    public String commitFlagBAOC(String msisdn) {
        ExchMsg request = new ExchMsg();
        ExchMsg response = null;
        long start = System.currentTimeMillis();
        try {
            request.setCommand("HLR_HW_ACT_BAOC");
            request.set("MSISDN", msisdn);
            request.set("BSG", "ALL");
            request.set("ClientTimeout", "25000");
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            logTime("Time to commitFlagBAOC sub " + msisdn + " result " + response.getError(), start);
            return response.getError();
        } catch (Exception e) {
            logTime("Exception to commitFlagBAOC sub " + msisdn, start);
            AppManager.logException(start, e);
            return "";
        }
    }
    //LinhNBV 20180611: Add method active flag BAIC

    public String activeFlagBAIC(String msisdn) {
        ExchMsg request = new ExchMsg();
        ExchMsg response = null;
        long start = System.currentTimeMillis();
        try {
            request.setCommand("HLR_HW_MOD_BAIC");
            request.set("MSISDN", msisdn);
            request.set("PROVISION", "FALSE");
            request.set("ClientTimeout", "25000");
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            logTime("Time to activeFlagBAIC sub " + msisdn + " result " + response.getError(), start);
            return response.getError();
        } catch (Exception e) {
            logTime("Exception to activeFlagBAIC sub " + msisdn, start);
            AppManager.logException(start, e);
            return "";
        }
    }

    public String activeFlagGPRSLCK(String msisdn) {
        ExchMsg request = new ExchMsg();
        ExchMsg response = null;
        long start = System.currentTimeMillis();
        try {
            request.setCommand("HLR_HW_MODI_SUB_GPRS");
            request.set("MSISDN", msisdn);
            request.set("LOCK", "FALSE");
            request.set("ClientTimeout", "25000");
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            logTime("Time to activeFlagGPRSLCK sub " + msisdn + " result " + response.getError(), start);
            return response.getError();
        } catch (Exception e) {
            logTime("Exception to activeFlagGPRSLCK sub " + msisdn, start);
            AppManager.logException(start, e);
            return "";
        }
    }
//LamNT Inactive Flag GPRSLCK

    public String inActiveFlagGPRSLCK(String msisdn) {
        ExchMsg request = new ExchMsg();
        ExchMsg response = null;
        long start = System.currentTimeMillis();
        try {
            request.setCommand("HLR_HW_MODI_SUB_GPRS");
            request.set("MSISDN", msisdn);
            request.set("LOCK", "TRUE");
            request.set("ClientTimeout", "25000");
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            logTime("Time to activeFlagGPRSLCK sub " + msisdn + " result " + response.getError(), start);
            return response.getError();
        } catch (Exception e) {
            logTime("Exception to activeFlagGPRSLCK sub " + msisdn, start);
            AppManager.logException(start, e);
            return "";
        }
    }

//LamNT Inactive Flag OCSHW_MODI_VALIDITY
    public String inActiveOCS(String msisdn) {
        ExchMsg request = new ExchMsg();
        ExchMsg response = null;
        long start = System.currentTimeMillis();
        try {
            request.setCommand("OCSHW_MODI_VALIDITY");
            request.set("MSISDN", msisdn);
            request.set("VALIDITY_INCREMENT", "-1");
            request.set("ClientTimeout", "25000");
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            logTime("Time to inActiveOCS sub " + msisdn + " result " + response.getError(), start);
            return response.getError();
        } catch (Exception e) {
            logTime("Exception to inActiveOCS sub " + msisdn, start);
            AppManager.logException(start, e);
            return "";
        }
    }
//LamNT active Flag OCSHW_MODI_VALIDITY

    public String activeOCS(String msisdn) {
        ExchMsg request = new ExchMsg();
        ExchMsg response = null;
        long start = System.currentTimeMillis();
        try {
            request.setCommand("OCSHW_MODI_VALIDITY");
            request.set("MSISDN", msisdn);
            request.set("VALIDITY_INCREMENT", "90");
            request.set("ClientTimeout", "25000");
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            logTime("Time to activeOCS sub " + msisdn + " result " + response.getError(), start);
            return response.getError();
        } catch (Exception e) {
            logTime("Exception to activeOCS sub " + msisdn, start);
            AppManager.logException(start, e);
            return "";
        }
    }
//LamNT active Flag OCSHW_ACTIVEFIRST

    public String activeOCSACTIVEFIRST(String msisdn) {
        ExchMsg request = new ExchMsg();
        ExchMsg response = null;
        long start = System.currentTimeMillis();
        try {
            request.setCommand("OCSHW_ACTIVEFIRST");
            request.set("ISDN", msisdn);
            request.set("ClientTimeout", "25000");
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            logTime("Time to active sub " + msisdn, start);
            return response.getError();
        } catch (Exception e) {
            logTime("Exception to active sub " + msisdn, start);
            logger.error(AppManager.logException(start, e));
            return "";
        }
    }

    public String activeFlagPLMNSS(String msisdn) {
        // PROVISION;FALSE;PLMNSPECSS;PLMN-SS-D
        ExchMsg request = new ExchMsg();
        ExchMsg response = null;
        long start = System.currentTimeMillis();
        try {
            request.setCommand("HLR_HW_MOD_PLMNSS");
            request.set("MSISDN", msisdn);
            request.set("PROVISION", "FALSE");
            request.set("PLMNSPECSS", "PLMN-SS-D");
            request.set("ClientTimeout", "25000");
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            logTime("Time to activeFlagPLMNSS sub " + msisdn + " result " + response.getError(), start);
            return response.getError();
        } catch (Exception e) {
            logTime("Exception to activeFlagPLMNSS sub " + msisdn, start);
            AppManager.logException(start, e);
            return "";
        }
    }
//LamNT  inActiveFlagPLMNSS HLR_HW_MOD_PLMNSS

    public String inActiveFlagPLMNSS(String msisdn) {
        // PROVISION;FALSE;PLMNSPECSS;PLMN-SS-D
        ExchMsg request = new ExchMsg();
        ExchMsg response = null;
        long start = System.currentTimeMillis();
        try {
            request.setCommand("HLR_HW_MOD_PLMNSS");
            request.set("MSISDN", msisdn);
            request.set("PROVISION", "TRUE");
            request.set("PLMNSPECSS", "PLMN-SS-D");
            request.set("ClientTimeout", "25000");
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            logTime("Time to inActiveFlagPLMNSS sub " + msisdn + " result " + response.getError(), start);
            return response.getError();
        } catch (Exception e) {
            logTime("Exception to inActiveFlagPLMNSS sub " + msisdn, start);
            AppManager.logException(start, e);
            return "";
        }
    }

    public String lockGPRS(String isdn) {
        long timeSt = System.currentTimeMillis();
        ExchMsg request = new ExchMsg();
        ExchMsg response = null;
        String err = "";
        try {
            logger.info("Start lockGPRS for sub " + isdn);
            request.setCommand("HLR_HW_MODI_SUB_GPRS");
            if (!isdn.startsWith("258")) {
                isdn = "258" + isdn;
            }
            request.set("MSISDN", isdn);
            request.set("LOCK", "TRUE");
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            err = response.getError();
            logger.info("End lockGPRS isdn " + isdn + " result " + err);
            return err;
        } catch (Exception ex) {
            logTime("Exception to lockGPRS msisdn:  " + isdn, timeSt);
            ex.printStackTrace();
            return err;
        }
    }

    public String bundleAdjustGroup(String groupId, String groupName, String groupType, String operationType) {
        ExchMsg request = new ExchMsg();
        ExchMsg response = null;
        long start = System.currentTimeMillis();
        try {
            request.setCommand("OCSHW_MANCUGINFO");
            request.set("OPERATION_TYPE", operationType);
            request.set("GROUP_NAME", groupName);
            request.set("GROUP_ID", groupId);
            request.set("GROUP_TYPE", groupType);
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            logTime("Time to bundleAdjustroup groupId " + groupId + ", groupName " + groupName + " operationType " + operationType
                    + ", groupType " + groupType + " errorcode " + response.getError(), start);
            return response.getError();
        } catch (Exception e) {
            logTime("Exception to bundleAdjustroup groupId " + groupId + ", groupName " + groupName, start);
            logger.error(AppManager.logException(start, e));
            return "";
        }
    }

    public String bundleAdjustMember(String msisdn, String operationType, String groupId, String groupType) {
        ExchMsg request = new ExchMsg();
        ExchMsg response = null;
        long start = System.currentTimeMillis();
        try {
            request.setCommand("OCSHW_MANGROUPMEMBER");
            if (!msisdn.startsWith("258")) {
                msisdn = "258" + msisdn;
            }
            request.set("MSISDN", msisdn);
            request.set("OPERATION_TYPE", operationType); //1 add, 2 delete
            request.set("USER_GROUP_ID", groupId);
            request.set("GROUP_TYPE", groupType);
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            logTime("Time to bundleAdjustMember msisdn " + msisdn + ", groupId " + groupId
                    + ", operationType " + operationType + " errorcode " + response.getError(), start);
            return response.getError();
        } catch (Exception e) {
            logTime("Exception to bundleAdjustMember msisdn " + msisdn + ", groupId " + groupId + ", operationType " + operationType, start);
            logger.error(AppManager.logException(start, e));
            return "";
        }
    }

    public String removePrice(String msisdn, String planId) {
        ExchMsg request = new ExchMsg();
        long timeSt = System.currentTimeMillis();
        ExchMsg response = null;
        String err = "";
        try {
            request.setCommand("OCSHW_UNSUBSCRIBEPRODUCT");
            request.set("MSISDN", msisdn);
            request.set("PRODUCTID", planId);
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            err = response.getError();
            logger.info("End removePrice isdn " + msisdn + " planId " + planId + " result " + err);
            return err;
        } catch (Exception ex) {
            logger.error("Had exception removePrice isdn " + msisdn + " planId " + planId);
            logger.error(AppManager.logException(timeSt, ex));
            return err;
        }
    }

    //LinhNBV start modified on April 10 2018: Add method view account info.
    public boolean checkActiveIsdn(String msisdn) {
        long timeSt = System.currentTimeMillis();
        boolean result = false;
        ExchMsg response = null;
        msisdn = msisdn.startsWith("258") ? msisdn : "258" + msisdn;
        try {
            logger.info("start to checkActiveIsdn for sub " + msisdn);
            ExchMsg request = new ExchMsg();
            request.setCommand("OCSHW_INTEGRATIONENQUIRY");
            request.set("MSISDN", msisdn);
//            logger.info("Before send " + request.toString());
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
//            logger.info("After send " + response.toString());
            if (response == null || response.getError() == null) {
                logger.error("ERROR checkActiveIsdn, response is null, sub " + msisdn);
                return result;
            }
            if (response.getError().equals("0")) {
                String lifeCycleState = (String) response.get("LIFE_CYCLE_STATE");
                logger.info("Life cycle State " + msisdn + ": " + lifeCycleState);
                if (lifeCycleState != null && lifeCycleState.trim().length() > 0 && "2".equals(lifeCycleState)) {
                    result = true;
                }
            } else {
                logger.error("ERROR checkActiveIsdn, sub " + msisdn + " detail response: " + response);
            }
        } catch (Exception ex) {
            StringBuilder br = new StringBuilder();
            br.setLength(0);
            br.append("ERROR checkActiveIsdn, msisdn ");
            br.append(msisdn);
            if (response != null) {
                br.append(" RESPONSE:\n").append(response);
            } else {
                br.append(" Response is null");
            }
            logger.error(br.toString(), ex);
            logger.error(AppManager.logException(timeSt, ex));
        }
        return result;
    }

    public String callEwalletRollback(EwalletDebitLog newLog, DbDebitRollback db) throws Exception {
        long timeSt = System.currentTimeMillis();
        String request = "";
        String response = "";
        String erroCode = "";
        String description = "";
        try {
            logger.info("Start call callEwalletRollback staff " + newLog.getStaffCode()
                    + " orgRequestId " + newLog.getRequestId());
            String strUrl = ResourceBundle.getBundle("configPayBonus").getString("DebitEwalletUrl");
//            String api = ResourceBundle.getBundle("configPayBonus").getString("api");            
            String funcName = ResourceBundle.getBundle("configPayBonus").getString("DebitEwalletFunctionName");
            String partnerCode = ResourceBundle.getBundle("configPayBonus").getString("DebitEwalletPartnerCode");
            String requestDate = sdf.format(new Date());
            String requestId = "0" + newLog.getRequestId();
            RevertTransaction paymentVoucher = new RevertTransaction();
            paymentVoucher.setRequestId(requestId);
            paymentVoucher.setRequestDate(requestDate);
            paymentVoucher.setPartnerCode(partnerCode);
            paymentVoucher.setMobile(newLog.getIsdn());
            paymentVoucher.setAmount(newLog.getMoney() + "");
            paymentVoucher.setOrgRequestId(newLog.getRequestId());
            String tempSignature = "Az1gW2WHRlzus3LqNB63kedzkWk6OjfUOrXUj7nw" + requestDate + partnerCode + requestId + newLog.getRequestId();
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(tempSignature.getBytes());
            byte[] digest = md.digest();
            StringBuilder sb = new StringBuilder();
            for (byte b : digest) {
                sb.append(String.format("%02x", b & 0xff));
            }
            paymentVoucher.setSignature(sb.toString());
            Gson gson = new Gson();
            request = gson.toJson(paymentVoucher, RevertTransaction.class);
            URL url = new URL(strUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setConnectTimeout(60000);
            conn.setReadTimeout(60000);
            conn.setRequestProperty("Authorization", "Basic TW92aXRlbDpNb3ZpdGVsMTIzIUAj");
            conn.setRequestProperty("Accept", "application/json");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);
            conn.getOutputStream().write(request.getBytes());
            Reader in = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));
            StringBuilder sbJSON = new StringBuilder();
            for (int c; (c = in.read()) >= 0;) {
                sbJSON.append((char) c);
            }
            Long endTime = System.currentTimeMillis();
            response = sbJSON.toString();
            newLog.setRequest(request);
            newLog.setRespone(response);
            newLog.setFunctionName(funcName);
            newLog.setUrl(strUrl);
            newLog.setDuration(endTime - timeSt);
            Gson responseGson = new Gson();
            ResponseWallet responseWallet = responseGson.fromJson(response, ResponseWallet.class);
            erroCode = responseWallet.getResponseCode();
            description = responseWallet.getResponseMessage();
            newLog.setErrorCode(erroCode);
            newLog.setDescription(description);
            db.saveEwalletDebigLog(newLog, requestId);
            return erroCode;
        } catch (Exception ex) {
            logger.error("Had exception call callEwalletRollback staff " + newLog.getStaffCode()
                    + " orgRequestId " + newLog.getRequestId());
            logger.error(AppManager.logException(timeSt, ex));
            return "";
        }
    }
    //LinhNBV 2180725: Call command HLR_HW_MODI_GPRS

    public String activeFlagMODIGPRS(String msisdn, String tplId) {
        ExchMsg request = new ExchMsg();
        ExchMsg response = null;
        long start = System.currentTimeMillis();
        try {
            request.setCommand("HLR_HW_MODI_GPRS");
            request.set("MSISDN", msisdn);
            request.set("PROVISION", "TRUE");
            request.set("TPLID", tplId);
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            logTime("Time to activeFlagMODIGPRS sub " + msisdn + " result " + response.getError(), start);
            return response.getError();
        } catch (Exception e) {
            logTime("Exception to activeFlagMODIGPRS sub " + msisdn, start);
            AppManager.logException(start, e);
            return "";
        }
    }

    //LinhNBV 2180725: Call command HLR_HW_QUERY_KI
    public String checkKiSim(String msisdn, String imsi) {
        ExchMsg request = new ExchMsg();
        ExchMsg response = null;
        long start = System.currentTimeMillis();
        try {
            request.setCommand("HLR_HW_QUERY_KI");
            request.set("IMSI", imsi);
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            logTime("Time to checkKiSim sub " + msisdn + " imsi " + imsi + " result " + response.getError(), start);
            return response.getError();
        } catch (Exception e) {
            logTime("Exception to checkKiSim sub " + msisdn + " imsi " + imsi, start);
            AppManager.logException(start, e);
            return "";
        }
    }

    //LinhNBV 2180725: Call command HLR_HW_ADD_KI
    public String addKiSim(String msisdn, String kiValue, String imsi, String k4sno, boolean isSim4G) {
        ExchMsg request = new ExchMsg();
        ExchMsg response = null;
        long start = System.currentTimeMillis();
        try {

            if (isSim4G) {
                request.setCommand("HLR_HW_ADD_KI_4G");
                request.set("CARDTYPE", "USIM");
                request.set("ALG", "MILENAGE");
                request.set("K4SNO", "2");
                request.set("OPSNO", "2");
            } else {
                request.setCommand("HLR_HW_ADD_KI");
                request.set("CARDTYPE", "SIM");
                request.set("ALG", "COMP128_2");
                request.set("K4SNO", k4sno);
            }

            request.set("OPERTYPE", "ADD");
            request.set("KIVALUE", kiValue);
            request.set("HLRSN", "1");
            request.set("IMSI", imsi);

            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            logTime("Time to addKiSim sub " + msisdn + " imsi: " + imsi + " result " + response.getError(), start);
            return response.getError();
        } catch (Exception e) {
            logTime("Exception to addKiSim sub " + msisdn + " imsi: " + imsi, start);
            AppManager.logException(start, e);
            return "";
        }
    }

    public AccountInfo viewAccInfo(String msisdn) {
        long timeSt = System.currentTimeMillis();
        AccountInfo accounts = null;
        ExchMsg response = null;
        SimpleDateFormat sdfVOCS = new SimpleDateFormat("M/d/yyyy hh:mm:ss aa", Locale.getDefault());
        try {
            logger.info("start to viewAccInfo for sub " + msisdn);
            ExchMsg request = new ExchMsg();
            request.setCommand("OCSHW_INTEGRATIONENQUIRY");
            if (!msisdn.startsWith("258")) {
                msisdn = "258" + msisdn;
            }
            request.set("MSISDN", msisdn);
//            logger.info("Before send " + request.toString());
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
//            logger.info("After send " + response.toString());
            accounts = new AccountInfo();
            accounts.setErr("ERROR");
            if (response == null || response.getError() == null) {
                logger.error("ERROR getAccountInfo, response is null, sub " + msisdn);
                accounts.setErr("Response getAccount is null");
                return accounts;
            }
            if (response.getError().equals("0")) {
                accounts.setErr(response.getError());
                String productStr = (String) response.get("PRODUCT_LIST");
                String expireStr = (String) response.get("EXPIRED_DATE_LIST");
                logger.info("list product of sub " + msisdn + ": " + productStr);
                logger.info("list Expire time of sub " + msisdn + ": " + expireStr);
                if (productStr != null && productStr.trim().length() > 0) {
                    String[] productInfo = productStr.split("&");
                    String[] expireInfo = expireStr.split("&");
                    for (int idx = 0; idx < productInfo.length; idx++) {
                        String bal = accounts.getAccount(productInfo[idx]);
                        Date expire = sdfVOCS.parse(expireInfo[idx]);
                        if (expire.getTime() > System.currentTimeMillis()) {
                            if (bal != null) {
                                accounts.putAccount(productInfo[idx], bal);
                            } else {
                                accounts.putProductExpire(productInfo[idx], new SimpleDateFormat("yyyyMMddHHmmss").format(expire));
                            }
                        }
                    }
                } else {
                    logger.error("ERROR getAccountInfo, account type null, sub " + msisdn);
                }
            } else {
                logger.error("ERROR getAccountInfo, sub " + msisdn + " detail response: " + response);
                accounts.setErr(response.getError());
                accounts.setDesc(response.getDescription());
            }
        } catch (Exception ex) {
            StringBuilder br = new StringBuilder();
            br.setLength(0);
            br.append("ERROR viewAccInfo, msisdn ");
            br.append(msisdn);
            if (response != null) {
                br.append(" RESPONSE:\n").append(response);
            } else {
                br.append(" Response is null");
            }
            logger.error(br.toString(), ex);
            logger.error(AppManager.logException(timeSt, ex));
        }
        return accounts;
    }

    public String getStatusTransaction(EwalletDebitLog newLog, DbDebitRollback db) throws Exception {
        long timeSt = System.currentTimeMillis();
        String request = "";
        String response = "";
        String erroCode = "";
        String description = "";
        try {
            logger.info("Start getStatusTransaction staff " + newLog.getStaffCode()
                    + " orgRequestId " + newLog.getRequestId());
            String strUrl = ResourceBundle.getBundle("configPayBonus").getString("urlGetStatusTransaction");
            String partnerCode = ResourceBundle.getBundle("configPayBonus").getString("DebitEwalletPartnerCode");//base.key.api.emola
            String keyGetStatus = ResourceBundle.getBundle("configPayBonus").getString("keyGetStatus");//
            String requestDate = sdf.format(new Date());
            String requestId = partnerCode + requestDate;

            RevertTransaction paymentVoucher = new RevertTransaction();
            paymentVoucher.setRequestId(requestId);
            paymentVoucher.setOrgRequestId(newLog.getRequestId());
            paymentVoucher.setPartnerCode(partnerCode);
            paymentVoucher.setRequestDate(requestDate);

            String tempSignature = keyGetStatus + requestDate + partnerCode + requestId + newLog.getRequestId();
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(tempSignature.getBytes());
            byte[] digest = md.digest();
            StringBuilder sb = new StringBuilder();
            for (byte b : digest) {
                sb.append(String.format("%02x", b & 0xff));
            }
            logger.info("original:" + tempSignature);
            logger.info("digested(hex):" + sb.toString());
            paymentVoucher.setSignature(sb.toString());
            Gson gson = new Gson();
            request = gson.toJson(paymentVoucher, RevertTransaction.class);
            URL url = new URL(strUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setConnectTimeout(60000);
            conn.setReadTimeout(60000);
            conn.setRequestProperty("Authorization", "Basic TW92aXRlbDpNb3ZpdGVsMTIzIUAj");
            conn.setRequestProperty("Accept", "application/json");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);
            conn.getOutputStream().write(request.getBytes());
            Reader in = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));
            StringBuilder sbJSON = new StringBuilder();
            for (int c; (c = in.read()) >= 0;) {
                sbJSON.append((char) c);
            }
            Long endTime = System.currentTimeMillis();
            response = sbJSON.toString();
            newLog.setRequest(request);
            newLog.setRespone(response);
            newLog.setFunctionName("getStatusTransaction");
            newLog.setUrl(strUrl);
            newLog.setDuration(endTime - timeSt);
            Gson responseGson = new Gson();
            ResponseWallet responseWallet = responseGson.fromJson(response, ResponseWallet.class);
            erroCode = responseWallet.getResponseCode();
            description = responseWallet.getResponseMessage();
            newLog.setErrorCode(erroCode);
            newLog.setDescription(description);
//            db.saveEwalletDebigLog(newLog, requestId);
            return erroCode;
        } catch (Exception ex) {
            logger.error("Had exception call callEwalletRollback staff " + newLog.getStaffCode()
                    + " orgRequestId " + newLog.getRequestId());
            logger.error(AppManager.logException(timeSt, ex));
            return "";
        }
    }

    public String checkActiveStatusOnOCS(String msisdn) {
        long timeSt = System.currentTimeMillis();
        String result = "-1";
        ExchMsg response = null;
        try {
            logger.info("start to checkActiveStatusOnOCS for sub " + msisdn);
            ExchMsg request = new ExchMsg();
            request.setCommand("OCSHW_INTEGRATIONENQUIRY");
            if (!msisdn.startsWith("258")) {
                msisdn = "258" + msisdn;
            }
            request.set("MSISDN", msisdn);
//            logger.info("Before send " + request.toString());
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
//            logger.info("After send " + response.toString());
            if (response == null || response.getError() == null) {
                logger.error("ERROR checkActiveStatusOnOCS, response is null, sub " + msisdn);
                return result;
            }
            if (response.getError().equals("0")) {
                result = (String) response.get("LIFE_CYCLE_STATE");
            } else {
                logger.error("ERROR checkActiveStatusOnOCS, sub " + msisdn + " detail response: " + response);
            }
        } catch (Exception ex) {
            StringBuilder br = new StringBuilder();
            br.setLength(0);
            br.append("ERROR checkActiveStatusOnOCS, msisdn ");
            br.append(msisdn);
            if (response != null) {
                br.append(" RESPONSE:\n").append(response);
            } else {
                br.append(" Response is null");
            }
            logger.error(br.toString(), ex);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            logTime("Finish checkActiveStatusOnOCS sub " + msisdn + " result " + result, timeSt);
            return result;
        }
    }

    public String registerMCA(String msisdn, String url) {
        logger.info("Start registerMCA sub " + msisdn);
        PostMethod post = new PostMethod(url);
        String soapResponse = "";
        String result = "";
        long start = System.currentTimeMillis();
        try {
            if (!msisdn.startsWith("258")) {
                msisdn = "258" + msisdn;
            }
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
            org.apache.commons.httpclient.HttpClient httpTransport = new org.apache.commons.httpclient.HttpClient(conMgr);
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

    public String addBalanceWeek(String msisdn, String money, String balanceId, String expDate) {
        long timeSt = System.currentTimeMillis();
        ExchMsg request = new ExchMsg();
        ExchMsg response = null;
        String err = "";
        try {
            logger.info("Start add Balance for sub " + msisdn + " value " + money + " accountid " + balanceId);
            request.setCommand("OCSHW_ADJUST_MONEY_EXP");
            request.set("SERIALNO", "");
            request.set("ISDN", msisdn);
            request.set("ACCOUNT_TYPE", balanceId);
            request.set("AMOUNT", money);
            request.set("VALIDITY_INCREMENT", "0");
            request.set("EXPIRE_DATE", expDate);
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            err = response.getError();
            logger.info("End addBalance isdn " + msisdn + " amount " + money + " balanceid " + balanceId + " result " + err);
            return err;
        } catch (Exception ex) {
            logger.error("Had exception addBalance isdn " + msisdn + " amount " + money);
            logger.error(AppManager.logException(timeSt, ex));
            return err;
        }
    }

    public String getMSCInfor(String msisdn, String staffCode) {
        ExchMsg request = new ExchMsg();
        ExchMsg response = null;
        long start = System.currentTimeMillis();
        try {
            request.setCommand("HLR_HW_QUERY_STATIC_DATA");
            request.set("MSISDN", msisdn);
            request.set("DETAIL", "TRUE");
            request.set("ClientTimeout", "25000");
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            if ("0".equals(response.getError())) {
                String dynamic4gsm = (String) response.get("DYNAMIC_4GSM");
                if (dynamic4gsm != null) {
                    dynamic4gsm = dynamic4gsm.trim().toUpperCase();
                    String[] splits = dynamic4gsm.split("\\n");
                    for (String property : splits) {
                        if ((property != null) && (property.trim().startsWith("MSCNUM"))) {
                            return property.substring(property.indexOf("=") + 1).trim();
                        }
                    }
                }
                return "";
            }
            logTime("Time to getMSCInfor sub " + msisdn + " StaffCode " + staffCode, start);
            return "";
        } catch (Exception e) {
            logTime("Exception to getMSCInfor sub " + msisdn + " staffcode " + staffCode, start);
            logger.error(AppManager.logException(start, e));
            return "";
        }
    }

    public boolean getCellId(String msisdn, String mscId, String staffCode) {
        boolean check = false;
        ExchMsg request = new ExchMsg();
        ExchMsg response = null;
        long start = System.currentTimeMillis();
        try {
            String mscNumber = ResourceBundle.getBundle("configPayBonus").getString(mscId);
            String network3g = ResourceBundle.getBundle("configPayBonus").getString("network3g");
            String networkRadio3g = ResourceBundle.getBundle("configPayBonus").getString("networkRadio3g");
            if ("3".equals(mscNumber)) {
                request.setCommand("MSC_NOKIA_TRACE_CELL");
            } else {
                request.setCommand("MSC_HW_TRACE_CELL");
            }
            request.set("MSISDN", msisdn);
            request.set("MSC_ID", mscNumber);
            request.set("ClientTimeout", "25000");
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            if ("0".equals(response.getError())) {
                String strDes = (response.get("DETAIL") != null) ? (String) response.get("DETAIL") : null;

                if (strDes != null && (strDes.contains(network3g) || strDes.contains(networkRadio3g))) {
                    check = true;
                }

            }
            logTime("Time to getCellId sub " + msisdn + " staffcode " + staffCode, start);
            return check;
        } catch (Exception e) {
            logTime("Exception to getCellId sub " + msisdn + " staffcode " + staffCode, start);
            logger.error(AppManager.logException(start, e));
            return false;
        }
    }

    public String getIMEIByGetCellId(String msisdn, String mscId, String staffCode) {
        ExchMsg request = new ExchMsg();
        ExchMsg response = null;
        long start = System.currentTimeMillis();
        String imei = "";
        try {
            String mscNumber = ResourceBundle.getBundle("configPayBonus").getString(mscId);
            String network3g = ResourceBundle.getBundle("configPayBonus").getString("network3g");
            String networkRadio3g = ResourceBundle.getBundle("configPayBonus").getString("networkRadio3g");
            if ("3".equals(mscNumber)) {
                request.setCommand("MSC_NOKIA_TRACE_CELL");
            } else {
                request.setCommand("MSC_HW_TRACE_CELL");
            }
            request.set("MSISDN", msisdn);
            request.set("MSC_ID", mscNumber);
            request.set("ClientTimeout", "25000");
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            if ("0".equals(response.getError())) {
                String strDes = (response.get("DETAIL") != null) ? (String) response.get("DETAIL") : null;

                Scanner scanner = new Scanner(strDes);
                while (scanner.hasNextLine()) {
                    String line = scanner.nextLine().trim();
                    if (line != null && line.startsWith("IMEISV  =")) {
                        imei = line.substring(line.lastIndexOf('=') + 1).trim();
                        break;
                    }
                }
                scanner.close();

            }
            logTime("Time to getCellId sub " + msisdn + " staffcode " + staffCode, start);
            return imei;
        } catch (Exception e) {
            logTime("Exception to getCellId sub " + msisdn + " staffcode " + staffCode, start);
            logger.error(AppManager.logException(start, e));
            return "";
        }
    }

    //LinhNBV 20181122: add method Deduct money daily
    public ResponseWallet chargeEmola(String requestId, String mobile, String amount, String transId, String userCall, DbEwalletDeductDaily db,
            String billCycleDate) throws Exception {
        long timeSt = System.currentTimeMillis();
        EwalletLog eLog = new EwalletLog();
        SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH.mm.ss");
        Date now = new Date();
        ResponseWallet responseWallet = null;
        String result = "";
        Reader in = null;
        String desc = "";
        try {
            String strDate = sdfDate.format(now).replace("-", "").replace(".", "").replace(" ", "");
            String requestDate = strDate;
            int transactionType = 1;
            String request = "";
            String response = "";
            String partnerCode = ResourceBundle.getBundle("configPayBonus").getString("debitEnte_emola_partnerCode");
            String urlString = ResourceBundle.getBundle("configPayBonus").getString("debitEnte_emola_url");
            String key = ResourceBundle.getBundle("configPayBonus").getString("debitEnte_emola_key");
            URL url = new URL(urlString);
            EmolaBean emola = new EmolaBean();
            emola.setRequestId(requestId);
            emola.setRequestDate(requestDate);
            emola.setPartnerCode(partnerCode);
            emola.setEnterpriseAccount(mobile);
            emola.setAmount(amount);
            emola.setContent("Deduct emola when topup value " + amount + " BillCycleDate " + billCycleDate);
            emola.setTransactionType(transactionType);
            eLog.setIsdn(mobile);
            eLog.setUrl(urlString);
            eLog.setAmountEmola(Double.valueOf(amount));
            eLog.setRequestId(requestId);
            String tempSignature = key + requestDate + amount + mobile + partnerCode + requestId;
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(tempSignature.getBytes());
            byte[] digest = md.digest();
            StringBuilder sb = new StringBuilder();
            for (byte b : digest) {
                sb.append(String.format("%02x", b & 0xff));
            }
            emola.setSignature(sb.toString());
            Gson gson = new Gson();
            request = gson.toJson(emola, EmolaBean.class);
            eLog.setRequest(request);
            logger.info("Request: " + request);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Authorization", "Basic TW92aXRlbDpNb3ZpdGVsMTIzIUAj");
            conn.setRequestProperty("Accept", "application/json");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setConnectTimeout(60000);
            conn.setReadTimeout(60000);
            conn.setDoOutput(true);
            conn.getOutputStream().write(request.getBytes());
            logger.info(mobile + " Response code when open Connection: "
                    + conn.getResponseCode() + "\nResponse message:" + conn.getResponseMessage());
            in = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));
            StringBuilder sbJSON = new StringBuilder();
            for (int c;
                    (c = in.read()) >= 0;) {
                sbJSON.append((char) c);
            }
            response = sbJSON.toString();
            logger.info("Response Wallet: " + response);
            eLog.setRespone(response);
            responseWallet = gson.fromJson(response, ResponseWallet.class);
            if (responseWallet != null && responseWallet.getResponseCode() != null) {
                result = responseWallet.getResponseCode();
                desc = responseWallet.getResponseMessage();
                if ("01".equals(result)) {
                    logger.info("Call eWallet success isdn " + mobile + " amount " + amount + " duration "
                            + (System.currentTimeMillis() - timeSt));
                } else {
                    logger.error("Call eWallet fail isdn " + mobile + " amount " + amount
                            + " duration " + (System.currentTimeMillis() - timeSt)
                            + " errorCode " + result
                            + " description " + desc);
                }
            } else {
                logger.error("Call eWallet error responseWallet is null isdn " + mobile + " amount " + amount + " duration "
                        + (System.currentTimeMillis() - timeSt));
            }
            eLog.setErrorCode(result);
            eLog.setDescription("chargeEmola " + desc);
            eLog.setDuration(System.currentTimeMillis() - timeSt);
            db.insertEwalletLog(eLog);
        } catch (Exception e) {
            logger.error("chargeEmola had exception isdn " + mobile + " amount " + amount + " duration "
                    + (System.currentTimeMillis() - timeSt) + " detail " + e.toString());
            logger.error(AppManager.logException(timeSt, e));
            try {
                eLog.setErrorCode("99");
                eLog.setDescription("chargeEmola had exception " + e.toString());
                eLog.setDuration(System.currentTimeMillis() - timeSt);
                db.insertEwalletLog(eLog);
            } catch (Exception ex) {
                logger.error("Try insert log eWallet_Log had exception isdn " + mobile);
                logger.error(AppManager.logException(timeSt, ex));
            }
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (Exception ex) {
                    logger.error("Failt to close inputstream " + ex.toString());
                }
            }
            return responseWallet;
        }
    }

    public String getCellIdRsString(String msisdn, String mscNum, String staffCode) {
        ExchMsg request = new ExchMsg();
        ExchMsg response = null;
        long start = System.currentTimeMillis();
        if (!msisdn.startsWith("258")) {
            msisdn = "258" + msisdn;
        }
        try {
            String mscId = ResourceBundle.getBundle("configPayBonus").getString(mscNum);
            if ("3".equals(mscId)) {
                request.setCommand("MSC_NOKIA_TRACE_CELL");
            } else {
                request.setCommand("MSC_HW_TRACE_CELL");
            }
            request.set("MSISDN", msisdn);
            request.set("MSC_ID", mscId);
            request.set("ClientTimeout", "25000");
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            if ("0".equals(response.getError())) {
                if ("3".equals(mscId)) {
                    String strCellId = (response.get("CELL_ID") != null) ? (String) response.get("CELL_ID") : null;
                    if (strCellId == null) {
                        return "";
                    }

                    String strLACid = (response.get("LAC") != null) ? (String) response.get("LAC") : null;
                    if (strLACid == null) {
                        return "";
                    }
                    String[] arrCellId = strCellId.split("H/");
                    String[] arrLACid = strLACid.split("H/");
                    return (((arrCellId != null) && (arrCellId.length > 0) && (arrLACid != null) && (arrLACid.length > 0)) ? arrCellId[0] + "|" + arrLACid[0] : "");
                }

                return (((response.get("CELL_ID") != null) && (response.get("LAC") != null)) ? response.get("CELL_ID") + "|" + response.get("LAC") : "");
            }
            logTime("Time to getCellId sub " + msisdn + " staffcode " + staffCode, start);
            return "";
        } catch (Exception e) {
            logTime("Exception to getCellId sub " + msisdn + " staffcode " + staffCode, start);
            logger.error(AppManager.logException(start, e));
            return "";
        }
    }

    public String topupWS(String requestId, String msisdn, String amount) {
        logger.info("Start topupWS for sub " + msisdn + ", requestId: " + requestId + ", amount: " + amount);
        String url = ResourceBundle.getBundle("configPayBonus").getString("urlTopup");
        PostMethod post = new PostMethod(url);
        String soapResponse = "";
        String result = "";
        long start = System.currentTimeMillis();
        try {
            if (!msisdn.startsWith("258")) {
                msisdn = "258" + msisdn;
            }
            String request = "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:ser=\"http://services.wsfw.vas.viettel.com/\">\n"
                    + "   <soapenv:Header>\n"
                    + "      <ser:client>?</ser:client>\n"
                    + "   </soapenv:Header>\n"
                    + "   <soapenv:Body>\n"
                    + "      <ser:topup>\n"
                    + "         <!--Optional:-->\n"
                    + "         <requestId>" + requestId + "</requestId>\n"
                    + "         <!--Optional:-->\n"
                    + "         <msisdn>" + msisdn + "</msisdn>\n"
                    + "         <!--Optional:-->\n"
                    + "         <ammount>" + amount + "</ammount>\n"
                    + "         <!--Optional:-->\n"
                    + "         <userName>emola</userName>\n"
                    + "         <!--Optional:-->\n"
                    + "         <passWord>Emola@20170317</passWord>\n"
                    + "         <!--Optional:-->\n"
                    + "         <branch>1004</branch>\n"
                    + "      </ser:topup>\n"
                    + "   </soapenv:Body>\n"
                    + "</soapenv:Envelope>";
            RequestEntity entity = new StringRequestEntity(request, "text/xml", "UTF-8");
            post.setRequestEntity(entity);
            MultiThreadedHttpConnectionManager conMgr = new MultiThreadedHttpConnectionManager();
            conMgr.setMaxConnectionsPerHost(20000);
            conMgr.setMaxTotalConnections(20000);
            org.apache.commons.httpclient.HttpClient httpTransport = new org.apache.commons.httpclient.HttpClient(conMgr);
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
            logTime("Time to topupWS msisdn: " + msisdn + " result: " + result, start);
            return result;
        } catch (Exception ex) {
            logTime("Exception to topupWS msisdn:  " + msisdn, start);
            logger.error(AppManager.logException(start, ex));
            return "ERR";
        } catch (Throwable e) {
            logTime("Exception to topupWS msisdn:  " + msisdn, start);
            logger.error(AppManager.logException(start, e));
            return "ERR";
        } finally {
            post.releaseConnection();
        }
    }
//  LinhNBV 20181203: For sub cabo, no need to receive 6 MT.

    public String callOcsChangeCusInfoCommand(String msisdn, String grade) {
        ExchMsg request = new ExchMsg();
        ExchMsg response = null;
        long start = System.currentTimeMillis();
        try {
            if (!msisdn.startsWith("258")) {
                msisdn = "258" + msisdn;
            }
            request.setCommand("OCSHW_CHANGE_CUS_INFO");
            request.set("MSISDN", msisdn);
            request.set("GRADE", grade);
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            logTime("Time to callOcsChangeCusInfoCommand sub " + msisdn + " result " + response.getError(), start);
            return response.getError();
        } catch (Exception e) {
            logTime("Exception to callOcsChangeCusInfoCommand sub " + msisdn, start);
            AppManager.logException(start, e);
            return "";
        }
    }

    public ResponseWallet chargeEmolaEpay(String requestId, String mobile, String amount, String transId, String userCall, DbEwalletDeductDaily db,
            String billCycleDate) throws Exception {
        long timeSt = System.currentTimeMillis();
        EwalletLog eLog = new EwalletLog();
        SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH.mm.ss");
        Date now = new Date();
        ResponseWallet responseWallet = null;
        String result = "";
        Reader in = null;
        String desc = "";
        try {
            String strDate = sdfDate.format(now).replace("-", "").replace(".", "").replace(" ", "");
            String requestDate = strDate;
            String request = "";
            String response = "";
            String partnerCode = ResourceBundle.getBundle("configPayBonus").getString("debitEnte_emola_partnerCode");
            String urlString = ResourceBundle.getBundle("configPayBonus").getString("epay_emola_url");
            String key = ResourceBundle.getBundle("configPayBonus").getString("debitEnte_emola_key");
            URL url = new URL(urlString);
            EpayEmolaBean emola = new EpayEmolaBean();
            emola.setRequestId(requestId);
            emola.setTransactionDate(requestDate);
            emola.setAmount(amount);
            emola.setDescription("Deduct emola when topup value " + amount + " BillCycleDate " + billCycleDate);
            emola.setSrcStoreCode(partnerCode);
            eLog.setIsdn(mobile);
            eLog.setUrl(urlString);
            eLog.setAmountEmola(Double.valueOf(amount));
            eLog.setRequestId(requestId);
            String tempSignature = key + requestId + requestDate + amount + partnerCode;
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(tempSignature.getBytes());
            byte[] digest = md.digest();
            StringBuilder sb = new StringBuilder();
            for (byte b : digest) {
                sb.append(String.format("%02x", b & 0xff));
            }
            emola.setSignature(sb.toString());
            Gson gson = new Gson();
            request = gson.toJson(emola, EpayEmolaBean.class);
            eLog.setRequest(request);
            logger.info("Request: " + request);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Authorization", "Basic TW92aXRlbDp6b3Awc3QhQCM=");
            conn.setRequestProperty("Accept", "application/json");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setConnectTimeout(60000);
            conn.setReadTimeout(60000);
            conn.setDoOutput(true);
            conn.getOutputStream().write(request.getBytes());
            logger.info(mobile + " Response code when open Connection: "
                    + conn.getResponseCode() + "\nResponse message:" + conn.getResponseMessage());
            in = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));
            StringBuilder sbJSON = new StringBuilder();
            for (int c;
                    (c = in.read()) >= 0;) {
                sbJSON.append((char) c);
            }
            response = sbJSON.toString();
            logger.info("Response Wallet: " + response);
            eLog.setRespone(response);
            responseWallet = gson.fromJson(response, ResponseWallet.class);
            if (responseWallet != null && responseWallet.getResponseCode() != null) {
                result = responseWallet.getResponseCode();
                desc = responseWallet.getResponseMessage();
                if ("01".equals(result)) {
                    logger.info("Call chargeEmolaEpay success isdn " + mobile + " amount " + amount + " duration "
                            + (System.currentTimeMillis() - timeSt));
                } else {
                    logger.error("Call chargeEmolaEpay fail isdn " + mobile + " amount " + amount
                            + " duration " + (System.currentTimeMillis() - timeSt)
                            + " errorCode " + result
                            + " description " + desc);
                }
            } else {
                logger.error("Call chargeEmolaEpay error responseWallet is null isdn " + mobile + " amount " + amount + " duration "
                        + (System.currentTimeMillis() - timeSt));
            }
            eLog.setErrorCode(result);
            eLog.setDescription("chargeEmolaEpay " + desc);
            eLog.setDuration(System.currentTimeMillis() - timeSt);
            db.insertEwalletLog(eLog);
        } catch (Exception e) {
            logger.error("chargeEmolaEpay had exception isdn " + mobile + " amount " + amount + " duration "
                    + (System.currentTimeMillis() - timeSt) + " detail " + e.toString());
            logger.error(AppManager.logException(timeSt, e));
            try {
                eLog.setErrorCode("99");
                eLog.setDescription("chargeEmolaEpay had exception " + e.toString());
                eLog.setDuration(System.currentTimeMillis() - timeSt);
                db.insertEwalletLog(eLog);
            } catch (Exception ex) {
                logger.error("Try insert log eWallet_Log had exception isdn " + mobile);
                logger.error(AppManager.logException(timeSt, ex));
            }
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (Exception ex) {
                    logger.error("Failt to close inputstream " + ex.toString());
                }
            }
            return responseWallet;
        }
    }
    //LinhNBV 20190117: Add Command for 4G

    public String modRoamingFlag(String msisdn, String flagValue) {
        ExchMsg request = new ExchMsg();
        ExchMsg response = null;
        long start = System.currentTimeMillis();
        try {
            if (!msisdn.startsWith("258")) {
                msisdn = "258" + msisdn;
            }
            request.setCommand("HLR_HW_MOD_ODBROAM");
            request.set("MSISDN", msisdn);
            request.set("ODBROAM", flagValue);
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            logTime("Time to modRoamingFlag sub " + msisdn + " result " + response.getError() + " flagValue: " + flagValue, start);
            return response.getError();
        } catch (Exception e) {
            logTime("Exception to modRoamingFlag sub " + msisdn + " flagValue: " + flagValue, start);
            AppManager.logException(start, e);
            return "";
        }
    }
//    LinhNBV 20190117: Mo/Chan Data 3G/4G: Chan: GPRSLOCK=TRUE, EPSLOCK=TRUE; --- Mo: GPRSLOCK=FALSE, EPSLOCK=FALSE;

    public String modDataFlag(String msisdn, String gprsLockValue, String epsLockValue) {
        ExchMsg request = new ExchMsg();
        ExchMsg response = null;
        long start = System.currentTimeMillis();
        try {
            if (!msisdn.startsWith("258")) {
                msisdn = "258" + msisdn;
            }
            request.setCommand("HLR_HW_MODI_SUB_LOCK_4G");
            request.set("MSISDN", msisdn);
            request.set("GPRSLOCK", gprsLockValue);
            request.set("EPSLOCK", epsLockValue);
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            logTime("Time to modDataFlag sub " + msisdn + " result " + response.getError(), start);
            return response.getError();
        } catch (Exception e) {
            logTime("Exception to modDataFlag sub " + msisdn, start);
            AppManager.logException(start, e);
            return "";
        }
    }

    public String commitFlagBAIC(String msisdn) {
        ExchMsg request = new ExchMsg();
        ExchMsg response = null;
        long start = System.currentTimeMillis();
        try {
            request.setCommand("HLR_HW_ACT_BAIC");
            request.set("MSISDN", msisdn);
            request.set("BSG", "ALL");
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            logTime("Time to commitFlagBAIC sub " + msisdn + " result " + response.getError(), start);
            return response.getError();
        } catch (Exception e) {
            logTime("Exception to commitFlagBAIC sub " + msisdn, start);
            AppManager.logException(start, e);
            return "";
        }
    }

    public String modFlagBAIC(String msisdn) {
        ExchMsg request = new ExchMsg();
        ExchMsg response = null;
        long start = System.currentTimeMillis();
        try {
            request.setCommand("HLR_HW_MOD_BAIC");
            request.set("MSISDN", msisdn);
            request.set("PROVISION", "TRUE");
            request.set("ClientTimeout", "25000");
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            logTime("Time to modFlagBAIC sub " + msisdn + " result " + response.getError(), start);
            return response.getError();
        } catch (Exception e) {
            logTime("Exception to modFlagBAIC sub " + msisdn, start);
            AppManager.logException(start, e);
            return "";
        }
    }
//    LinhNBV 20190120: For changeSim 4G

    public String modImsi(String msisdn, String newImsi) {
        ExchMsg request = new ExchMsg();
        ExchMsg response = null;
        long start = System.currentTimeMillis();
        try {
            request.setCommand("HLR_HW_MODI_IMSI");
            request.set("MSISDN", msisdn);
            request.set("NEWIMSI", newImsi);
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            logTime("Time to modImsi sub " + msisdn + " result " + response.getError(), start);
            return response.getError();
        } catch (Exception e) {
            logTime("Exception to modImsi sub " + msisdn, start);
            AppManager.logException(start, e);
            return "";
        }
    }

    public String modEPS(String msisdn, String provision, long constantAMBRMAXUL, long constantAMBRMAXDL) {
        ExchMsg request = new ExchMsg();
        ExchMsg response = null;
        long start = System.currentTimeMillis();
        try {
            request.setCommand("HLR_HW_MOD_EPS");
            request.set("MSISDN", msisdn);
            request.set("PROVISION", provision);
            request.set("AMBRMAXUL", constantAMBRMAXUL);
            request.set("AMBRMAXDL", constantAMBRMAXDL);
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            logTime("Time to modEPS sub " + msisdn + " result " + response.getError(), start);
            return response.getError();
        } catch (Exception e) {
            logTime("Exception to modEPS sub " + msisdn, start);
            AppManager.logException(start, e);
            return "";
        }
    }

    public String modTPLOPTGPRS(String msisdn, String provision, String tplId, String delOld) {
        ExchMsg request = new ExchMsg();
        ExchMsg response = null;
        long start = System.currentTimeMillis();
        try {
            request.setCommand("HLR_HW_MOD_TPLOPTGPRS");
            request.set("MSISDN", msisdn);
            request.set("PROVISION", provision);
            request.set("TPLID", tplId);
            request.set("DELOLD", delOld);
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            logTime("Time to modTPLOPTGPRS sub " + msisdn + " result " + response.getError(), start);
            return response.getError();
        } catch (Exception e) {
            logTime("Exception to modTPLOPTGPRS sub " + msisdn, start);
            AppManager.logException(start, e);
            return "";
        }
    }

    public String removeKI(String oldImsi) {
        ExchMsg request = new ExchMsg();
        ExchMsg response = null;
        long start = System.currentTimeMillis();
        try {
            request.setCommand("HLR_HW_REMOVE_KI");
            request.set("IMSI", oldImsi);
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            logTime("Time to removeKI oldImsi " + oldImsi + " result " + response.getError(), start);
            return response.getError();
        } catch (Exception e) {
            logTime("Exception to removeKI oldImsi " + oldImsi, start);
            AppManager.logException(start, e);
            return "";
        }
    }

    public String changeSim(String msisdn, String newImsi) {
        ExchMsg request = new ExchMsg();
        ExchMsg response = null;
        long start = System.currentTimeMillis();
        try {
            request.setCommand("OCSHW_CHANGESIM");
            request.set("MSISDN", msisdn);
            request.set("NEWIMSI", newImsi);
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            logTime("Time to changeSim sub " + msisdn + " result " + response.getError(), start);
            return response.getError();
        } catch (Exception e) {
            logTime("Exception to changeSim sub " + msisdn, start);
            AppManager.logException(start, e);
            return "";
        }
    }

    public String querySubPCRF(String msisdn) {
        if (!msisdn.startsWith("258")) {
            msisdn = "258" + msisdn;
        }
        ExchMsg request = new ExchMsg();
        long timeSt = System.currentTimeMillis();
        ExchMsg response = null;
        String err = "";
        try {
            request.setCommand("PCRF_QUERYINFO");
            request.set("MSISDN", msisdn);
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            err = response.getError();
            logger.info("End querySubPCRF isdn " + msisdn + " result " + err);
            return err;
        } catch (Exception ex) {
            logger.error("Had exception querySubPCRF isdn " + msisdn);
            logger.error(AppManager.logException(timeSt, ex));
            return err;
        }
    }

    public String addSubViaPCRF(String msisdn) {
        if (!msisdn.startsWith("258")) {
            msisdn = "258" + msisdn;
        }
        ExchMsg request = new ExchMsg();
        long timeSt = System.currentTimeMillis();
        ExchMsg response = null;
        String err = "";
        try {
            request.setCommand("PCRF_ADDSUB_NO_SERVICE");
            request.set("MSISDN", msisdn);
            request.set("USR_MSISDN", msisdn);
            request.set("PAID_TYPE", ""); //20180727 Huynq change value from 0 to 2147483646 follow ThangPN3 "BÃ¡o cÃ¡o káº¿t quáº£ thá»±c hiá»‡n chá»‰ thá»‹ diá»…n táº­p T6 " cá»§a P.TGÄ� TÃ o Tháº¯ng

            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);

            err = response.getError();
            logger.info("End addSubViaPCRF isdn " + msisdn + " result " + err);
            return err;
        } catch (Exception ex) {
            logger.error("Had exception addSubViaPCRF isdn " + msisdn);
            logger.error(AppManager.logException(timeSt, ex));
            return err;
        }
    }

    public String removeSubServicePCRF(String msisdn, String srvName) {
        if (!msisdn.startsWith("258")) {
            msisdn = "258" + msisdn;
        }
        ExchMsg request = new ExchMsg();
        long timeSt = System.currentTimeMillis();
        ExchMsg response = null;
        String err = "";
        try {
            request.setCommand("PCRF_UNSUBSCRIBESERVICE");
            request.set("MSISDN", msisdn);
            request.set("ADD_SRVNAME", srvName);

            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);

            err = response.getError();
            logger.info("End removeSubServicePCRF isdn " + msisdn + " result " + err);
            return err;
        } catch (Exception ex) {
            logger.error("Had exception removeSubServicePCRF isdn " + msisdn);
            logger.error(AppManager.logException(timeSt, ex));
            return err;
        }
    }

    public String addSubServiceViaPCRF(String msisdn, String srvName, String expireTime) {
        if (!msisdn.startsWith("258")) {
            msisdn = "258" + msisdn;
        }
        ExchMsg request = new ExchMsg();
        long timeSt = System.currentTimeMillis();
        ExchMsg response = null;
        String err = "";
        try {
            request.setCommand("PCRF_SUBSCRIBESERVICE");
            request.set("MSISDN", msisdn);
            request.set("ADD_SRVNAME", srvName);
            request.set("EXPIRE_TIME", expireTime);
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            err = response.getError();
            logger.info("End addSubServiceViaPCRF isdn " + msisdn + " result " + err);
            return err;
        } catch (Exception ex) {
            logger.error("Had exception addSubServiceViaPCRF isdn " + msisdn);
            logger.error(AppManager.logException(timeSt, ex));
            return err;
        }
    }

    public String updateQuotaPCRF(String msisdn, String quotaName, String quotaBalance) {
        if (!msisdn.startsWith("258")) {
            msisdn = "258" + msisdn;
        }
        ExchMsg request = new ExchMsg();
        long timeSt = System.currentTimeMillis();
        ExchMsg response = null;
        String err = "";
        try {
            request.setCommand("PCRF_UPDATE_SUB_QUOTA");
            request.set("MSISDN", msisdn);
            request.set("QTNAME", quotaName);
            request.set("QTABALANCE", quotaBalance);
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            err = response.getError();
            logger.info("End updateQuotaPCRF isdn " + msisdn + " result " + err);
            return err;
        } catch (Exception ex) {
            logger.error("Had exception updateQuotaPCRF isdn " + msisdn);
            logger.error(AppManager.logException(timeSt, ex));
            return err;
        }
    }

    public String getServiceOfSubPCRFV2(String msisdn, String serviceName) {
        if (!msisdn.startsWith("258")) {
            msisdn = "258" + msisdn;
        }
        ExchMsg request = new ExchMsg();
        long timeSt = System.currentTimeMillis();
        ExchMsg response = null;
        String err = "";
        try {
            request.setCommand("PCRF_GET_SPECSERVICE");
            request.set("MSISDN", msisdn);
            request.set("ADD_SRVNAME", serviceName);
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            err = response.getError();
            logger.info("End getServiceOfSubPCRFV2 isdn " + msisdn + " result " + err);
            return err;
        } catch (Exception ex) {
            logger.error("Had exception getServiceOfSubPCRFV2 isdn " + msisdn);
            logger.error(AppManager.logException(timeSt, ex));
            return err;
        }
    }

    public String getServiceOfSubPCRFV3(String msisdn, String serviceName) {
        ExchMsg request = new ExchMsg();
        long timeSt = System.currentTimeMillis();
        ExchMsg response = null;
        String err = "";
        try {
            request.setCommand("PCRF_GET_SPECSERVICE");
            request.set("MSISDN", msisdn);
            request.set("ADD_SRVNAME", serviceName);
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            err = response.getOriginal();
            logger.info("End getServiceOfSubPCRFV2 isdn " + msisdn + " result " + err);
            return err;
        } catch (Exception ex) {
            logger.error("Had exception getServiceOfSubPCRFV2 isdn " + msisdn);
            logger.error(AppManager.logException(timeSt, ex));
            return err;
        }
    }

    public String updateServicesPCRF(String msisdn, String servicesName, String expireTime, String startDate) {
        if (!msisdn.startsWith("258")) {
            msisdn = "258" + msisdn;
        }
        ExchMsg request = new ExchMsg();
        long timeSt = System.currentTimeMillis();
        ExchMsg response = null;
        String err = "";
        try {
            request.setCommand("PCRF_UPDATE_SUBS_SERVICE");
            request.set("MSISDN", msisdn);
            request.set("ADD_SRVNAME", servicesName);
            request.set("EXPIRE_TIME", expireTime);
            request.set("START_DATE", startDate);
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            err = response.getError();
            logger.info("End updateServicesPCRF isdn " + msisdn + " result " + err);
            return err;
        } catch (Exception ex) {
            logger.error("Had exception updateServicesPCRF isdn " + msisdn);
            logger.error(AppManager.logException(timeSt, ex));
            return err;
        }
    }

    public String rechargeSubQuotaPCRF(String msisdn, String quotaValue, String quotaName) {
        if (!msisdn.startsWith("258")) {
            msisdn = "258" + msisdn;
        }
        ExchMsg request = new ExchMsg();
        long timeSt = System.currentTimeMillis();
        ExchMsg response = null;
        String err = "";
        try {
            request.setCommand("PCRF_RECHARGE_SUB_QUOTA");
            request.set("MSISDN", msisdn);
            request.set("QTVALUE", quotaValue);
            request.set("QTNAME", quotaName);
            request.set("QTACLASS", "0");
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            err = response.getError();
            logger.info("End updateServicesPCRF isdn " + msisdn + " result " + err);
            return err;
        } catch (Exception ex) {
            logger.error("Had exception updateServicesPCRF isdn " + msisdn);
            logger.error(AppManager.logException(timeSt, ex));
            return err;
        }
    }

    public String getImsiOnOCSByIsdn(String msisdn) {
        if (!msisdn.startsWith("258")) {
            msisdn = "258" + msisdn;
        }
        logger.info("Start getImsiOnOCS isdn " + msisdn);
        ExchMsg request = new ExchMsg();
        long timeSt = System.currentTimeMillis();
        ExchMsg response = null;
        String imsi = "";
        try {
            request.setCommand("OCSHW_INTEGRATIONENQUIRY");
            request.set("MSISDN", msisdn);
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            imsi = (String) response.get("IMSI");
            logger.info("End getImsiOnOCS isdn " + msisdn + " result " + imsi);
            return imsi;
        } catch (Exception ex) {
            logger.error("Had exception getImsiOnOCS isdn " + msisdn);
            logger.error(AppManager.logException(timeSt, ex));
            return imsi;
        }
    }

    public String getImsiOnHLRByIsdn(String msisdn) {
        if (!msisdn.startsWith("258")) {
            msisdn = "258" + msisdn;
        }
        logger.info("Start getImsiOnHLRByIsdn isdn " + msisdn);
        ExchMsg request = new ExchMsg();
        long timeSt = System.currentTimeMillis();
        ExchMsg response = null;
        String imsi = "";
        try {
            request.setCommand("HLR_HW_QUERY_STATIC_DATA");
            request.set("MSISDN", msisdn);
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            imsi = (String) response.get("IMSI");
            logger.info("End getImsiOnHLRByIsdn isdn " + msisdn + " result " + imsi);
            return imsi;
        } catch (Exception ex) {
            logger.error("Had exception getImsiOnHLRByIsdn isdn " + msisdn);
            logger.error(AppManager.logException(timeSt, ex));
            return imsi;
        }
    }

    public String checkIsdnExistOnOCS(String msisdn) {
        if (!msisdn.startsWith("258")) {
            msisdn = "258" + msisdn;
        }
        logger.info("Start checkIsdnExistOnOCS isdn " + msisdn);
        ExchMsg request = new ExchMsg();
        long timeSt = System.currentTimeMillis();
        ExchMsg response = null;
        String result = "";
        try {
            request.setCommand("OCSHW_INTEGRATIONENQUIRY");
            request.set("MSISDN", msisdn);
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            result = (String) response.getError();
            logger.info("End checkIsdnExistOnOCS isdn " + msisdn + " result " + result);
            return result;
        } catch (Exception ex) {
            logger.error("Had exception checkIsdnExistOnOCS isdn " + msisdn);
            logger.error(AppManager.logException(timeSt, ex));
            return result;
        }
    }

    public String getIsdnOnHLRByImsi(String imsi) {
        logger.info("Start getIsdnOnHLRByImsi imsi " + imsi);
        ExchMsg request = new ExchMsg();
        long timeSt = System.currentTimeMillis();
        ExchMsg response = null;
        String isdn = "";
        try {
            request.setCommand("HLR_HW_QUERY_STATIC_DATA");
            request.set("IMSI", imsi);
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            isdn = (String) response.get("MSISDN");
            logger.info("End getIsdnOnHLRByImsi isdn " + imsi + " result " + isdn);
            return isdn;
        } catch (Exception ex) {
            logger.error("Had exception getIsdnOnHLRByImsi isdn " + imsi);
            logger.error(AppManager.logException(timeSt, ex));
            return isdn;
        }
    }

    public String removeSubOnHLR(String msisdn) {
        if (!msisdn.startsWith("258")) {
            msisdn = "258" + msisdn;
        }
        logger.info("Start removeSubOnHLR isdn " + msisdn);
        ExchMsg request = new ExchMsg();
        long timeSt = System.currentTimeMillis();
        ExchMsg response = null;
        String result = "";
        try {
            request.setCommand("HLR_HW_REMOVE_SUB");
            request.set("MSISDN", msisdn);
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            result = (String) response.getError();
            logger.info("End removeSubOnHLR isdn " + msisdn + " result " + result);
            return result;
        } catch (Exception ex) {
            logger.error("Had exception removeSubOnHLR isdn " + msisdn);
            logger.error(AppManager.logException(timeSt, ex));
            return result;
        }
    }

    public String registerSubOnHLR(String msisdn, String imsi, String TPLID) {
        if (!msisdn.startsWith("258")) {
            msisdn = "258" + msisdn;
        }
        logger.info("Start registerSubOnHLR isdn " + msisdn);
        ExchMsg request = new ExchMsg();
        long timeSt = System.currentTimeMillis();
        ExchMsg response = null;
        String result = "";
        try {
            request.setCommand("HLR_HW_REGIST_SUB");
            request.set("MSISDN", msisdn);
            request.set("HLRSN", 1);
            request.set("IMSI", imsi);
            request.set("TPLID", TPLID);

            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            result = (String) response.getError();
            logger.info("End registerSubOnHLR isdn " + msisdn + " result " + result);
            return result;
        } catch (Exception ex) {
            logger.error("Had exception registerSubOnHLR isdn " + msisdn);
            logger.error(AppManager.logException(timeSt, ex));
            return result;
        }
    }

    public String queryIntegration(String msisdn) {
        String isdn = "258";
        ExchMsg request = new ExchMsg();
        long timeSt = System.currentTimeMillis();
        if (msisdn != null) {
            isdn = msisdn.startsWith("258") ? msisdn : "258" + msisdn;
        } else {
            isdn = "";
        }
        ExchMsg response = null;
        String err = "";
        try {
            request.setCommand("OCSHW_INTEGRATIONENQUIRY");
            request.set("MSISDN", isdn);

            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            if ("0".equals(response.getError())) {
                return response.getOriginal();
            }
            err = "ERROR";
            logger.info("End queryBalance isdn " + isdn + " result " + err);
            return err;
        } catch (Exception ex) {
            logger.error("Had exception queryBalance isdn " + isdn);
            logger.error(AppManager.logException(timeSt, ex));
            return err;
        }
    }

    /**
     * Get Product of Subscriber by product id on OCS
     *
     * @param strSoapBody
     * @param productID
     * @return
     */
    public List<IntegrationProduct> getIntegrationProduct(String strSoapBody, String productB, String productB2) {
        long start = System.currentTimeMillis();
        logger.info("IntegrationProduct productID starting...productB:" + productB + ",productB2:" + productB2 + " time " + start);
        IntegrationProduct p = null;
        List<IntegrationProduct> lstIntegrationProduct = null;
        try {
            InputStream is = new ByteArrayInputStream(strSoapBody.getBytes());
            SOAPMessage soapMessage = MessageFactory.newInstance(SOAPConstants.SOAP_1_2_PROTOCOL).createMessage(null, is);
            //SOAPMessage soapMessage = MessageFactory.newInstance().createMessage(null, is);
            SOAPEnvelope soapEnvelope = soapMessage.getSOAPPart().getEnvelope();
            SOAPBody soapBody = soapEnvelope.getBody();
            @SuppressWarnings("unchecked")
            Iterator<SOAPBodyElement> elements = soapBody.getChildElements();
            while (elements.hasNext()) {
                logger.info(">>>>>>>>>>>>>1");
                SOAPBodyElement element = elements.next();
                @SuppressWarnings("unchecked")
                Iterator<SOAPBodyElement> chlElement = element.getChildElements();
                while (chlElement.hasNext()) {
                    logger.info(">>>>>>>>>>>>>2");
                    SOAPBodyElement param = chlElement.next();
                    if ("IntegrationEnquiryResult".equals(param.getNodeName())) {
                        Iterator<SOAPBodyElement> BalanceRecord = param.getChildElements();
                        while (BalanceRecord.hasNext()) {
                            logger.info(">>>>>>>>>>>>>3");
                            SOAPBodyElement param1 = BalanceRecord.next();
                            if ("SubscriberInfo".equals(param1.getNodeName())) {
                                Iterator<SOAPBodyElement> chlElement1 = param1.getChildElements();
                                lstIntegrationProduct = new ArrayList<IntegrationProduct>();
                                while (chlElement1.hasNext()) {
                                    logger.info(">>>>>>>>>>>>>4");
                                    SOAPBodyElement chlElement2 = chlElement1.next();
                                    if ("Product".equals(chlElement2.getNodeName())) {
                                        Iterator<SOAPBodyElement> param2 = chlElement2.getChildElements();
                                        p = new IntegrationProduct();
                                        while (param2.hasNext()) {
                                            logger.info(">>>>>>>>>>>>>5");
                                            SOAPBodyElement chlElement3 = param2.next();
                                            if (chlElement3.getNodeName().equalsIgnoreCase(p.ID)) {
                                                p.setId(chlElement3.getValue());
                                            } else if (chlElement3.getNodeName().equalsIgnoreCase(p.PRODUCTORDERKEY)) {
                                                p.setProductOrderKey(chlElement3.getValue());
                                            } else if (chlElement3.getNodeName().equalsIgnoreCase(p.EFFECTIVEDATE)) {
                                                p.setEffectiveDate(chlElement3.getValue());
                                            } else if (chlElement3.getNodeName().equalsIgnoreCase(p.EXPIREDDATE)) {
                                                p.setExpiredDate(chlElement3.getValue());
                                            } else if (chlElement3.getNodeName().equalsIgnoreCase(p.STATUS)) {
                                                p.setStatus(chlElement3.getValue());
                                            } else if (chlElement3.getNodeName().equalsIgnoreCase(p.CURCYCLESTARTTIME)) {
                                                p.setCurCycleStartTime(chlElement3.getValue());
                                            } else if (chlElement3.getNodeName().equalsIgnoreCase(p.CURCYCLEENDTIME)) {
                                                p.setCurCycleEndTime(chlElement3.getValue());
                                            } else if (chlElement3.getNodeName().equalsIgnoreCase(p.BILLSTATUS)) {
                                                p.setBillStatus(chlElement3.getValue());
                                            }
                                            logger.info(">>>>>>>>>>>>>5");
                                        }
                                        if (p.getId().equalsIgnoreCase(productB) || p.getId().equalsIgnoreCase(productB2)) {
                                            logger.info("IntegrationProduct productID:" + p.getId() + " -->IntegrationProduct: " + p.toString() + ", time " + start);
                                            lstIntegrationProduct.add(p);
                                        }
                                    }
                                }

                            }
                        }
                    }
                }
            }
            logger.info("IntegrationProduct productID end." + lstIntegrationProduct != null ? lstIntegrationProduct.size() : 0);
            return lstIntegrationProduct;
        } catch (Exception e) {
            logger.error(AppManager.logException(start, e));
            return null;
        }

    }

    /**
     *
     * Add price code in the future
     */
    public String addPriceV3(String msisdn, String planId, String effectDate, String expireDate) {
        ExchMsg request = new ExchMsg();
        long timeSt = System.currentTimeMillis();
        ExchMsg response = null;
        String err = "";
        try {
            request.setCommand("OCSHW_SUBSCRIBEPRODUCT3");
            request.set("MSISDN", msisdn);
            request.set("PRODUCTID", planId);
            if (!"".equals(effectDate)) {
                request.set("EFFECTIVE_DATE", effectDate);
            }
            if (!"".equals(expireDate)) {
                request.set("EXPIRE_DATE", expireDate);
            }
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            err = response.getError();
            logger.info("End addPrice isdn " + msisdn + " planId " + planId + " result " + err);
            return err;
        } catch (Exception ex) {
            logger.error("Had exception addPrice isdn " + msisdn + " planId " + planId);
            logger.error(AppManager.logException(timeSt, ex));
            return err;
        }
    }

    public boolean checkIsdnOnlineOnHRL(String msisdn, String mscNumber) {
        ExchMsg request = new ExchMsg();
        ExchMsg response = null;
        long start = System.currentTimeMillis();
        String statusIsdn = "";
        try {
            if ("3".equals(mscNumber)) {
                request.setCommand("MSC_NOKIA_TRACE_CELL");
            } else {
                request.setCommand("MSC_HW_TRACE_CELL");
            }
            request.set("MSISDN", msisdn);
            request.set("MSC_ID", mscNumber);
            request.set("ClientTimeout", "25000");
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            if ("0".equals(response.getError())) {
                String strDes = (response.get("DETAIL") != null) ? (String) response.get("DETAIL") : null;

                Scanner scanner = new Scanner(strDes);
                while (scanner.hasNextLine()) {
                    String line = scanner.nextLine().trim();
                    if (line != null && line.startsWith("IMSI Attach Flag  =")) {
                        statusIsdn = line.substring(line.lastIndexOf('=') + 1).trim();
                        if ("Attached".equals(statusIsdn)) {
                            return Boolean.TRUE;
                        }
                    }
                }
                scanner.close();
            }
            logTime("Time to checkIsdnOnlineOnHRL sub " + msisdn, start);
            return Boolean.TRUE;
        } catch (Exception e) {
            logTime("Exception to checkIsdnOnlineOnHRL sub " + msisdn, start);
            logger.error(AppManager.logException(start, e));
            return Boolean.TRUE;
        }
    }

    public boolean checkIsdnExistOnOCS(String msisdn, String imsi) {
        if (!msisdn.startsWith("258")) {
            msisdn = "258" + msisdn;
        }
        logger.info("Start checkIsdnExistOnOCS isdn " + msisdn);
        ExchMsg request = new ExchMsg();
        long timeSt = System.currentTimeMillis();
        ExchMsg response = null;
        String result = "";
        try {
            request.setCommand("OCSHW_INTEGRATIONENQUIRY");
            request.set("MSISDN", msisdn);
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            result = (String) response.getError();
            logger.info("End checkIsdnExistOnOCS isdn " + msisdn + " result " + result);
            if ("0".equals(result)) {
                String imsiIsdn = (String) response.get("IMSI");
                logger.info(" checkIsdnExistOnOCSisdn " + msisdn + " IMSI on OCS " + imsiIsdn);
                if (imsiIsdn != null && imsiIsdn.equals(imsi)) {
                    return true;
                }
            }
            return false;
        } catch (Exception ex) {
            logger.error("Had exception checkIsdnExistOnOCS isdn " + msisdn);
            logger.error(AppManager.logException(timeSt, ex));
            return false;
        }
    }

    public String queryScratchCard(String serial) {
        ExchMsg request = new ExchMsg();
        long timeSt = System.currentTimeMillis();
        ExchMsg response = null;
        String err = "";
        try {
            request.setCommand("VCHW_QUERYCARD");
            request.set("SEQUENCE", serial);
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            err = response.getError();
            if ("0".equals(err)) {
                String hotCardFlag = (String) response.get("HOTCARDFLAG");
                if ("0".equals(hotCardFlag) || "1".equals(hotCardFlag)) {
                    err = "0";
                } else {
                    err = hotCardFlag;
                }
            }
            logger.info("End queryScratchCard serial " + serial + " errCode: " + err);
            return err;
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error("Have queryScratchCard serial " + serial);
            logger.error(AppManager.logException(timeSt, ex));
            return err;
        }
    }

    public String callEwalletV3(long aciontAuditId, int channelTypeId, String mobile, long amount,
            String actionCode, String staffCode, String transId, DbProcessorAbstract db) throws Exception {

        if (amount <= 0) {
            logger.info("The value is not greater than 0 so default return success isdn " + mobile + " amount " + amount);
            return "01";
        }
        String request = "";
        String response = "";
        String errorCode = "";
        String description = "";
        long timeSt = System.currentTimeMillis();
        EwalletLog eLog = new EwalletLog();
        DbSubProfileProcessor dbBonusFirst = null;
        DbPayBonusSecond dbBonusSecond = null;
        DbMakeSaleTranCug dbCug = null;
        DbConnectKitProcessor dbKit = null;
        DbBonusConnectKit dbBonusKit = null;
        //        LinhNBV 20181015: Add DbPayBonusAgentVip
        DbPayBonusAgentVipProcessor dbBonusAgentVip = null;
        DbMobileShopChannel dbMobileShop = null;
        DbPromotionScaner dbBranchScaner = null;
        //Bacnx 20190122: Add bonus for channel when change sim 4G
        DbBonus4GChangeSim dbBonusChannelSim4G = null;
        //Bacnx 20190320: Add bonus for update profile
        DbSubProfileUpdateProcessor dbSubProfileUpdateProcessor = null;
        DbKitBatchConnectProcessor dbKitBatchConnectProcessor = null;
        DbKitBatchSubProcessor dbKitBatchProccesser = null;
        DbKitBatchRetryPayComission dbKitBatchRetryPayComission = null;
        try {
            logger.info("Start call Ewallet isdn " + mobile + " amount " + amount);
            if (db.getClass() == DbSubProfileProcessor.class) {
                dbBonusFirst = (DbSubProfileProcessor) db;
            } else if (db.getClass() == DbPayBonusSecond.class) {
                dbBonusSecond = (DbPayBonusSecond) db;
            } else if (db.getClass() == DbMakeSaleTranCug.class) {
                dbCug = (DbMakeSaleTranCug) db;
            } else if (db.getClass() == DbConnectKitProcessor.class) {
                dbKit = (DbConnectKitProcessor) db;
            } else if (db.getClass() == DbPayBonusAgentVipProcessor.class) {//LinhNBV 20181015: Init dbBonusAgentVip to save log ewallet
                dbBonusAgentVip = (DbPayBonusAgentVipProcessor) db;
            } else if (db.getClass() == DbMobileShopChannel.class) {
                dbMobileShop = (DbMobileShopChannel) db;
            } else if (db.getClass() == DbBonusConnectKit.class) {
                dbBonusKit = (DbBonusConnectKit) db;
            } else if (db.getClass() == DbBonus4GChangeSim.class) {
                dbBonusChannelSim4G = (DbBonus4GChangeSim) db;
            } else if (db.getClass() == DbPromotionScaner.class) {
                dbBranchScaner = (DbPromotionScaner) db;
            } else if (db.getClass() == DbSubProfileUpdateProcessor.class) {
                dbSubProfileUpdateProcessor = (DbSubProfileUpdateProcessor) db;
            } else if (db.getClass() == DbKitBatchConnectProcessor.class) {
                dbKitBatchConnectProcessor = (DbKitBatchConnectProcessor) db;
            } else if (db.getClass() == DbKitBatchSubProcessor.class) {
                dbKitBatchProccesser = (DbKitBatchSubProcessor) db;
            } else if (db.getClass() == DbKitBatchRetryPayComission.class) {
                dbKitBatchRetryPayComission = (DbKitBatchRetryPayComission) db;
            }

            String urlStr = ResourceBundle.getBundle("configPayBonus").getString("ewallet_url");
            String user = ResourceBundle.getBundle("configPayBonus").getString("username");
            String funcName = ResourceBundle.getBundle("configPayBonus").getString("functionName");
            String transIdIsdn = transId + mobile;
            eLog.setMobile(mobile);
            eLog.setUrl(urlStr);
            eLog.setUserName(user);
            eLog.setFunctionName(funcName);
            eLog.setActionCode(actionCode);
            eLog.setAmount(amount);
            eLog.setAtionAuditId(aciontAuditId);
            eLog.setChannelTypeId(channelTypeId);
            eLog.setTransId(transIdIsdn);
            eLog.setStaffCode(staffCode);

            try {
                URL url = new URL(urlStr + "api/");
                logger.info("url of api:" + urlStr + "api/" + ", isdnWallet " + mobile);
                WalletBean eWallet = new WalletBean();
                eWallet.setUserName("SyncUser");
                eWallet.setFunctionName(funcName);
                eWallet.setRequestId(transIdIsdn);
                String functionParam = "{\"mobile\":\"%MOBILE%\",\"actionCode\":\"%ACTION_CODE%\",\"amount\":\"%AMOUNT%\",\"transID\":\"%TRANS_ID%\"}";
                functionParam = functionParam.replace("%MOBILE%", mobile)
                        .replace("%AMOUNT%", amount + "")
                        .replace("%ACTION_CODE%", actionCode)
                        .replace("%TRANS_ID%", transIdIsdn);
                eWallet.setFunctionParams(functionParam);
                logger.info("Init Gson Library, isdnWallet " + mobile);
                Gson gson = new Gson();
                request = gson.toJson(eWallet, WalletBean.class);
                eLog.setRequest(request);
                logger.info("request:" + request + " isdnWallet " + mobile);

                HttpURLConnection conn = (HttpURLConnection) url.openConnection();

                conn.setRequestMethod("POST");
                conn.setRequestProperty("Authorization", "Basic TW92aXRlbDpNb3ZpdGVsMTIzIUAj");
                conn.setRequestProperty("Accept", "application/json");
                conn.setRequestProperty("Content-Type", "application/json");
                conn.setConnectTimeout(60000);
                conn.setReadTimeout(60000);
                conn.setDoOutput(true);
                conn.getOutputStream().write(request.getBytes());

                Reader in = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));
                StringBuilder sbJSON = new StringBuilder();
                for (int c;
                        (c = in.read()) >= 0;) {
                    sbJSON.append((char) c);
                }
                response = sbJSON.toString();
                logger.info(response + " isdn " + mobile);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            eLog.setRespone(response);
            Gson responseGson = new Gson();
            ResponseWallet responseWallet = responseGson.fromJson(response, ResponseWallet.class);
            if (responseWallet != null && responseWallet.getResponseCode() != null) {
                errorCode = responseWallet.getResponseCode();
                description = responseWallet.getResponseMessage();
                if ("01".equals(errorCode)) {
                    logger.info("Call eWallet success isdn " + mobile + " amount " + amount);
                } else {
                    logger.error("Call eWallet fail isdn " + mobile + " amount " + amount + " desc " + description);
                }
            } else {
                logger.error("Call eWallet error isdn " + mobile + " amount " + amount);
            }
            eLog.setErrorCode(errorCode);
            eLog.setDescription(description);
            eLog.setDuration(System.currentTimeMillis() - timeSt);
            if (dbBonusFirst != null) {
                dbBonusFirst.insertEwalletLog(eLog);
            } else if (dbBonusSecond != null) {
                dbBonusSecond.insertEwalletLog(eLog);
            } else if (dbCug != null) {
                dbCug.insertEwalletLog(eLog);
            } else if (dbKit != null) {
                dbKit.insertEwalletLog(eLog);
            } else if (dbBonusAgentVip != null) {
                dbBonusAgentVip.insertEwalletLog(eLog);
            } else if (dbMobileShop != null) {
                dbMobileShop.insertEwalletLog(eLog);
            } else if (dbBonusKit != null) {
                dbBonusKit.insertEwalletLog(eLog);
            } else if (dbBonusChannelSim4G != null) {
                dbBonusChannelSim4G.insertEwalletLog(eLog);
            } else if (dbBranchScaner != null) {
                dbBranchScaner.insertEwalletLog(eLog);
            } else if (dbSubProfileUpdateProcessor != null) {
                dbSubProfileUpdateProcessor.insertEwalletLog(eLog);
            } else if (dbKitBatchConnectProcessor != null) {
                dbKitBatchConnectProcessor.insertEwalletLog(eLog);
            } else if (dbKitBatchProccesser != null) {
                dbKitBatchProccesser.insertEwalletLog(eLog);
            } else if (dbKitBatchRetryPayComission != null) {
                dbKitBatchRetryPayComission.insertEwalletLog(eLog);
            }
            return errorCode;
        } catch (Exception ex) {
            logger.error("Had exception call eWallet isdn " + mobile + " amount " + amount);
            try {
                eLog.setErrorCode(errorCode);
                eLog.setDescription(description);
                eLog.setDuration(System.currentTimeMillis() - timeSt);
                if (dbBonusFirst != null) {
                    dbBonusFirst.insertEwalletLog(eLog);
                } else if (dbBonusSecond != null) {
                    dbBonusSecond.insertEwalletLog(eLog);
                } else if (dbCug != null) {
                    dbCug.insertEwalletLog(eLog);
                } else if (dbKit != null) {
                    dbKit.insertEwalletLog(eLog);
                } else if (dbBonusAgentVip != null) {
                    dbBonusAgentVip.insertEwalletLog(eLog);
                } else if (dbMobileShop != null) {
                    dbMobileShop.insertEwalletLog(eLog);
                } else if (dbBonusKit != null) {
                    dbBonusKit.insertEwalletLog(eLog);
                } else if (dbBonusChannelSim4G != null) {
                    dbBonusChannelSim4G.insertEwalletLog(eLog);
                } else if (dbBranchScaner != null) {
                    dbBranchScaner.insertEwalletLog(eLog);
                } else if (dbSubProfileUpdateProcessor != null) {
                    dbSubProfileUpdateProcessor.insertEwalletLog(eLog);
                } else if (dbKitBatchConnectProcessor != null) {
                    dbKitBatchConnectProcessor.insertEwalletLog(eLog);
                } else if (dbKitBatchProccesser != null) {
                    dbKitBatchProccesser.insertEwalletLog(eLog);
                } else if (dbKitBatchRetryPayComission != null) {
                    dbKitBatchRetryPayComission.insertEwalletLog(eLog);
                }
            } catch (Exception e) {
                logger.error("Try insert log eWallet_Log had exception isdn " + mobile);
                logger.error(AppManager.logException(timeSt, ex));
            }
            return errorCode;
        }
    }

    /**
     * Block external subs belong Vodacom and Mcel
     *
     * @param isdn
     * @param extCode VOD: Vodacom ; MCE: Mcel
     * @param exchangeID route to exchange
     * @return
     */
    public String blockExternalSub(String isdn, String extCode, String exchangeID) {
        logger.info("Start block sub " + isdn + " belong " + extCode);
        String DSP = "";
        if ("VOD".equals(extCode)) {
            isdn = isdn.startsWith("258") ? isdn : "258" + isdn;
            DSP = "101";
        } else if ("MCE".equals(extCode)) {
            isdn = isdn.startsWith("258") ? isdn.substring(3) : isdn;
            DSP = "102";
        } else {
            return null;
        }
        ExchMsg request = new ExchMsg();
        long timeSt = System.currentTimeMillis();
        ExchMsg response = null;
        String err = "";
        try {
            request.setCommand("GMSC_HW_ADD_CLRDSG");
            request.setExchId(exchangeID);
            request.set("DSP", DSP);
            request.set("MSISDN", isdn);
            request.set("FUNC", "NIN");
            request.set("DAI", "ALL");
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            err = response.getError();
            logger.info("Response " + response != null ? response.toString() : "response null...");
            logger.info("End blockExternalSub isdn " + isdn + " ext code" + extCode + " result " + err);
            return err;
        } catch (Exception ex) {
            logger.error("Had exception blockExternalSub isdn " + isdn + " ext code" + extCode);
            logger.error(AppManager.logException(timeSt, ex));
            return err;
        }
    }

    /**
     * Unblock external subs belong Vodacom and Mcel
     *
     * @param isdn
     * @param extCode VOD: Vodacom ; MCE: Mcel
     * @param exchangeID route to exchange
     * @return
     */
    public String unblockExternalSub(String isdn, String extCode, String exchangeID) {
        logger.info("Start unblock sub " + isdn + " belong " + extCode);
        String DSP = "";
        if ("VOD".equals(extCode)) {
            isdn = isdn.startsWith("258") ? isdn : "258" + isdn;
            DSP = "101";
        } else if ("MCE".equals(extCode)) {
            isdn = isdn.startsWith("258") ? isdn.substring(3) : isdn;
            DSP = "102";
        } else {
            return null;
        }
        ExchMsg request = new ExchMsg();
        long timeSt = System.currentTimeMillis();
        ExchMsg response = null;
        String err = "";
        try {
            request.setCommand("GMSC_HW_RMV_CLRDSG");
            request.setExchId(exchangeID);
            request.set("DSP", DSP);
            request.set("MSISDN", isdn);
            request.set("CDADDR", "ALL");
            request.set("DAI", "ALL");
            request.set("CUSTID", "65534");
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            err = response.getError();
            logger.info("Response " + response != null ? response.toString() : "response null...");
            logger.info("End unblockExternalSub isdn " + isdn + " ext code" + extCode + " result " + err);
            return err;
        } catch (Exception ex) {
            logger.error("Had exception unblockExternalSub isdn " + isdn + " ext code" + extCode);
            logger.error(AppManager.logException(timeSt, ex));
            return err;
        }
    }

    //LinhNBV 20190914: Add method get original base on command.
    public String getOriginalOfCommand(String msisdn, String command, HashMap<String, String> lstParams) {
        ExchMsg request = new ExchMsg();
        long timeSt = System.currentTimeMillis();
        ExchMsg response = null;
        String original = "";
        if (!msisdn.startsWith("258")) {
            msisdn = "258" + msisdn;
        }
        try {
            request.setCommand(command);
            for (String tmpKey : lstParams.keySet()) {
                String key = tmpKey;
                String value = lstParams.get(key);
                request.set(key, value);
            }
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            original = response.getOriginal();
            logger.info("End getOriginalOfCommand isdn " + msisdn);
            return original;
        } catch (Exception ex) {
            logger.error("Had exception getOriginalOfCommand isdn " + msisdn);
            logger.error(AppManager.logException(timeSt, ex));
            return original;
        }
    }

    /**
     * Check sub status on GMSC by command LST_CLRDSG
     *
     * @param isdn
     * @param extCode detect Vodacom or Mcel
     * @return
     */
    public boolean checkSubStatusOnGmsc(String isdn, String extCode) {
        logger.info("Start checkBlockExtSubGMSC sub " + isdn);
        ExchMsg request = new ExchMsg();
        long timeSt = System.currentTimeMillis();
        ExchMsg response = null;
        String strDes = "";
        boolean result = false;
        if ("VOD".equals(extCode)) {
            isdn = isdn.startsWith("258") ? isdn : "258" + isdn;
        } else if ("MCE".equals(extCode)) {
            isdn = isdn.startsWith("258") ? isdn.substring(3) : isdn;
        } else {
            logger.info("Input invalid extCode :" + extCode);
            return result;
        }
        try {
            request.setCommand("LST_CLRDSG");
            request.set("ECLI", "K'" + isdn);
            request.set("SCLI", "K'" + isdn);
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            logger.info("Response " + response != null ? response.toString() : "response null...");
            if ("0".equals(response.getError())) {
                strDes = (response.get("DETAIL") != null) ? (String) response.get("DETAIL") : null;
                if (strDes != null && strDes.contains(isdn)) {
                    result = true;
                }
            }
            logger.info("Response " + response != null ? response.toString() : "response null...");
            logger.info("End checkBlockExtSubGMSC isdn " + isdn + " result " + result);
            return result;
        } catch (Exception ex) {
            logger.error("Had exception checkBlockExtSubGMSC isdn " + isdn);
            logger.error(AppManager.logException(timeSt, ex));
            return false;
        }
    }

    public String[] createGroupCUG(String groupId) {
        ExchMsg request = new ExchMsg();
        ExchMsg response = null;
        long start = System.currentTimeMillis();
        try {
            request.setExchId("vocs3_1");//Don't have isdn, can't routing to vOCS
            request.setCommand("OCS_ADD_VPN_CLUSTER");
            request.set("GROUP_ID", groupId);
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            logTime("Time to createGroupCUG groupId " + groupId + ", errorcode " + response.getError(), start);
            String res = response.getError() + "|" + response.getDescription();
            return res.split("\\|");
        } catch (Exception e) {
            logTime("Exception to createGroupCUG groupId " + groupId, start);
            logger.error(AppManager.logException(start, e));
            return null;
        }
    }

    public String[] addMemberGroupCUG(String groupId, String isdn) {
        ExchMsg request = new ExchMsg();
        ExchMsg response = null;
        long start = System.currentTimeMillis();
        try {
            request.setCommand("OCS_ADD_VPN_MEMBER");
            if (!isdn.startsWith("258")) {
                isdn = "258" + isdn;
            }
            request.set("MSISDN", isdn);
            request.set("USER_GROUP_ID", groupId);
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            logTime("Time to addMemberGroupCUG groupId " + groupId + ", errorcode " + response.getError(), start);
            String res = response.getError() + "|" + response.getDescription();
            return res.split("\\|");
        } catch (Exception e) {
            logTime("Exception to addMemberGroupCUG groupId " + groupId, start);
            logger.error(AppManager.logException(start, e));
            return null;
        }
    }

    public List<Offer> parseListOffer(String originalXML) throws ParserConfigurationException, SAXException, IOException {
        List<Offer> listOffer = new ArrayList<Offer>();
        Offer offer = null;

        String xmlRecords = originalXML;

        DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        InputSource is = new InputSource();
        is.setCharacterStream(new StringReader(xmlRecords));
        Document doc = db.parse(is);
        NodeList nList = doc.getElementsByTagName("Offer");
        System.out.println(nList.getLength());
        for (int temp = 0; temp < nList.getLength(); temp++) {
            Node node = nList.item(temp);
            if (node.getNodeType() == Node.ELEMENT_NODE) {
                org.w3c.dom.Element eElement = (org.w3c.dom.Element) node;
                offer = new Offer();
                offer.setName(eElement.getElementsByTagName("Name").item(0).getTextContent());
                offer.setId(eElement.getElementsByTagName("Id").item(0).getTextContent());
                offer.setVersion(eElement.getElementsByTagName("Version").item(0).getTextContent());
                offer.setExpDate(eElement.getElementsByTagName("ExpDate").item(0).getTextContent());
                offer.setEffDate(eElement.getElementsByTagName("EffDate").item(0).getTextContent());
                offer.setIdMember(eElement.getElementsByTagName("IdMember").item(0).getTextContent());
                offer.setListMember(eElement.getElementsByTagName("ListMember").item(0).getTextContent());
                offer.setState(eElement.getElementsByTagName("State").item(0).getTextContent());
                offer.setRecurringDate(eElement.getElementsByTagName("RecurringDate").item(0).getTextContent());
                offer.setUpgradeOrNot(eElement.getElementsByTagName("UpgradeOrNot").item(0).getTextContent());
                offer.setUpgradeTime(eElement.getElementsByTagName("UpgradeTime").item(0).getTextContent());
                offer.setDowngradeTime(eElement.getElementsByTagName("DowngradeTime").item(0).getTextContent());
                listOffer.add(offer);
            }
        }
        return listOffer;
    }

    public String changeProduct(String msisdn, String newProduct) {
        ExchMsg request = new ExchMsg();
        long timeSt = System.currentTimeMillis();
        ExchMsg response = null;
        String err = "";
        try {
            if (!msisdn.startsWith("258")) {
                msisdn = "258" + msisdn;
            }
            request.setCommand("OCSHW_CHANGEMAINPROD1");
            request.set("MSISDN", msisdn);
            request.set("NEW_MAIN_PRODUCT", newProduct);
            response = (ExchMsg) channel.sendAll(request, REQUEST_TIME_OUT, true);
            err = response.getError();
            logger.info("End changeProduct isdn " + msisdn + " newProduct " + newProduct + " result " + err);
            return err;
        } catch (Exception ex) {
            logger.error("Had exception changeProduct isdn " + msisdn + " newProduct " + newProduct);
            logger.error(AppManager.logException(timeSt, ex));
            return err;
        }
    }

    public String getStatusTransactionV2(EwalletDebitLog newLog) throws Exception {//return description for ex: 20410068058|21000.0000|1
        long timeSt = System.currentTimeMillis();
        String request = "";
        String response = "";
        String erroCode = "";
        String description = "";
        try {
            logger.info("Start getStatusTransaction staff " + newLog.getStaffCode()
                    + " orgRequestId " + newLog.getRequestId());
            String strUrl = ResourceBundle.getBundle("configPayBonus").getString("urlGetStatusTransaction");
            String partnerCode = ResourceBundle.getBundle("configPayBonus").getString("DebitEwalletPartnerCode");//base.key.api.emola
            String keyGetStatus = ResourceBundle.getBundle("configPayBonus").getString("keyGetStatus");//
            String requestDate = sdf.format(new Date());
            String requestId = partnerCode + requestDate;

            RevertTransaction paymentVoucher = new RevertTransaction();
            paymentVoucher.setRequestId(requestId);
            paymentVoucher.setOrgRequestId(newLog.getRequestId());
            paymentVoucher.setPartnerCode(partnerCode);
            paymentVoucher.setRequestDate(requestDate);

            String tempSignature = keyGetStatus + requestDate + partnerCode + requestId + newLog.getRequestId();
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(tempSignature.getBytes());
            byte[] digest = md.digest();
            StringBuilder sb = new StringBuilder();
            for (byte b : digest) {
                sb.append(String.format("%02x", b & 0xff));
            }
            logger.info("original:" + tempSignature);
            logger.info("digested(hex):" + sb.toString());
            paymentVoucher.setSignature(sb.toString());
            Gson gson = new Gson();
            request = gson.toJson(paymentVoucher, RevertTransaction.class);
            URL url = new URL(strUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setConnectTimeout(60000);
            conn.setReadTimeout(60000);
            conn.setRequestProperty("Authorization", "Basic TW92aXRlbDpNb3ZpdGVsMTIzIUAj");
            conn.setRequestProperty("Accept", "application/json");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);
            conn.getOutputStream().write(request.getBytes());
            Reader in = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));
            StringBuilder sbJSON = new StringBuilder();
            for (int c; (c = in.read()) >= 0;) {
                sbJSON.append((char) c);
            }
            response = sbJSON.toString();
            Gson responseGson = new Gson();
            ResponseWallet responseWallet = responseGson.fromJson(response, ResponseWallet.class);
            erroCode = responseWallet.getResponseCode();
            description = responseWallet.getResponseMessage();
            if (description == null) {
                description = "NA";
            }
            if ("01".equals(erroCode)) {
                logger.error("Transaction success " + newLog.getStaffCode()
                        + " orgRequestId " + newLog.getRequestId());
                return "01|" + description;
            } else {
                return "02|" + description;
            }
        } catch (Exception ex) {
            logger.error("Had exception call callEwalletRollback staff " + newLog.getStaffCode()
                    + " orgRequestId " + newLog.getRequestId());
            logger.error(AppManager.logException(timeSt, ex));
            return "";
        }
    }
}
