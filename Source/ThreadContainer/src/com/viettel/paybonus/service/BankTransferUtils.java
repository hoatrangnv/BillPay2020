/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.service;

import java.util.ResourceBundle;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.RequestEntity;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;

/**
 *
 * @author dev_linh
 */
public class BankTransferUtils {

    public static String callWSSOAPcheckTrans(String transCode, String bankName, String amount) {
        if (bankName.length() > 3) {
            bankName = bankName.substring(0, 3);
        }
        String wsdlUrl = ResourceBundle.getBundle("configPayBonus").getString("urlBankTransferWS");
        org.apache.commons.httpclient.HttpClient httpTransport = null;
        MultiThreadedHttpConnectionManager conMgr = null;
        //innit connection
        if (conMgr == null) {
            conMgr = new MultiThreadedHttpConnectionManager();
            conMgr.setMaxConnectionsPerHost(20000);
            conMgr.setMaxTotalConnections(20000);
        }
        if (httpTransport == null) {
            httpTransport = new org.apache.commons.httpclient.HttpClient(conMgr);
            HttpConnectionManager conMgr1 = httpTransport.getHttpConnectionManager();
            HttpConnectionManagerParams conPars = conMgr1.getParams();
            conPars.setMaxTotalConnections(2000);
            conPars.setConnectionTimeout(30000); //timeout ket noi : ms
            conPars.setSoTimeout(60000); //timeout doc ket qua : ms
        }

        PostMethod post = new PostMethod(wsdlUrl);
        String soapResponse = "";
        String result = "";
        String userName = "00797ce8fadf77a6e0f9019ba58022f5";
        String pass = "00797ce8fadf77a6f13baddb16669dfc6a6c7d6a93c496cb";
        try {
            String request = " <soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:ser=\"http://services.wsfw.vas.viettel.com/\">\n"
                    + "   <soapenv:Header/>\n"
                    + "   <soapenv:Body>\n"
                    + "      <ser:checkTrans>\n"
                    + "         <!--Optional:-->\n"
                    + "         <transCode>" + transCode.replace("&", "&amp;") + "</transCode>\n"
                    + "         <!--Optional:-->\n"
                    + "         <bankName>" + bankName + "</bankName>\n"
                    + "         <!--Optional:-->\n"
                    + "         <amount>" + amount + "</amount>\n"
                    + "         <!--Optional:-->\n"
                    + "         <userName>" + userName + "</userName>\n"
                    + "         <!--Optional:-->\n"
                    + "         <passWord>" + pass + "</passWord>\n"
                    + "      </ser:checkTrans>\n"
                    + "   </soapenv:Body>\n"
                    + "</soapenv:Envelope> ";

            RequestEntity entity = new StringRequestEntity(request, "text/xml", "UTF-8");
            post.setRequestEntity(entity);
            httpTransport.executeMethod(post);
            soapResponse = post.getResponseBodyAsString(609600000);
            //Parse reponse
            XmlConfig soapMsg = new XmlConfig();
            soapMsg.load(soapResponse, null);
            org.jdom.Element root = soapMsg.getDocument().getRootElement();
            org.jdom.Element ele = XmlUtil.findElement(root, "errorCode");
            result += ele.getText();
            return result;
        } catch (Exception ex) {
            return result;
        } finally {
            post.releaseConnection();
        }
    }

    public static String callWSSOAPupdateTrans(String transCode, String bankName, String amount, String staffApprove, String staffCodeCreate) {
        if (bankName.length() > 3) {
            bankName = bankName.substring(0, 3);
        }
        String wsdlUrl = ResourceBundle.getBundle("configPayBonus").getString("urlBankTransferWS");
        org.apache.commons.httpclient.HttpClient httpTransport = null;
        MultiThreadedHttpConnectionManager conMgr = null;
        //innit connection
        if (conMgr == null) {
            conMgr = new MultiThreadedHttpConnectionManager();
            conMgr.setMaxConnectionsPerHost(20000);
            conMgr.setMaxTotalConnections(20000);
        }
        if (httpTransport == null) {
            httpTransport = new org.apache.commons.httpclient.HttpClient(conMgr);
            HttpConnectionManager conMgr1 = httpTransport.getHttpConnectionManager();
            HttpConnectionManagerParams conPars = conMgr1.getParams();
            conPars.setMaxTotalConnections(2000);
            conPars.setConnectionTimeout(30000); //timeout ket noi : ms
            conPars.setSoTimeout(60000); //timeout doc ket qua : ms
        }

        PostMethod post = new PostMethod(wsdlUrl);
        String soapResponse = "";
        String result = "";
        String userName = "00797ce8fadf77a6e0f9019ba58022f5";
        String pass = "00797ce8fadf77a6f13baddb16669dfc6a6c7d6a93c496cb";
        try {
            String request = " <soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:ser=\"http://services.wsfw.vas.viettel.com/\">\n"
                    + "   <soapenv:Header/>\n"
                    + "   <soapenv:Body>\n"
                    + "      <ser:updateTrans>\n"
                    + "         <!--Optional:-->\n"
                    + "         <transCode>" + transCode.replace("&", "&amp;") + "</transCode>\n"
                    + "         <!--Optional:-->\n"
                    + "         <bankName>" + bankName + "</bankName>\n"
                    + "         <!--Optional:-->\n"
                    + "         <amount>" + amount + "</amount>\n"
                    + "         <!--Optional:-->\n"
                    + "         <userName>" + userName + "</userName>\n"
                    + "         <!--Optional:-->\n"
                    + "         <passWord>" + pass + "</passWord>\n"
                    + "         <staffCreate>" + staffCodeCreate + "</staffCreate>\n"
                    + "         <staffApprove>" + staffApprove + "</staffApprove>\n"
                    + "         <purpose>2</purpose> "
                    + "      </ser:updateTrans>\n"
                    + "   </soapenv:Body>\n"
                    + "</soapenv:Envelope> ";

            RequestEntity entity = new StringRequestEntity(request, "text/xml", "UTF-8");
            post.setRequestEntity(entity);
            httpTransport.executeMethod(post);
            soapResponse = post.getResponseBodyAsString(609600000);
            //Parse reponse
            XmlConfig soapMsg = new XmlConfig();
            soapMsg.load(soapResponse, null);
            org.jdom.Element root = soapMsg.getDocument().getRootElement();
            org.jdom.Element ele = XmlUtil.findElement(root, "errorCode");
            result += ele.getText();
            return result;
        } catch (Exception ex) {
            return result;
        } finally {
            post.releaseConnection();
        }
    }

    public static String callWSSOAProllbackTrans(String transCode, String bankName, String amount, String staffApprove, String staffCodeCreate) {
        if (bankName.length() > 3) {
            bankName = bankName.substring(0, 3);
        }
        String wsdlUrl = ResourceBundle.getBundle("configPayBonus").getString("urlBankTransferWS");
        org.apache.commons.httpclient.HttpClient httpTransport = null;
        MultiThreadedHttpConnectionManager conMgr = null;
        //innit connection
        if (conMgr == null) {
            conMgr = new MultiThreadedHttpConnectionManager();
            conMgr.setMaxConnectionsPerHost(20000);
            conMgr.setMaxTotalConnections(20000);
        }
        if (httpTransport == null) {
            httpTransport = new org.apache.commons.httpclient.HttpClient(conMgr);
            HttpConnectionManager conMgr1 = httpTransport.getHttpConnectionManager();
            HttpConnectionManagerParams conPars = conMgr1.getParams();
            conPars.setMaxTotalConnections(2000);
            conPars.setConnectionTimeout(30000); //timeout ket noi : ms
            conPars.setSoTimeout(60000); //timeout doc ket qua : ms
        }

        PostMethod post = new PostMethod(wsdlUrl);
        String soapResponse = "";
        String result = "";
        String userName = "00797ce8fadf77a6e0f9019ba58022f5";
        String pass = "00797ce8fadf77a6f13baddb16669dfc6a6c7d6a93c496cb";
        try {
            String request = "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:ser=\"http://services.wsfw.vas.viettel.com/\">\n"
                    + "   <soapenv:Header/>\n"
                    + "   <soapenv:Body>\n"
                    + "      <ser:rollbackTrans>\n"
                    + "         <!--Optional:-->\n"
                    + "         <transCode>" + transCode.replace("&", "&amp;") + "</transCode>\n"
                    + "         <!--Optional:-->\n"
                    + "         <bankName>" + bankName + "</bankName>\n"
                    + "         <!--Optional:-->\n"
                    + "         <amount>" + amount + "</amount>\n"
                    + "         <!--Optional:-->\n"
                    + "         <reason>systemSM_rollback_cancelOrder</reason>\n"
                    + "         <!--Optional:-->\n"
                    + "         <userName>" + userName + "</userName>\n"
                    + "         <!--Optional:-->\n"
                    + "         <passWord>" + pass + "</passWord>\n"
                    + "      </ser:rollbackTrans>\n"
                    + "   </soapenv:Body>\n"
                    + "</soapenv:Envelope>";

            RequestEntity entity = new StringRequestEntity(request, "text/xml", "UTF-8");
            post.setRequestEntity(entity);
            httpTransport.executeMethod(post);
            soapResponse = post.getResponseBodyAsString(609600000);
            //Parse reponse
            XmlConfig soapMsg = new XmlConfig();
            soapMsg.load(soapResponse, null);
            org.jdom.Element root = soapMsg.getDocument().getRootElement();
            org.jdom.Element ele = XmlUtil.findElement(root, "errorCode");
            result += ele.getText();
            return result;
        } catch (Exception ex) {
            return result;
        } finally {
            post.releaseConnection();
        }
    }
}
