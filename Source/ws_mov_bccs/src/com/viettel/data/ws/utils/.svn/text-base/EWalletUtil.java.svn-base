/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.data.ws.utils;

import com.google.gson.Gson;
import com.viettel.smsfw.database.DbProcessorAbstract;
import com.viettel.vas.wsfw.database.DbProcessor;
import com.viettel.vas.wsfw.object.EmolaBean;
import com.viettel.vas.wsfw.object.QueryEwalletInfo;
import com.viettel.vas.wsfw.object.ResponseWallet;
import com.viettel.vas.wsfw.object.RevertTransaction;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.ResourceBundle;
import org.apache.log4j.Logger;

/**
 *
 * @author itbl_linh
 */
public class EWalletUtil {

    public static ResponseWallet revertTransaction(DbProcessorAbstract db, String userName, Logger logger, String orgRequestId, String isdn)
            throws Exception {
        if (orgRequestId == null || orgRequestId.trim().length() <= 0) {
            return new ResponseWallet();
        }
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        String partnerCode = "E-MOLA";
        String request = "";
        String response = "";
        logger.info("Start call callEwalletRollback staff " + userName + " isdn " + isdn);
        String strUrl = ResourceBundle.getBundle("vas").getString("debit_enter_ewallet_url");
        String requestDate = sdf.format(new Date());
        String requestId = "0" + orgRequestId;
        ResponseWallet responseWallet = null;
        DbProcessor dbKitBatch = null;
        if (db.getClass() == DbProcessor.class) {
            dbKitBatch = (DbProcessor) db;
        }

        try {
            RevertTransaction paymentVoucher = new RevertTransaction();
            paymentVoucher.setRequestId(requestId);
            paymentVoucher.setRequestDate(requestDate);
            paymentVoucher.setPartnerCode(partnerCode);
            paymentVoucher.setOrgRequestId(orgRequestId);
            String tempSignature = "Az1gW2WHRlzus3LqNB63kedzkWk6OjfUOrXUj7nw" + requestDate + partnerCode + requestId + orgRequestId;
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
            response = sbJSON.toString();
            logger.info("isdn " + isdn + " " + response);
            responseWallet = gson.fromJson(response, ResponseWallet.class);
            if (dbKitBatch != null) {
                dbKitBatch.insertEwalletConnectKitLog(2L, requestId, "", "", userName, request, response,
                        responseWallet.getResponseCode(), responseWallet.getResponseMessage(), responseWallet.getRequestId(), isdn);
            }

            return responseWallet;

        } catch (Exception e) {
            if (dbKitBatch != null) {
                dbKitBatch.insertEwalletConnectKitLog(2L, requestId, "", "", userName, request, response,
                        "98", "Have exception when revert transaction: " + e.getMessage(), "", isdn);
            }

            return responseWallet;
        }
    }

    public static ResponseWallet chargeEmolaEnterprise(String requestId, DbProcessorAbstract db, String enterpriseAccount,
            Double totalAmount, Logger logger, String isdn, String staffCode)
            throws Exception {
        long timeSt = System.currentTimeMillis();
        SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH.mm.ss");
        Date now = new Date();
        ResponseWallet responseWallet = null;
        Reader in = null;
        String request = "";
        String response = "";
        DbProcessor dbKitBatch = null;
        if (db.getClass() == DbProcessor.class) {
            dbKitBatch = (DbProcessor) db;
        }
        try {
            String strDate = sdfDate.format(now).replace("-", "").replace(".", "").replace(" ", "");
            String requestDate = strDate;
            int transactionType = 1;
            String partnerCode = ResourceBundle.getBundle("vas").getString("debitEnte_emola_partnerCode");
            String urlString = ResourceBundle.getBundle("vas").getString("debit_enter_ewallet_url");
            String key = ResourceBundle.getBundle("vas").getString("debitEnte_emola_key");
            URL url = new URL(urlString);
            EmolaBean emola = new EmolaBean();
            emola.setRequestId(requestId);
            emola.setRequestDate(requestDate);
            emola.setPartnerCode(partnerCode);
            emola.setEnterpriseAccount(enterpriseAccount);
            emola.setAmount(totalAmount + "");
            emola.setContent("Deduct emola when connect KIT " + isdn);
            emola.setTransactionType(transactionType);
            String tempSignature = key + requestDate + totalAmount + enterpriseAccount + partnerCode + requestId;
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
            logger.info("Request: " + request + " isdn " + isdn);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Authorization", "Basic TW92aXRlbDpNb3ZpdGVsMTIzIUAj");
            conn.setRequestProperty("Accept", "application/json");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setConnectTimeout(60000);
            conn.setReadTimeout(60000);
            conn.setDoOutput(true);
            conn.getOutputStream().write(request.getBytes());
            logger.info(isdn + " Response code when open Connection: "
                    + conn.getResponseCode() + "\nResponse message:" + conn.getResponseMessage());
            in = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));
            StringBuilder sbJSON = new StringBuilder();
            for (int c;
                    (c = in.read()) >= 0;) {
                sbJSON.append((char) c);
            }
            response = sbJSON.toString();
            logger.info("Response Wallet: " + response + " isdn " + isdn);
            responseWallet = gson.fromJson(response, ResponseWallet.class);
            if (dbKitBatch != null) {
                dbKitBatch.insertEwalletConnectKitLog(1L, requestId, String.valueOf(totalAmount), "863143140", staffCode, request,
                        response, responseWallet.getResponseCode(), responseWallet.getResponseMessage(), responseWallet.getRequestId(), isdn);
            }


        } catch (Exception e) {
            if (dbKitBatch != null) {
                dbKitBatch.insertEwalletConnectKitLog(1L, requestId, String.valueOf(totalAmount), "863143140", staffCode, request,
                        response, "99", "Have exception when payment voucher: " + e.getMessage(), "", isdn);
            }


        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (Exception ex) {
                    logger.error("Failt to close inputstream " + ex.toString());
                }
            }
            logger.info("chargeEmolaEnterprise isdn " + isdn + " amount " + totalAmount + " duration "
                    + (System.currentTimeMillis() - timeSt));
            return responseWallet;
        }
    }

    public static ResponseWallet chargeEmolaEnterpriseV2(String requestId, DbProcessorAbstract db, String enterpriseAccount,
            Double totalAmount, Logger logger, String isdn, String staffCode, String key, String partnerCode, String description)
            throws Exception {
        long timeSt = System.currentTimeMillis();
        SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH.mm.ss");
        Date now = new Date();
        ResponseWallet responseWallet = null;
        Reader in = null;
        String request = "";
        String response = "";
        DbProcessor dbKitBatch = null;
        if (db.getClass() == DbProcessor.class) {
            dbKitBatch = (DbProcessor) db;
        }
        try {
            String strDate = sdfDate.format(now).replace("-", "").replace(".", "").replace(" ", "");
            String requestDate = strDate;
            int transactionType = 1;
//            String partnerCode = ResourceBundle.getBundle("configPayBonus").getString("debitEnte_emola_partnerCode");
            String urlString = ResourceBundle.getBundle("vas").getString("debit_enter_ewallet_url");
//            String key = ResourceBundle.getBundle("configPayBonus").getString("debitEnte_emola_key");
            URL url = new URL(urlString);
            EmolaBean emola = new EmolaBean();
            emola.setRequestId(requestId);
            emola.setRequestDate(requestDate);
            emola.setPartnerCode(partnerCode);
            emola.setEnterpriseAccount(enterpriseAccount);
            emola.setAmount(totalAmount + "");
            emola.setContent(description);
            emola.setTransactionType(transactionType);
            String tempSignature = key + requestDate + totalAmount + enterpriseAccount + partnerCode + requestId;
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
            logger.info("Request: " + request + " isdn " + isdn);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
//            conn.setRequestProperty("Authorization", "Basic TW92aXRlbDp6b3Awc3QhQCM=");
            conn.setRequestProperty("Authorization", "Basic TW92aXRlbDpNb3ZpdGVsMTIzIUAj");
            conn.setRequestProperty("Accept", "application/json");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setConnectTimeout(60000);
            conn.setReadTimeout(60000);
            conn.setDoOutput(true);
            conn.getOutputStream().write(request.getBytes());
            logger.info(isdn + " Response code when open Connection: "
                    + conn.getResponseCode() + "\nResponse message:" + conn.getResponseMessage());
            in = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));
            StringBuilder sbJSON = new StringBuilder();
            for (int c;
                    (c = in.read()) >= 0;) {
                sbJSON.append((char) c);
            }
            response = sbJSON.toString();
            logger.info("Response Wallet: " + response + " isdn " + isdn);
            responseWallet = gson.fromJson(response, ResponseWallet.class);
            if (dbKitBatch != null) {
                dbKitBatch.insertEwalletConnectKitLog(1L, requestId, String.valueOf(totalAmount), "863143140", staffCode, request,
                        response, responseWallet.getResponseCode(), responseWallet.getResponseMessage(), responseWallet.getRequestId(), isdn);
            }


        } catch (Exception e) {
            if (dbKitBatch != null) {
                dbKitBatch.insertEwalletConnectKitLog(1L, requestId, String.valueOf(totalAmount), "863143140", staffCode, request,
                        response, "99", "Have exception when payment voucher: " + e.getMessage(), "", isdn);
            }


        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (Exception ex) {
                    logger.error("Failt to close inputstream " + ex.toString());
                }
            }
            logger.info("chargeEmolaEnterprise isdn " + isdn + " amount " + totalAmount + " duration "
                    + (System.currentTimeMillis() - timeSt));
            return responseWallet;
        }
    }

    public static QueryEwalletInfo queryCustomerInfo(String isdn, Logger logger)
            throws Exception {
        long timeSt = System.currentTimeMillis();
        QueryEwalletInfo responseWallet = null;
        Reader in = null;
        String request = "";
        String response = "";
        if (isdn.startsWith("258")) {
            isdn = isdn.substring(3);
        }
        try {
            String urlString = ResourceBundle.getBundle("vas").getString("query_customer_emola_url") + isdn;
            URL url = new URL(urlString);
            logger.info("Request: " + request + " isdn " + isdn);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Authorization", "Basic TW92aXRlbDp6b3Awc3QhQCM=");
//            conn.setRequestProperty("Authorization", "Basic TW92aXRlbDpNb3ZpdGVsMTIzIUAj");
            conn.setRequestProperty("Accept", "application/json");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setConnectTimeout(60000);
            conn.setReadTimeout(60000);
            conn.setDoOutput(true);
            conn.getOutputStream().write(request.getBytes());
            logger.info(isdn + " Response code when open Connection: "
                    + conn.getResponseCode() + "\nResponse message:" + conn.getResponseMessage());
            in = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));
            StringBuilder sbJSON = new StringBuilder();
            for (int c;
                    (c = in.read()) >= 0;) {
                sbJSON.append((char) c);
            }
            response = sbJSON.toString();
            logger.info("Response Wallet: " + response + " isdn " + isdn);
            Gson gson = new Gson();
            responseWallet = gson.fromJson(response, QueryEwalletInfo.class);


        } catch (Exception e) {
            logger.error("Failt to close inputstream " + e.toString());

        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (Exception ex) {
                    logger.error("Failt to close inputstream " + ex.toString());
                }
            }
            logger.info("query customer information isdn " + isdn + " duration "
                    + (System.currentTimeMillis() - timeSt));
            return responseWallet;
        }
    }

    private static Date string2Date(String value) throws Exception {
        SimpleDateFormat dateTime = new SimpleDateFormat("ddMMyyyy");
        return dateTime.parse(value);
    }
}
