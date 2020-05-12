/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.service;

import com.google.gson.Gson;
import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbChangeSim4GProcessor;
import com.viettel.paybonus.database.DbConnectKitProcessor;
import com.viettel.paybonus.database.DbCreateChannelProcessor;
import com.viettel.paybonus.database.DbKitBatchConnectProcessor;
import com.viettel.paybonus.database.DbKitChangeToStudentProcessor;
import com.viettel.paybonus.obj.Bonus;
import com.viettel.paybonus.obj.ChannelWalletBean;
import com.viettel.paybonus.obj.EmolaBean;
import com.viettel.paybonus.obj.KitBatchInfo;
import com.viettel.paybonus.obj.PaymentVoucher;
import com.viettel.paybonus.obj.QueryInforResponse;
import com.viettel.paybonus.obj.RequestChangeSim;
import com.viettel.paybonus.obj.ResponseWallet;
import com.viettel.paybonus.obj.RevertTransaction;
import com.viettel.paybonus.obj.Subscriber;
import com.viettel.paybonus.obj.SubscriberWallet;
import com.viettel.threadfw.database.DbProcessorAbstract;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.MessageDigest;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.apache.log4j.Logger;

/**
 *
 * @author itbl_linh
 */
public class EWalletUtil {

    public static Logger logger = Logger.getLogger(EWalletUtil.class);

    public static ResponseWallet paymentVoucher(DbProcessorAbstract db, Record record, Double totalAmount, Logger logger, String isdn) throws Exception {
        ResponseWallet responseWallet = null;
        SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH.mm.ss");
        Date now = new Date();
        String strDate = sdfDate.format(now).replace("-", "").replace(".", "").replace(" ", "");
        Bonus bn = null;
        RequestChangeSim requestChangeSim = null;
        KitBatchInfo kitBatchInfo = null;
        String tmpId = "", voucherCode = "", staffCode = "";
        if (record.getClass() == Bonus.class) {
            bn = (Bonus) record;
            tmpId = bn.getId() + "";
            staffCode = bn.getUserName();
            voucherCode = bn.getVoucherCode();
        } else if (record.getClass() == RequestChangeSim.class) {
            requestChangeSim = (RequestChangeSim) record;
            tmpId = requestChangeSim.getId() + "";
            voucherCode = requestChangeSim.getVoucherCode();
            staffCode = requestChangeSim.getStaffCode();
        } else if (record.getClass() == KitBatchInfo.class) {
            kitBatchInfo = (KitBatchInfo) record;
            tmpId = kitBatchInfo.getKitBatchId() + "";
            staffCode = kitBatchInfo.getCreateUser().toUpperCase();
            voucherCode = kitBatchInfo.getEmolaVoucherCode();
        }
        if (tmpId == null || voucherCode == null || staffCode == null || tmpId.isEmpty() || voucherCode.isEmpty() || staffCode.isEmpty()) {
            return responseWallet;
        }
        String requestId = strDate + tmpId;
        String requestDate = strDate;
        String partnerCode = "E-MOLA";
        DbConnectKitProcessor dbConnectKit = null;
        DbChangeSim4GProcessor dbChangeSim4G = null;
        DbKitBatchConnectProcessor dbKitBatch = null;
        DbKitChangeToStudentProcessor dbChangeStudentProduct = null;
        if (db.getClass() == DbConnectKitProcessor.class) {
            dbConnectKit = (DbConnectKitProcessor) db;
        } else if (db.getClass() == DbChangeSim4GProcessor.class) {
            dbChangeSim4G = (DbChangeSim4GProcessor) db;
        } else if (db.getClass() == DbKitBatchConnectProcessor.class) {
            dbKitBatch = (DbKitBatchConnectProcessor) db;
        } else if (db.getClass() == DbKitChangeToStudentProcessor.class) {
            dbChangeStudentProduct = (DbKitChangeToStudentProcessor) db;
        }
        String mobile = "";
        if (dbConnectKit != null) {
            mobile = dbConnectKit.getIsdnWalletByStaffCode(staffCode);
        } else if (dbChangeSim4G != null) {
            mobile = dbChangeSim4G.getIsdnWalletByStaffCode(staffCode);
        } else if (dbKitBatch != null) {
            mobile = isdn;
        } else if (dbChangeStudentProduct != null) {
            mobile = dbChangeStudentProduct.getIsdnWalletByStaffCode(staffCode);
        }
        if (mobile == null || mobile.isEmpty()) {
            return responseWallet;
        }
//        String voucherCode = bn.getVoucherCode();
        int transactionType = 5;
        Locale locale = Locale.ENGLISH;
        NumberFormat nf = NumberFormat.getNumberInstance(locale);
        nf.setMinimumFractionDigits(2);
        nf.setMaximumFractionDigits(2);
        String money = nf.format(totalAmount);
        String request = "";
        String response = "";
        try {
            String urlPaymentVoucher = ResourceBundle.getBundle("configPayBonus").getString("urlPaymentVoucher");
            String keyPaymentVoucher = ResourceBundle.getBundle("configPayBonus").getString("keyPaymentVoucher");
            URL url = new URL(urlPaymentVoucher);
            PaymentVoucher paymentVoucher = new PaymentVoucher();
            paymentVoucher.setRequestId(requestId);
            paymentVoucher.setRequestDate(requestDate);
            paymentVoucher.setPartnerCode(partnerCode);
            paymentVoucher.setMobile(mobile);
            paymentVoucher.setAmount(money);
            paymentVoucher.setVoucherCode(voucherCode);
            paymentVoucher.setTransactionType(transactionType);
            String tempSignature = keyPaymentVoucher + requestDate + money + mobile + partnerCode + voucherCode + requestId;
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(tempSignature.getBytes());
            byte[] digest = md.digest();
            StringBuilder sb = new StringBuilder();
            for (byte b : digest) {
                sb.append(String.format("%02x", b & 0xff));
            }
//            logger.info("original:" + tempSignature);
//            logger.info("digested(hex):" + sb.toString());
            paymentVoucher.setSignature(sb.toString());
            Gson gson = new Gson();
            request = gson.toJson(paymentVoucher, PaymentVoucher.class);
            logger.info("request:" + request + " isdn " + isdn);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Authorization", "Basic TW92aXRlbDpNb3ZpdGVsMTIzIUAj");
            conn.setRequestProperty("Accept", "application/json");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setConnectTimeout(60000);
            conn.setReadTimeout(60000);
            conn.setDoOutput(true);
            conn.getOutputStream().write(request.getBytes());
            logger.info("Response code when open Connection: " + conn.getResponseCode() + " isdn " + isdn
                    + "\nResponse message:" + conn.getResponseMessage());
            Reader in = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));
            StringBuilder sbJSON = new StringBuilder();
            for (int c;
                    (c = in.read()) >= 0;) {
                sbJSON.append((char) c);
            }
            response = sbJSON.toString();
            logger.info(response + " isdn " + isdn);
            responseWallet = gson.fromJson(response, ResponseWallet.class);
            if (dbConnectKit != null) {
                dbConnectKit.insertEwalletConnectKitLog(1L, requestId, String.valueOf(totalAmount), voucherCode, staffCode, request,
                        response, responseWallet.getResponseCode(), responseWallet.getResponseMessage(), responseWallet.getRequestId(), isdn);
            } else if (dbChangeSim4G != null) {
                dbChangeSim4G.insertEwalletConnectKitLog(1L, requestId, String.valueOf(totalAmount), voucherCode, staffCode, request,
                        response, responseWallet.getResponseCode(), responseWallet.getResponseMessage(), responseWallet.getRequestId(), isdn);
            } else if (dbKitBatch != null) {
                dbKitBatch.insertEwalletConnectKitLog(1L, requestId, String.valueOf(totalAmount), voucherCode, staffCode, request,
                        response, responseWallet.getResponseCode(), responseWallet.getResponseMessage(), responseWallet.getRequestId(), isdn);
            } else if (dbChangeStudentProduct != null) {
                dbChangeStudentProduct.insertEwalletConnectKitLog(1L, requestId, String.valueOf(totalAmount), voucherCode, staffCode, request,
                        response, responseWallet.getResponseCode(), responseWallet.getResponseMessage(), responseWallet.getRequestId(), isdn);
            }

            return responseWallet;

        } catch (Exception e) {
            if (dbConnectKit != null) {
                dbConnectKit.insertEwalletConnectKitLog(1L, requestId, String.valueOf(totalAmount), voucherCode, staffCode, request,
                        response, "99", "Have exception when payment voucher: " + e.getMessage(), "", isdn);
            } else if (dbChangeSim4G != null) {
                dbChangeSim4G.insertEwalletConnectKitLog(1L, requestId, String.valueOf(totalAmount), voucherCode, staffCode, request,
                        response, "99", "Have exception when payment voucher: " + e.getMessage(), "", isdn);
            } else if (dbKitBatch != null) {
                dbKitBatch.insertEwalletConnectKitLog(1L, requestId, String.valueOf(totalAmount), voucherCode, staffCode, request,
                        response, "99", "Have exception when payment voucher: " + e.getMessage(), "", isdn);
            } else if (dbChangeStudentProduct != null) {
                dbChangeStudentProduct.insertEwalletConnectKitLog(1L, requestId, String.valueOf(totalAmount), voucherCode, staffCode, request,
                        response, "99", "Have exception when payment voucher: " + e.getMessage(), "", isdn);
            }

            return responseWallet;
        }
    }

    public static ResponseWallet revertTransaction(DbProcessorAbstract db, Bonus bn, Logger logger, String orgRequestId, String isdn)
            throws Exception {
        if (orgRequestId == null || orgRequestId.trim().length() <= 0) {
            return new ResponseWallet();
        }
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        String partnerCode = "E-MOLA";
        String request = "";
        String response = "";
        logger.info("Start call callEwalletRollback staff " + bn.getUserName() + " isdn " + isdn);
        String strUrl = ResourceBundle.getBundle("configPayBonus").getString("DebitEwalletUrl");
        String requestDate = sdf.format(new Date());
        String requestId = "0" + orgRequestId;
        ResponseWallet responseWallet = null;
        DbConnectKitProcessor dbConnectKit = null;
        DbKitBatchConnectProcessor dbKitBatch = null;
        if (db.getClass() == DbConnectKitProcessor.class) {
            dbConnectKit = (DbConnectKitProcessor) db;
        } else if (db.getClass() == DbKitBatchConnectProcessor.class) {
            dbKitBatch = (DbKitBatchConnectProcessor) db;
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
            if (dbConnectKit != null) {
                dbConnectKit.insertEwalletConnectKitLog(2L, requestId, "", "", bn.getUserName(), request, response,
                        responseWallet.getResponseCode(), responseWallet.getResponseMessage(), responseWallet.getRequestId(), isdn);
            } else if (dbKitBatch != null) {
                dbKitBatch.insertEwalletConnectKitLog(2L, requestId, "", "", bn.getUserName(), request, response,
                        responseWallet.getResponseCode(), responseWallet.getResponseMessage(), responseWallet.getRequestId(), isdn);
            }

            return responseWallet;

        } catch (Exception e) {
            if (dbConnectKit != null) {
                dbConnectKit.insertEwalletConnectKitLog(2L, requestId, "", "", bn.getUserName(), request, response,
                        "98", "Have exception when revert transaction: " + e.getMessage(), "", isdn);
            } else if (dbKitBatch != null) {
                dbKitBatch.insertEwalletConnectKitLog(2L, requestId, "", "", bn.getUserName(), request, response,
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
        DbConnectKitProcessor dbConnectKit = null;
        DbKitBatchConnectProcessor dbKitBatch = null;
        if (db.getClass() == DbConnectKitProcessor.class) {
            dbConnectKit = (DbConnectKitProcessor) db;
        } else if (db.getClass() == DbKitBatchConnectProcessor.class) {
            dbKitBatch = (DbKitBatchConnectProcessor) db;
        }
        try {
            String strDate = sdfDate.format(now).replace("-", "").replace(".", "").replace(" ", "");
            String requestDate = strDate;
            int transactionType = 1;
            String partnerCode = ResourceBundle.getBundle("configPayBonus").getString("debitEnte_emola_partnerCode");
            String urlString = ResourceBundle.getBundle("configPayBonus").getString("debitEnte_emola_url");
            String key = ResourceBundle.getBundle("configPayBonus").getString("debitEnte_emola_key");
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
            if (dbConnectKit != null) {
                dbConnectKit.insertEwalletConnectKitLog(1L, requestId, String.valueOf(totalAmount), enterpriseAccount, staffCode, request,
                        response, responseWallet.getResponseCode(), responseWallet.getResponseMessage(), responseWallet.getRequestId(), isdn);
            } else if (dbKitBatch != null) {
                dbKitBatch.insertEwalletConnectKitLog(1L, requestId, String.valueOf(totalAmount), enterpriseAccount, staffCode, request,
                        response, responseWallet.getResponseCode(), responseWallet.getResponseMessage(), responseWallet.getRequestId(), isdn);
            }


        } catch (Exception e) {
            if (dbConnectKit != null) {
                dbConnectKit.insertEwalletConnectKitLog(1L, requestId, String.valueOf(totalAmount), enterpriseAccount, staffCode, request,
                        response, "99", "Have exception when payment voucher: " + e.getMessage(), "", isdn);
            } else if (dbKitBatch != null) {
                dbKitBatch.insertEwalletConnectKitLog(1L, requestId, String.valueOf(totalAmount), enterpriseAccount, staffCode, request,
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
            Double totalAmount, Logger logger, String isdn, String staffCode, String key, String partnerCode)
            throws Exception {
        long timeSt = System.currentTimeMillis();
        SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH.mm.ss");
        Date now = new Date();
        ResponseWallet responseWallet = null;
        Reader in = null;
        String request = "";
        String response = "";
        DbConnectKitProcessor dbConnectKit = null;
        DbKitBatchConnectProcessor dbKitBatch = null;
        if (db.getClass() == DbConnectKitProcessor.class) {
            dbConnectKit = (DbConnectKitProcessor) db;
        } else if (db.getClass() == DbKitBatchConnectProcessor.class) {
            dbKitBatch = (DbKitBatchConnectProcessor) db;
        }
        try {
            String strDate = sdfDate.format(now).replace("-", "").replace(".", "").replace(" ", "");
            String requestDate = strDate;
            int transactionType = 1;
//            String partnerCode = ResourceBundle.getBundle("configPayBonus").getString("debitEnte_emola_partnerCode");
            String urlString = ResourceBundle.getBundle("configPayBonus").getString("debitEnte_emola_url");
//            String key = ResourceBundle.getBundle("configPayBonus").getString("debitEnte_emola_key");
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
            if (dbConnectKit != null) {
                dbConnectKit.insertEwalletConnectKitLog(1L, requestId, String.valueOf(totalAmount), enterpriseAccount, staffCode, request,
                        response, responseWallet.getResponseCode(), responseWallet.getResponseMessage(), responseWallet.getRequestId(), isdn);
            } else if (dbKitBatch != null) {
                dbKitBatch.insertEwalletConnectKitLog(1L, requestId, String.valueOf(totalAmount), enterpriseAccount, staffCode, request,
                        response, responseWallet.getResponseCode(), responseWallet.getResponseMessage(), responseWallet.getRequestId(), isdn);
            }


        } catch (Exception e) {
            if (dbConnectKit != null) {
                dbConnectKit.insertEwalletConnectKitLog(1L, requestId, String.valueOf(totalAmount), enterpriseAccount, staffCode, request,
                        response, "99", "Have exception when payment voucher: " + e.getMessage(), "", isdn);
            } else if (dbKitBatch != null) {
                dbKitBatch.insertEwalletConnectKitLog(1L, requestId, String.valueOf(totalAmount), enterpriseAccount, staffCode, request,
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

    public static String functionChannelWallet(DbCreateChannelProcessor db, Logger logger, String mobile, String customerName, String gender, String doB, String idType,
            String idNo, String address, String progressType, String channelType, String ewalletId, String parentId, String idIssuePlace,
            String idIssueDate, String id, String shopId) throws Exception {

        String request = "";
        Long ewallet_Id = 0L;
        Date birthday = new Date();
        Long parent_id = 0L;
        Date issueDate = new Date();
        try {
            String content = null;
            if (ewalletId != null && !ewalletId.equals("")) {
                ewallet_Id = Long.valueOf(ewalletId);
            }
            if (parentId != null && !parentId.equals("")) {
                parent_id = Long.valueOf(parentId);
            }
            if (doB != null && !doB.equals("")) {
                birthday = string2Date(doB);
            }
            if (idIssueDate != null && !idIssueDate.equals("")) {
                issueDate = string2Date(idIssueDate);
            }
            ResourceBundle configList = ResourceBundle.getBundle("configPayBonus");
            String BASE_URL = configList.getString("CreateWalletForSubscriber_wsdlUrl");
            String API = configList.getString("API");
            String userNameString = configList.getString("UserName");
            String pasString = configList.getString("PassWord");
            String funString = "SyncChannel";
            ChannelWalletBean channelWallet = new ChannelWalletBean();
            channelWallet.setMobile(mobile);
            channelWallet.setCustomerName(customerName);
            channelWallet.setGender(gender);
            channelWallet.setDoB(doB);
            channelWallet.setIdType(idType);
            channelWallet.setIdNo(idNo);
            channelWallet.setAddress(address);
            channelWallet.setProgressType(progressType);
            channelWallet.setChannelType(channelType);
            channelWallet.setEwalletId(ewalletId);
            channelWallet.setParentId(parentId);
            channelWallet.setIdIssuePlace(idIssuePlace);
            channelWallet.setIdIssueDate(idIssueDate);
            channelWallet.setShopId(shopId);

            Gson gson = new Gson();
            request = gson.toJson(channelWallet, ChannelWalletBean.class);
            logger.info("Request Wallet:" + request + " mobile " + mobile);
            // set the connection timeout value to 60 seconds (60000 milliseconds)
            final HttpParams httpParams = new BasicHttpParams();
            HttpConnectionParams.setConnectionTimeout(httpParams, 60000);
            HttpConnectionParams.setSoTimeout(httpParams, 60000);
            HttpClient client = new DefaultHttpClient(httpParams);
            HttpPost post = new HttpPost(BASE_URL + API);
            List nameValuePairs = new ArrayList();
            TextSecurity sec = TextSecurity.getInstance();
            String str = pasString + "|" + id;
            String passEncrypt = sec.Encrypt(str);
            nameValuePairs.add(new BasicNameValuePair("Username", userNameString));
            nameValuePairs.add(new BasicNameValuePair("Password", passEncrypt));
            nameValuePairs.add(new BasicNameValuePair("FunctionName", funString));
            nameValuePairs.add(new BasicNameValuePair("RequestId", id));
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
            logger.info("Content Wallet:" + content + " mobile " + mobile);
            return content;
        } catch (Exception ex) {
            try {
                logger.error("Had exception at functionChannelWallet, now try to insert log " + ex.toString() + " mobile " + mobile);
                db.insertLogCallWsWallet(mobile, ewallet_Id, progressType, 0L, 0L, request + "---Exception Job---" + ex,
                        customerName, birthday, idNo, channelType, parent_id, idIssuePlace, issueDate);
            } catch (Exception e) {
                logger.error("Can not insert log at functionChannelWallet " + e.toString() + " mobile " + mobile);
            }
            return "ERROR";
        }
    }

    public static String createCustomerEmolaAccount(DbCreateChannelProcessor db, Logger log, long custId, String userName, String mobile, String customerName, String gender,
            String doB, String idType, String idNo, String address, String progressType, String id) throws Exception {
        String request = "";
        try {
            String content = null;
            ResourceBundle configList = ResourceBundle.getBundle("configPayBonus");
            String BASE_URL = configList.getString("CreateWalletForCust_wsdlUrl");
            String API = configList.getString("API");
            String userNameString = configList.getString("UserName");
            String pasString = configList.getString("PassWord");
            String funString = "CustomerSync";
            Subscriber sub = new Subscriber();
            sub.setCustId("" + custId);
            sub.setUserName(userName);
            sub.setMobile(mobile);
            sub.setCustomerName(customerName);
            sub.setGender(gender);
            sub.setDoB(doB);
            sub.setIdType(idType);
            sub.setIdNo(idNo);
            sub.setAddress(address);
            sub.setProgressType(progressType);

            Gson gson = new Gson();
            request = gson.toJson(sub, Subscriber.class);
            log.info("Request Wallet:" + request + " mobile " + mobile);
            //HttpClient client = new DefaultHttpClient();
            // set the connection timeout value to 60 seconds (60000 milliseconds)
            final HttpParams httpParams = new BasicHttpParams();
            HttpConnectionParams.setConnectionTimeout(httpParams, 60000);
            HttpConnectionParams.setSoTimeout(httpParams, 60000);
            HttpClient client = new DefaultHttpClient(httpParams);
            HttpPost post = new HttpPost(BASE_URL + API);
            List nameValuePairs = new ArrayList();
            TextSecurity sec = TextSecurity.getInstance();
            String str = pasString + "|" + id;
            String passEncrypt = sec.Encrypt(str);
            //System.out.println("passEncrypt:" + passEncrypt);
            nameValuePairs.add(new BasicNameValuePair("Username", userNameString));
            nameValuePairs.add(new BasicNameValuePair("Password", passEncrypt));
            nameValuePairs.add(new BasicNameValuePair("RequestId", id));
            nameValuePairs.add(new BasicNameValuePair("FunctionName", funString));

            nameValuePairs.add(new BasicNameValuePair("FunctionParams", request));
            post.setEntity(new UrlEncodedFormEntity(nameValuePairs));
            HttpResponse response = client.execute(post);
            log.info("NameValuePairs:" + nameValuePairs + " mobile " + mobile);
            log.info("Post response:" + post + " mobile " + mobile);
            BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
            StringBuilder sb = new StringBuilder();
            String output;
            while ((output = rd.readLine()) != null) {
                sb.append(output);
            }
            content = sb.toString();
            return content;
        } catch (Exception ex) {
            try {
                log.error("Had exception ad createCustomerEmolaAccount, now try to insert log " + ex.toString() + " mobile " + mobile);
                db.insertLogCallWsWallet(mobile, custId, progressType, 0L, 0L, request + "---Exception Job---" + ex,
                        customerName, new Date(), idNo, "", 0l, "", new Date());
            } catch (Exception e) {
                log.error("Can not insert log at createCustomerEmolaAccount " + e.toString() + " mobile " + mobile);
            }
            return "ERROR";
        }
    }

    private static Date string2Date(String value) throws Exception {
        SimpleDateFormat dateTime = new SimpleDateFormat("ddMMyyyy");
        return dateTime.parse(value);
    }

    //Check ewallet
    public static String checkToWallet(String custId, String userName, String mobile, String customerName, String gender,
            String doB, String idType, String idNo, String address, String progressType, String id, Logger log) throws Exception {
        String request = "";
        try {
            ResourceBundle resource = ResourceBundle.getBundle("configPayBonus");
            String content = null;
            String BASE_URL = resource.getString("CreateWalletForCust_wsdlUrl");
            String API = resource.getString("API");
            String userNameString = resource.getString("UserName");
            String pasString = resource.getString("PassWord");
            String funString = "CustomerSync";
            SubscriberWallet sub = new SubscriberWallet();
            sub.setCustId(custId);
            sub.setUserName(userName);
            sub.setMobile(mobile);
            sub.setCustomerName(customerName);
            sub.setGender(gender);
            sub.setDoB(doB);
            sub.setIdType(idType);
            sub.setIdNo(idNo);
            sub.setAddress(address);
            sub.setProgressType(progressType);
            Gson gson = new Gson();
            request = gson.toJson(sub, SubscriberWallet.class);
            // set the connection timeout value to 60 seconds (60000 milliseconds)
            final HttpParams httpParams = new BasicHttpParams();
            HttpConnectionParams.setConnectionTimeout(httpParams, 60000);
            HttpConnectionParams.setSoTimeout(httpParams, 60000);
            HttpClient client = new DefaultHttpClient(httpParams);
            HttpPost post = new HttpPost(BASE_URL + API);
            List nameValuePairs = new ArrayList();
            TextSecurity sec = TextSecurity.getInstance();
            String str = pasString + "|" + id;
            System.out.println("ID:" + id);
            String passEncrypt = sec.Encrypt(str);
            nameValuePairs.add(new BasicNameValuePair("Username", userNameString));
            nameValuePairs.add(new BasicNameValuePair("Password", passEncrypt));
            nameValuePairs.add(new BasicNameValuePair("FunctionName", funString));
            nameValuePairs.add(new BasicNameValuePair("RequestId", id));
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
            return content;
        } catch (Exception ex) {
            log.error("checkToWallet isdn " + mobile + " custId " + custId + " " + ex.toString());
            return "Error:";
        }
    }

    /**
     * Checking a number is a enterprise ewallet
     *
     * @param mobile
     * @return
     * @throws Exception
     */
    public static QueryInforResponse queryEnterpriseAccountInfo(String mobile) throws Exception {
        if (mobile.startsWith("258")) {
            mobile = mobile.substring(3);
        }
        QueryInforResponse queryInforResponse = null;
        String request = "";
        String response = "";
        try {
            String urlGetVoucher = ResourceBundle.getBundle("configPayBonus").getString("urlQueryEnterpriseAccountInfo");
            urlGetVoucher = urlGetVoucher.replace("{Mobile}", mobile);
            URL url = new URL(urlGetVoucher);
            Gson gson = new Gson();
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Authorization", "Basic TW92aXRlbDp6b3Awc3QhQCM=");
            conn.setRequestProperty("Accept", "application/json");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setConnectTimeout(60000);
            conn.setReadTimeout(60000);
            conn.setDoOutput(true);
            conn.getOutputStream().write(request.getBytes());
            logger.info(request);
            Reader in = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));
            StringBuilder sbJSON = new StringBuilder();
            for (int c;
                    (c = in.read()) >= 0;) {
                sbJSON.append((char) c);
            }
            response = sbJSON.toString();
            logger.info(response);
            queryInforResponse = gson.fromJson(response, QueryInforResponse.class);
            logger.info("Query infor fro " + mobile + " result " + queryInforResponse.toString());
            return queryInforResponse;
        } catch (Exception e) {
            logger.error("Have exception when check enterprise account. Error  " + e.toString());
            e.printStackTrace();
            return null;
        }
    }
}
