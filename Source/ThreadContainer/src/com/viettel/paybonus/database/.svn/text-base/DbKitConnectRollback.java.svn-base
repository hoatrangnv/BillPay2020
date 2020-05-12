/*
 * Copyright 2012 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.paybonus.database;

import com.google.gson.Gson;
import com.viettel.cluster.agent.integration.Record;
import com.viettel.threadfw.manager.AppManager;
import com.viettel.paybonus.obj.*;
import com.viettel.threadfw.database.DbProcessorAbstract;
import com.viettel.vas.util.PoolStore;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.ResourceBundle;
import org.apache.log4j.Logger;
import java.util.Date;

/**
 *
 * Thong tin phien ban
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class DbKitConnectRollback extends DbProcessorAbstract {

//    private Logger logger;
    private String loggerLabel = DbKitConnectRollback.class.getSimpleName() + ": ";
//    private StringBuffer br = new StringBuffer();
    private PoolStore poolStore;
    private String dbNameCofig;

    public DbKitConnectRollback() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
        dbNameCofig = "cm_pre";
        poolStore = new PoolStore(dbNameCofig, logger);
    }

    public DbKitConnectRollback(String sessionName, Logger logger) throws SQLException, Exception {
        this.logger = logger;
        dbNameCofig = sessionName;
        poolStore = new PoolStore(dbNameCofig, logger);
    }

    public void closeStatement(Statement st) {
        try {
            if (st != null) {
                st.close();
                st = null;
            }
        } catch (Exception ex) {
            st = null;
        }
    }

    public void logTimeDb(String strLog, long timeSt) {
        long timeEx = System.currentTimeMillis() - timeSt;

        if (timeEx >= AppManager.minTimeDb && AppManager.loggerDbMap != null) {
            br.setLength(0);
            br.append(loggerLabel).
                    append(AppManager.getTimeLevelDb(timeEx)).append(": ").
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

    @Override
    public Record parse(ResultSet rs) {
        EwalletDebitLog record = new EwalletDebitLog();
        long timeSt = System.currentTimeMillis();
        try {
            record.setId(rs.getLong("id"));
            record.setStaffCode(rs.getString("staff_code"));
            record.setRequestId(rs.getString("request_id"));
            record.setResultCode("0");
            record.setDescription("Processing");
        } catch (Exception ex) {
            logger.error("ERROR parse MoRecord");
            logger.error(AppManager.logException(timeSt, ex));
        }
        return record;
    }
    
    public ResponseWallet getStatusTransaction(String orgRequestId, String staffCode) throws Exception {
        long timeSt = System.currentTimeMillis();
        String request = "";
        String response = "";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        try {
            logger.info("Start getStatusTransaction staff " + staffCode
                    + " orgRequestId " + orgRequestId);
            String strUrl = ResourceBundle.getBundle("configPayBonus").getString("urlGetStatusTransaction");
            String partnerCode = ResourceBundle.getBundle("configPayBonus").getString("DebitEwalletPartnerCode");//base.key.api.emola
            String keyGetStatus = ResourceBundle.getBundle("configPayBonus").getString("keyGetStatus");//
            String requestDate = sdf.format(new Date());
            String requestId = partnerCode + requestDate;

            RevertTransaction paymentVoucher = new RevertTransaction();
            paymentVoucher.setRequestId(requestId);
            paymentVoucher.setOrgRequestId(orgRequestId);
            paymentVoucher.setPartnerCode(partnerCode);
            paymentVoucher.setRequestDate(requestDate);

            String tempSignature = keyGetStatus + requestDate + partnerCode + requestId + orgRequestId;
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
            logger.info("response: " + response);
            Gson responseGson = new Gson();
            ResponseWallet responseWallet = responseGson.fromJson(response, ResponseWallet.class);
            return responseWallet;
        } catch (Exception ex) {
            logger.error("Had exception call callEwalletRollback staff " + staffCode
                    + " orgRequestId " + orgRequestId);
            logger.error(AppManager.logException(timeSt, ex));
            return null;
        }
    }

    public ResponseWallet revertTransaction(String staffCode, Logger logger, String orgRequestId)
            throws Exception {
        if (orgRequestId == null || orgRequestId.trim().length() <= 0) {
            return new ResponseWallet();
        }
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        String partnerCode = "E-MOLA";
        String request = "";
        String response = "";
        logger.info("Start call callEwalletRollback staff " + staffCode);
        String strUrl = ResourceBundle.getBundle("configPayBonus").getString("DebitEwalletUrl");
        String requestDate = sdf.format(new Date());
        String requestId = "0" + orgRequestId;
        ResponseWallet responseWallet = null;

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
            logger.info("response: " + response);
            responseWallet = gson.fromJson(response, ResponseWallet.class);
            insertEwalletConnectKitLog(2L, requestId, "", "", staffCode, request, response,
                    responseWallet.getResponseCode(), responseWallet.getResponseMessage(), responseWallet.getRequestId());
            return responseWallet;

        } catch (Exception e) {
            insertEwalletConnectKitLog(2L, requestId, "", "", staffCode, request, response,
                    "98", "Have exception when revert transaction: " + e.getMessage(), "");
            return responseWallet;
        }


    }

    public int insertEwalletConnectKitLog(Long transactionType, String requestId, String amount,
            String voucherCode, String staffCode, String request, String response, String errCode, String description, String orgRequestId) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "insert into EWALLET_CONNECT_KIT_LOG values (EWALLET_CONNECT_KIT_LOG_SEQ.nextval,?,?,?,?,?,sysdate,?,?,?,?,?)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, transactionType);
            ps.setString(2, requestId);
            ps.setString(3, amount);
            ps.setString(4, voucherCode);
            ps.setString(5, staffCode);
            ps.setString(6, request);
            ps.setString(7, response);
            ps.setString(8, errCode);
            ps.setString(9, description);
            ps.setString(10, orgRequestId);
            result = ps.executeUpdate();
            logger.info("End insertEwalletConnectKitLog staffCode " + staffCode + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertEwalletConnectKitLog: ").
                    append(sql).append("\n")
                    .append(" staffCode ")
                    .append(staffCode)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int updateEwalletConnectKitLog(String staffCode, long eWalletDebitLogId, String orgRequetsId, String desc) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(dbNameCofig);
            sql = "update cm_pre.ewallet_connect_kit_log set response = ?, err_code = '98'"
                    + " where id = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, desc);
            ps.setLong(2, eWalletDebitLogId);
            result = ps.executeUpdate();
            logger.info("End updateEwalletConnectKitLog staffCode " + staffCode + " id " + eWalletDebitLogId
                    + " orgRequetsId " + orgRequetsId
                    + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR updateEwalletConnectKitLog: ").
                    append(sql).append("\n")
                    .append(" staffCode ")
                    .append(staffCode)
                    .append(" orgRequetsId ")
                    .append(orgRequetsId)
                    .append(" eWalletDebitLogId ")
                    .append(eWalletDebitLogId)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
            logger.error(AppManager.logException(startTime, ex));
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }
    
    
    public String getTelByStaffCode(String staffCode) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        String tel = null;
        String sqlMo = " select cellphone from vsa_v3.users where user_name = ? and status = 1";
        PreparedStatement psMo = null;
        try {
            connection = getConnection(dbNameCofig);
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setString(1, staffCode.toLowerCase());
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                tel = rs1.getString("cellphone");
            }
            if (tel == null) {
                tel = "";
                logger.info("tel is null - staff_code: " + staffCode);
            }
            logTimeDb("Time to getTelByStaffCode: " + staffCode, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getTelByStaffCode " + staffCode);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return tel;
    }

    @Override
    public int[] deleteQueue(List<Record> listRecords) {
        int[] res = new int[0];
        return res;
    }

    @Override
    public int[] insertQueueHis(List<Record> listRecords) {
        int[] res = new int[0];
        return res;
    }

    @Override
    public int[] insertQueueOutput(List<Record> listRecords) {
        int[] res = new int[0];
        return res;
    }

    @Override
    public int[] updateQueueInput(List<Record> listRecords) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void processTimeoutRecord(List<String> ids) {
        StringBuilder sb = new StringBuilder();
        try {
//            The first delete queue timeout
            deleteQueueTimeout(ids);
//            Save history
            for (String sd : ids) {
                sb.append(":" + sd);
            }
            logger.warn("Dispatcher not get reponse from agent, so processTimeoutRecord ID " + sb.toString());
        } catch (Exception ex) {
            logger.error("ERROR processTimeoutRecord ID " + sb.toString());
        }
    }

    @Override
    public void updateSqlMoParam(List<Record> lrc) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public int[] deleteQueueTimeout(List<String> listId) {
        int[] res = new int[0];
        return res;
    }

    public int sendSms(String msisdn, String message, String channel) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(dbNameCofig);
            sql = "INSERT INTO mt (mt_id,msisdn,message,mo_his_id,retry_num,receive_time,channel) "
                    + "VALUES(mt_SEQ.nextval,?,?,0,0,sysdate,?)";
            ps = connection.prepareStatement(sql);
            if (!msisdn.startsWith("258")) {
                msisdn = "258" + msisdn;
            }
            ps.setString(1, msisdn.trim());
            ps.setString(2, message.trim());
            ps.setString(3, channel.trim());
            result = ps.executeUpdate();
            logger.info("End sendSms isdn " + msisdn + " message " + message + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR sendSms: ").
                    append(sql).append("\n")
                    .append(" isdn ")
                    .append(msisdn)
                    .append(" message ")
                    .append(message)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
            logger.error(AppManager.logException(startTime, ex));
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }
}
