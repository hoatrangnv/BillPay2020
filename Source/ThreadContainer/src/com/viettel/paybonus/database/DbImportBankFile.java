/*
 * Copyright 2012 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.paybonus.database;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.threadfw.manager.AppManager;
import com.viettel.paybonus.obj.*;
import com.viettel.threadfw.database.DbProcessorAbstract;
import com.viettel.vas.util.PoolStore;
import com.viettel.vas.util.obj.Param;
import com.viettel.vas.util.obj.ParamList;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.ResourceBundle;
import org.apache.log4j.Logger;
import java.sql.Connection;
import com.viettel.vas.util.ConnectionPoolManager;
import java.sql.PreparedStatement;

/**
 *
 * Thong tin phien ban
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class DbImportBankFile extends DbProcessorAbstract {

    private String loggerLabel = DbImportBankFile.class.getSimpleName() + ": ";
    private PoolStore poolStore;
    private String dbNameCofig;

    public DbImportBankFile() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
        dbNameCofig = ResourceBundle.getBundle("configPayBonus").getString("dbNameConfig");
        ConnectionPoolManager.loadConfig("../etc/database.xml");
        poolStore = new PoolStore(dbNameCofig, logger);
    }

    public DbImportBankFile(String sessionName, Logger logger) throws SQLException, Exception {
        this.logger = logger;
        dbNameCofig = sessionName;
        ConnectionPoolManager.loadConfig("../etc/database.xml");
        poolStore = new PoolStore(sessionName, logger);
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
        Bonus record = new Bonus();
        return record;
    }

    @Override
    public int[] deleteQueue(List<Record> listRecords) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int[] insertQueueHis(List<Record> listRecords) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int[] insertQueueOutput(List<Record> listRecords) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int[] updateQueueInput(List<Record> listRecords) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void processTimeoutRecord(List<String> ids) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateSqlMoParam(List<Record> lrc) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public int sendSms(String msisdn, String message, String channel) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbapp2");
            sql = "INSERT INTO mt (mt_id,msisdn,message,mo_his_id,retry_num,receive_time,channel) "
                    + "VALUES(mt_SEQ.nextval,?,?,null,0,sysdate,?)";
            ps = connection.prepareStatement(sql);
            if (!msisdn.startsWith("258")) {
                msisdn = "258" + msisdn;
            }
            ps.setString(1, msisdn);
            ps.setString(2, message);
            ps.setString(3, channel);
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
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int insertBankFileInfo(long bankFileInfoId, String fileName, Date fileTime, String header, String trailer,
            String fileSequence, int totalRecord, long totalPay, long totalCom, String importDir, String backupDir,
            String unrateDir, String errCode, String desc) {
        List<ParamList> listParam = new ArrayList<ParamList>();
        int res = 0;
        long timeSt = System.currentTimeMillis();
        PreparedStatement ps = null;
        Connection connection = null;
        try {
            connection = ConnectionPoolManager.getConnection(dbNameCofig);
            String sqlInsert = "INSERT INTO bank_file_info(BANK_FILE_INFO_ID,FILE_NAME,FILE_TIME,HEADER,TRAILER,FILE_SEQUENCE,"
                    + "TOTAL_RECORD,TOTAL_PAY,TOTAL_COM,FILE_PATH_INPUT,FILE_PATH_BACKUP,FILE_PATH_UNRATE,ERR_CODE,DESCRIPTION) "
                    + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            ps = connection.prepareStatement(sqlInsert);
            ps.setLong(1, bankFileInfoId);
            ps.setString(2, fileName);
            ps.setDate(3, new java.sql.Date(fileTime.getTime()));
            ps.setString(4, header);
            ps.setString(5, trailer);
            ps.setString(6, fileSequence);
            ps.setInt(7, totalRecord);
            ps.setLong(8, totalPay);
            ps.setLong(9, totalCom);
            ps.setString(10, importDir);
            ps.setString(11, backupDir);
            ps.setString(12, unrateDir);
            ps.setString(13, errCode);
            ps.setString(14, desc);
            res = ps.executeUpdate();
        } catch (Exception ex) {
            logger.error("ERROR insertBankFileInfo  fileName " + fileName, ex);
            logger.error(AppManager.logException(timeSt, ex));
            return -1;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to insertBankFileInfo, fileName " + fileName + " result: " + res, timeSt);
            return res;
        }
    }

    public int insertBankFileDetail(List<BankFileDetail> listRecords, String fileName) {
        long timeStart = System.currentTimeMillis();
        PreparedStatement ps = null;
        Connection connection = null;
        int result = 0;
        try {
            connection = ConnectionPoolManager.getConnection(dbNameCofig);
            String sqlInsert = "INSERT INTO bank_file_detail (BANK_FILE_DETAIL_ID,BANK_FILE_INFO_ID,REFERENCE,"
                    + "VALUE_PAY,VALUE_COM,FILE_TIME,IMPORT_TIME,TRANS_ID,TERMINAL_ID,TERMINAL_LOCATION) \n"
                    + "VALUES(BANK_FILE_DETAIL_SEQ.nextval,?,?,?,?,?,sysdate,?,?,?)";
            ps = connection.prepareStatement(sqlInsert);
            for (BankFileDetail rc : listRecords) {
                ps.setLong(1, rc.getBankFileInfoId());
                ps.setString(2, rc.getReference());
                ps.setLong(3, rc.getValuePay());
                ps.setLong(4, rc.getValueCom());
                ps.setDate(5, new java.sql.Date(rc.getFileTime().getTime()));
                ps.setString(6, rc.getTransId());
                ps.setString(7, rc.getTerminalId());
                ps.setString(8, rc.getTerminalLocation());
                ps.addBatch();
            }
            ps.executeBatch();
        } catch (Exception ex) {
            logger.error("ERROR insertBankFileDetail fileName " + fileName, ex);
            logger.error(AppManager.logException(timeStart, ex));
            result = -1;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to insertBankFileDetail, fileName " + fileName, timeStart);
            return result;
        }
    }

    public Long getSequence(String sequenceName, String fileName) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        Long sequenceValue = null;
        String sqlMo = "select " + sequenceName + ".nextval as sequence from dual";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection(dbNameCofig);
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                sequenceValue = rs1.getLong("sequence");
            }
            logTimeDb("Time to getSequence: " + sequenceName, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getSequence " + sequenceName);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
            return sequenceValue;
        }
    }
}
