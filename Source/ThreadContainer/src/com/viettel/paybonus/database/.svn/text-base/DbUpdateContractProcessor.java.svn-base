/*
 * Copyright 2012 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.paybonus.database;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.obj.RequestChannel;
import com.viettel.threadfw.manager.AppManager;
import com.viettel.threadfw.database.DbProcessorAbstract;
import static com.viettel.threadfw.database.DbProcessorAbstract.QUERY_TIMEOUT;
import com.viettel.vas.util.ConnectionPoolManager;
import com.viettel.vas.util.PoolStore;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
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
public class DbUpdateContractProcessor extends DbProcessorAbstract {

//    private Logger logger;
    private String loggerLabel = DbUpdateContractProcessor.class.getSimpleName() + ": ";
//    private StringBuffer br = new StringBuffer();
    private PoolStore poolStore;
    private String dbNameCofig;
    private String sqlDeleteMo = "update sm.request_update_contract set process_date = sysdate, status = ?, contract_status = ?, description = ? where id = ?";

    public DbUpdateContractProcessor() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
        dbNameCofig = "dbsm";
        poolStore = new PoolStore(dbNameCofig, logger);
    }

    public DbUpdateContractProcessor(String sessionName, Logger logger) throws SQLException, Exception {
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
        RequestChannel record = new RequestChannel();
        long timeSt = System.currentTimeMillis();
        try {
            record.setId(rs.getLong("id"));
            record.setRequestUserIsdn(rs.getString("channel_isdn"));
            record.setChannelName(rs.getString("channel_name"));
            record.setChannelIsdn(rs.getString("channel_isdn"));
            record.setCreateDate(rs.getTimestamp("create_date"));
            record.setStatus(rs.getLong("status"));
            record.setChannelWallet(rs.getString("channel_wallet"));
            record.setIsdnWallet(rs.getString("isdn_wallet"));
            record.setParentIdWallet(rs.getLong("parent_id_wallet"));
            record.setImgUrl(rs.getString("img_url"));
            record.setImgUrl1(rs.getString("img_url1"));
            record.setImgUrl2(rs.getString("img_url2"));
            record.setImgPath(rs.getString("img_path"));
            record.setDistrict(rs.getString("district"));
            record.setPrecint(rs.getString("precinct"));
            record.setImgUrl3(rs.getString("img_url3"));
            record.setbINumber(rs.getString("bi_number"));
            record.setAddress(rs.getString("address"));
            record.setNuitNumber(rs.getString("nuit_number"));
            record.setStaffCode(rs.getString("staff_code"));

            record.setDescription("Start Processing");
            record.setResultCode("0");
        } catch (Exception ex) {
            logger.error("ERROR parse MoRecord");
            logger.error(AppManager.logException(timeSt, ex));
        }
        return record;
    }

    @Override
    public int[] deleteQueue(List<Record> listRecords) {
        long timeStart = System.currentTimeMillis();
        PreparedStatement ps = null;
        Connection connection = null;
        String batchId = "";
        try {
            connection = ConnectionPoolManager.getConnection(dbNameCofig);
            ps = connection.prepareStatement(sqlDeleteMo);
            for (Record rc : listRecords) {
                RequestChannel sd = (RequestChannel) rc;
                batchId = sd.getBatchId();
                ps.setLong(1, sd.getStatus());
                ps.setLong(2, sd.getContractStatus());
                ps.setString(3, sd.getDescription());
                ps.setLong(4, sd.getId());

                ps.addBatch();
            }
            return ps.executeBatch();
        } catch (Exception ex) {
            logger.error("ERROR deleteQueue REQUEST_UPDATE_CONTRACT batchid " + batchId, ex);
            logger.error(AppManager.logException(timeStart, ex));
            return null;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to deleteQueue REQUEST_UPDATE_CONTRACT, batchid " + batchId, timeStart);
        }
    }

    public int updateContractPath(String path, String staffCode) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder brBuilder = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "update staff set image_url = ?, is_checked = null where status = 1 and staff_code = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, path);
            ps.setString(2, staffCode);
            result = ps.executeUpdate();
            logger.info("End updateContractPath staffCode " + staffCode + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            brBuilder.setLength(0);
            brBuilder.append(loggerLabel).append(new Date()).
                    append("\nERROR updateContractPath: ").
                    append(sql).append("\n")
                    .append(" staffCode ")
                    .append(staffCode)
                    .append(" result ")
                    .append(result);
            logger.error(brBuilder + ex.toString());
            result = -1;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
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
            connection = getConnection("dbapp2");
            sql = "INSERT INTO mt (mt_id,msisdn,message,mo_his_id,retry_num,receive_time,channel) "
                    + "VALUES(mt_SEQ.nextval,?,?,null,0,sysdate,?)";
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
    
    public Long getSequence(String sequenceName, String dbName) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        Long sequenceValue = null;
        String sqlMo = "select " + sequenceName + ".nextval as sequence from dual";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection(dbName);
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
        }
        return sequenceValue;
    }
}
