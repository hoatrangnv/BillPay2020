/*
 * Copyright 2012 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.emola.process;

import com.viettel.paybonus.database.*;
import com.viettel.cluster.agent.integration.Record;
import com.viettel.threadfw.manager.AppManager;
import com.viettel.paybonus.obj.*;
import com.viettel.threadfw.database.DbProcessorAbstract;
import static com.viettel.threadfw.database.DbProcessorAbstract.QUERY_TIMEOUT;
import com.viettel.vas.util.PoolStore;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.apache.log4j.Logger;
import java.sql.Connection;
import com.viettel.vas.util.ConnectionPoolManager;
import com.viettel.vas.util.obj.Param;
import com.viettel.vas.util.obj.ParamList;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

/**
 *
 * Thong tin phien ban
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class EmolaTransactionDbProcessor extends DbProcessorAbstract {

    private String loggerLabel = DbOpenFlagRegisterInfo.class.getSimpleName() + ": ";
    private PoolStore poolStore;
    private String dbNameCofig;

    public EmolaTransactionDbProcessor() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
        dbNameCofig = "dbEmolaMsSql";
        poolStore = new PoolStore(dbNameCofig, logger);
    }

    public EmolaTransactionDbProcessor(String sessionName, Logger logger) throws SQLException, Exception {
        this.logger = logger;
        dbNameCofig = sessionName;
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
        EmolaTransactionInfo record = new EmolaTransactionInfo();
        long timeSt = System.currentTimeMillis();
        try {
            record.setId(rs.getLong("RequestId"));
            record.setTransCode(rs.getString("TransCode"));
            record.setRequestId(rs.getLong("RequestId"));
            record.setCusId(rs.getLong("CustomerID"));
            record.setAmount(rs.getDouble("TransAmount"));
            record.setActivedTime(rs.getTimestamp("ActiveDate"));
            record.setTransactionTime(rs.getTimestamp("CreatedDate"));
            record.setServiceId(rs.getInt("ServiceId"));
            record.setMobile(rs.getString("Mobile"));
            record.setCusName(rs.getString("CustomerName"));
            record.setResultCode("0");
            record.setDescription("Processing");
            record.setAgentWallet(rs.getString("AgentWallet"));
            record.setAgentName(rs.getString("AgentName"));
            record.setAgentCode(rs.getString("AgentCode"));
            record.setAgentChannelCode(rs.getString("ChannelCode"));
        } catch (Exception ex) {
            logger.error("ERROR parse MoRecord");
            logger.error(AppManager.logException(timeSt, ex));
        }
        return record;
    }

    @Override
    public int[] deleteQueue(List<Record> listRecords) {
        return new int[0];
//        long timeStart = System.currentTimeMillis();
//        PreparedStatement ps = null;
//        Connection connection = null;
//        String batchId = "";
//        try {
//            dbNameCofig = ResourceBundle.getBundle("configPayBonus").getString("dbBocNameConfig");
//            connection = ConnectionPoolManager.getConnection(dbNameCofig);
//            ps = connection.prepareStatement(sqlDeleteMo);
//            for (Record rc : listRecords) {
//                LogRegisterInfo sd = (LogRegisterInfo) rc;
//                batchId = sd.getBatchId();
//                ps.setLong(1, sd.getLogRegisterInfoId());
//                ps.addBatch();
//            }
//            return ps.executeBatch();
//        } catch (Exception ex) {
//            logger.error("ERROR deleteQueue LOG_REGISTER_INFO batchid " + batchId, ex);
//            logger.error(AppManager.logException(timeStart, ex));
//            return null;
//        } finally {
//            closeStatement(ps);
//            closeConnection(connection);
//            logTimeDb("Time to deleteQueue LOG_REGISTER_INFO, batchid " + batchId, timeStart);
//        }
    }

    @Override
    public int[] insertQueueHis(List<Record> listRecords) {
        List<ParamList> listParam = new ArrayList<ParamList>();
        String batchId = "";
        long timeSt = System.currentTimeMillis();
        try {
            for (Record rc : listRecords) {
                EmolaTransactionInfo sd = (EmolaTransactionInfo) rc;
                batchId = sd.getBatchId();
                ParamList paramList = new ParamList();
                paramList.add(new Param("emola_transaction_id", sd.getRequestId(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param("customer_id", sd.getCusId(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param("trans_code", sd.getTransCode(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("request_id", sd.getRequestId(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param("amount", sd.getAmount(), Param.DataType.DOUBLE, Param.IN));
                paramList.add(new Param("service_id", sd.getServiceId(), Param.DataType.INT, Param.IN));
                paramList.add(new Param("cus_name", sd.getCusName(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("mobile", sd.getMobile(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("actived_time", sd.getActivedTime(), Param.DataType.TIMESTAMP, Param.IN));                
                paramList.add(new Param("transaction_time", sd.getTransactionTime(), Param.DataType.TIMESTAMP, Param.IN));  
                paramList.add(new Param("agent_wallet", sd.getAgentWallet(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("agent_name", sd.getAgentName(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("agent_code", sd.getAgentCode(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("agent_channel_code", sd.getAgentChannelCode(), Param.DataType.STRING, Param.IN));              
                paramList.add(new Param("err_code", sd.getResultCode(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("description", sd.getDescription(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("cluster_name", sd.getClusterName(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("note_name", sd.getNodeName(), Param.DataType.STRING, Param.IN));
                listParam.add(paramList);
            }
            poolStore = new PoolStore("dbEmolaPromotion", logger);
            int[] res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "emola_transaction_his");
            logTimeDb("Time to insertQueueHis emola_transaction_his, batchid " + batchId + " total result: " + res.length, timeSt);
            return res;
        } catch (Exception ex) {
            logger.error("ERROR retry insertQueueHis emola_transaction_his batchid " + batchId, ex);
            logger.error(AppManager.logException(timeSt, ex));
            return null;
        }
    }

    @Override
    public int[] insertQueueOutput(List<Record> listRecords) {
        return new int[0];
//        List<ParamList> listParam = new ArrayList<ParamList>();
//        String batchId = "";
//        int[] res = new int[0];
//        long timeSt = System.currentTimeMillis();
//        try {
//            String countryCode = ResourceBundle.getBundle("configPayBonus").getString("country_code");
//            String sms_code = ResourceBundle.getBundle("configPayBonus").getString("sms_code");
//            for (Record rc : listRecords) {
//                LogRegisterInfo sd = (LogRegisterInfo) rc;
//                if (sd.getMessage() == null || sd.getIsdn() == null || sd.getMessage().trim().length() <= 0
//                        || sd.getIsdn().trim().length() <= 0) {
//                    continue;
//                }
//                batchId = sd.getBatchId();
//                ParamList paramList = new ParamList();
//                paramList.add(new Param("MO_HIS_ID", sd.getLogRegisterInfoId(), Param.DataType.LONG, Param.IN));
//                paramList.add(new Param("MT_ID", "MT_SEQ.NEXTVAL", Param.DataType.CONST, Param.IN));
//                paramList.add(new Param("MSISDN", countryCode + sd.getIsdn(), Param.DataType.STRING, Param.IN));
//                paramList.add(new Param("MESSAGE", sd.getMessage(), Param.DataType.STRING, Param.IN));
//                paramList.add(new Param("CHANNEL", sms_code, Param.DataType.CONST, Param.IN));
//                listParam.add(paramList);
//            }
//            if (listParam.size() > 0) {
//                res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "MT");
//                logTimeDb("Time to insertQueueOutput MT, batchid " + batchId + " total result: " + res.length, timeSt);
//            } else {
//                logTimeDb("List Record to insert Queue Output is empty, batchid " + batchId, timeSt);
//            }
//            return res;
//        } catch (Exception ex) {
//            logger.error("ERROR insertQueueOutput batchid " + batchId, ex);
//            try {
//                res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "MT");
//                logTimeDb("Time to retry insertQueueOutput MT, batchid " + batchId + " total result: " + res.length, timeSt);
//                return res;
//            } catch (Exception ex1) {
//                logger.error("ERROR retry insertQueueOutput MT, batchid " + batchId, ex1);
//                logger.error(AppManager.logException(timeSt, ex));
//                return null;
//            }
//        }
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
                sb.append(": ").append(sd);
            }
            logger.warn("Dispatcher not get reponse from agent, so processTimeoutRecord ID " + sb.toString());
        } catch (Exception ex) {
            logger.error("ERROR processTimeoutRecord ID " + sb.toString() + " " + ex.toString());
        }
    }

    public int[] deleteQueueTimeout(List<String> listId) {
        return new int[0];
    }

    @Override
    public void updateSqlMoParam(List<Record> lrc) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public int insertEmolaTransaction(EmolaTransactionInfo obj) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder brBuilder = new StringBuilder();
        String sql = "";
        int result = 0;
        int index = 1;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbEmolaPromotion");
            sql = "INSERT INTO emola_transaction_info (EMOLA_TRANSACTION_ID,CUSTOMER_ID,TRANS_CODE,REQUEST_ID,AMOUNT,ACTIVED_TIME,TRANSACTION_TIME,SERVICE_ID,"
                    + "MOBILE,CUS_NAME,LOG_TIME,BTS_REG,AGENT_WALLET,AGENT_NAME,AGENT_CODE,AGENT_CHANNEL_CODE) \n"
                    + "VALUES(emola_transaction_info_seq.nextval,?,?,?,?,?,?,?,?,?,sysdate,?,?,?,?,?)";
            ps = connection.prepareStatement(sql);
            //ps.setLong(index++, obj.getId());
            ps.setLong(index++, obj.getCusId());
            ps.setString(index++, obj.getTransCode());
            ps.setLong(index++, obj.getRequestId());
            ps.setDouble(index++, obj.getAmount());
            ps.setTimestamp(index++, new Timestamp(obj.getActivedTime().getTime()));
            ps.setTimestamp(index++, new Timestamp(obj.getTransactionTime().getTime()));
            ps.setInt(index++, obj.getServiceId());
            ps.setString(index++, obj.getMobile());
            ps.setString(index++, obj.getCusName());
            ps.setString(index++, obj.getBtsReg());
            ps.setString(index++, obj.getAgentWallet());
            ps.setString(index++, obj.getAgentName());
            ps.setString(index++, obj.getAgentCode());
            ps.setString(index++, obj.getAgentChannelCode());
            result = ps.executeUpdate();
            logger.info("End insertEmolaTransaction transCode " + obj.getTransCode()+ " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            brBuilder.setLength(0);
            brBuilder.append(loggerLabel).append(new Date()).
                    append("\nERROR insertEmolaTransaction: ").
                    append(sql).append("\n")
                    .append(" transCode ")
                    .append(obj.getTransCode())
                    .append(" result ")
                    .append(result);
            logger.error(brBuilder + ex.toString());
            logger.error(AppManager.logException(startTime, ex));
            result = -1;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }
public String getChannelInfo(String mobile) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        String targetChannel = Config.getConfig(Config.targetChannelCode, logger);
        String staffCode = "";
        long startTime = System.currentTimeMillis();        
        try {
            connection = getConnection("dbSm");
            sql = "select * from sm.staff where isdn_wallet = ? and status = 1";
//            sql = "select * from sm.staff where isdn_wallet = ? and status = 1 and channel_type_id in(" + targetChannel + ")";
            ps = connection.prepareStatement(sql);
            ps.setString(1, mobile);
            rs = ps.executeQuery();
            while (rs.next()) {
                staffCode = rs.getString("staff_code");
                if (staffCode != null && !staffCode.isEmpty()) {
                    break;
                }
            }
            logger.info("End getChannelInfo mobile " + mobile + "\n target channel" + targetChannel + " result " + staffCode + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR getChannelInfo: ").
                    append(sql).append("\n")
                    .append(" mobile ")
                    .append(mobile)
                    .append(" result ")
                    .append(staffCode);
            logger.error(br + ex.toString());
            logger.error(AppManager.logException(startTime, ex));
        } finally {
            closeStatement(ps);
            closeResultSet(rs);
            closeConnection(connection);
            return staffCode;
        }
    }
        
    public String getCell(String staffCode, String cellId, String lacId) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement psGetCell = null;
        String cell = "";
        String sqlGetCell = "select cell from cell where ci = TO_NUMBER (util.convert_cell_id (?)) "
                + " and lac = TO_NUMBER (util.convert_lac (?)) and is_delete = 0 and ROWNUM < 2";
        try {
            connection = ConnectionPoolManager.getConnection("dbEmolaPromotion");
            psGetCell = connection.prepareStatement(sqlGetCell);
            if (QUERY_TIMEOUT > 0) {
                psGetCell.setQueryTimeout(QUERY_TIMEOUT);
            }
            psGetCell.setString(1, cellId);
            psGetCell.setString(2, lacId);
            rs = psGetCell.executeQuery();
            while (rs.next()) {
                String cellValue = rs.getString("cell");
                if (cellValue != null && cellValue.length() > 0) {
                    cell = cellValue;
                    break;
                }
            }
            logTimeDb("Time to getCell staffcode " + staffCode + " cellId " + cellId
                    + " lacId" + lacId + " result: " + cell, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getCell staffcode " + staffCode);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs);
            closeStatement(psGetCell);
            closeConnection(connection);
        }
        return cell;
    }
    
    public String getBts(String staffCode, String cellId, String lacId) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement psGetBts = null;
        String bts = "";
        String sqlGetBts = "select bts_code from bts where bts_id in (select bts_id from cell where ci = TO_NUMBER (util.convert_cell_id (?)) "
                + " and lac = TO_NUMBER (util.convert_lac (?)) and is_delete = 0 and ROWNUM < 2)";
        try {
            connection = ConnectionPoolManager.getConnection("dbEmolaPromotion");
            psGetBts = connection.prepareStatement(sqlGetBts);
            if (QUERY_TIMEOUT > 0) {
                psGetBts.setQueryTimeout(QUERY_TIMEOUT);
            }
            psGetBts.setString(1, cellId);
            psGetBts.setString(2, lacId);
            rs = psGetBts.executeQuery();
            while (rs.next()) {
                String btsValue = rs.getString("bts_code");
                if (btsValue != null && btsValue.length() > 0) {
                    bts = btsValue;
                    break;
                }
            }
            logTimeDb("Time to getCell staffcode " + staffCode + " cellId " + cellId
                    + " lacId" + lacId + " result: " + bts, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getCell staffcode " + staffCode);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs);
            closeStatement(psGetBts);
            closeConnection(connection);
        }
        return bts;
    }

    public HashMap<String, Config> getEmolaConfig() {
        long timeSt = System.currentTimeMillis();
        Connection connection = null;
        ResultSet rs1 = null;
        String sqlMo = "SELECT * FROM emola_promotion_config where status = 1";
        PreparedStatement psMo = null;
        HashMap<String, Config> map = new HashMap<String, Config>();
        try {
            connection = ConnectionPoolManager.getConnection("dbEmolaPromotion");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                Config conf = new Config();
                conf.setConfigId(rs1.getLong("CONFIG_ID"));
                conf.setValue(rs1.getString("VALUE"));
                conf.setCode(rs1.getString("CODE"));
                map.put(rs1.getString("CODE").toLowerCase(), conf);
                continue;
            }
        } catch (Exception ex) {
            logger.error("ERROR getEmolaConfig ");
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeStatement(psMo);
            closeConnection(connection);
        }
        return map;
    }
   
    public Long getSequence(String sequenceName, String dbName) {
            ResultSet rs1 = null;
            Connection connection = null;
            Long sequenceValue = null;
            String sqlMo = "select " + sequenceName + ".nextval as sequence from dual";
            PreparedStatement psMo = null;
            try {
                    connection = getConnection(dbName);
                    psMo = connection.prepareStatement(sqlMo);
                    rs1 = psMo.executeQuery();
                    while (rs1.next()) {
                            sequenceValue = rs1.getLong("sequence");
                    }
                    logger.info("End getSequence sequenceName " + sequenceName);
            } catch (Exception ex) {
                    logger.error("ERROR getSequence " + sequenceName);
            } finally {
                    closeResultSet(rs1);
                    closeStatement(psMo);
                    closeConnection(connection);
            }
            return sequenceValue;
    }
    
}
