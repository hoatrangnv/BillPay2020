///*
// * Copyright 2012 Viettel Telecom. All rights reserved.
// * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
// */
//package com.viettel.paybonus.database;
//
//import com.viettel.cluster.agent.integration.Record;
//import com.viettel.paybonus.obj.EliteGroupInput;
//import com.viettel.threadfw.manager.AppManager;
//import com.viettel.threadfw.database.DbProcessorAbstract;
//import com.viettel.vas.util.ConnectionPoolManager;
//import com.viettel.vas.util.PoolStore;
//import com.viettel.vas.util.obj.Param;
//import com.viettel.vas.util.obj.ParamList;
//import java.sql.Connection;
//import java.sql.PreparedStatement;
//import java.sql.ResultSet;
//import java.sql.SQLException;
//import java.sql.Statement;
//import java.util.ArrayList;
//import java.util.List;
//import org.apache.log4j.Logger;
//
///**
// *
// * Thong tin phien ban
// *
// * @author HuyNQ13
// * @version 1.0
// * @since 24-03-2016
// */
//public class DbEliteGroupPay extends DbProcessorAbstract {
//
//    private String loggerLabel = DbEliteGroupPay.class.getSimpleName() + ": ";
//    private PoolStore poolStore;
//    private String dbNameCofig;
//    private String sqlDeleteMo = "delete elite_group_input where mo_id = ?";
//
//    public DbEliteGroupPay() throws SQLException, Exception {
//        this.logger = Logger.getLogger(loggerLabel);
//        dbNameCofig = "dbElite";
//        poolStore = new PoolStore(dbNameCofig, logger);
//    }
//
//    public DbEliteGroupPay(String sessionName, Logger logger) throws SQLException, Exception {
//        this.logger = logger;
//        dbNameCofig = sessionName;
//        poolStore = new PoolStore(sessionName, logger);
//    }
//
//    public void closeStatement(Statement st) {
//        try {
//            if (st != null) {
//                st.close();
//                st = null;
//            }
//        } catch (Exception ex) {
//            st = null;
//        }
//    }
//
//    public void logTimeDb(String strLog, long timeSt) {
//        long timeEx = System.currentTimeMillis() - timeSt;
//
//        if (timeEx >= AppManager.minTimeDb && AppManager.loggerDbMap != null) {
//            br.setLength(0);
//            br.append(loggerLabel).
//                    append(AppManager.getTimeLevelDb(timeEx)).append(": ").
//                    append(strLog).
//                    append(": ").
//                    append(timeEx).
//                    append(" ms");
//
//            logger.warn(br);
//        } else {
//            br.setLength(0);
//            br.append(loggerLabel).
//                    append(strLog).
//                    append(": ").
//                    append(timeEx).
//                    append(" ms");
//
//            logger.info(br);
//        }
//    }
//
//    @Override
//    public Record parse(ResultSet rs) {
//        EliteGroupInput record = new EliteGroupInput();
//        long timeSt = System.currentTimeMillis();
//        try {
//            record.setMoId(rs.getLong("MO_ID"));
//            record.setMsisdn(rs.getString("MSISDN"));
//            record.setCommand(rs.getString("COMMAND"));
//            record.setInput(rs.getString("PARAM"));
//            record.setReceiveTime(rs.getTimestamp("RECEIVE_TIME"));
//            record.setActionType(rs.getLong("ACTION_TYPE"));
//            record.setChannel(rs.getString("CHANNEL"));
//            record.setChannelType(rs.getString("CHANNEL_TYPE"));
//            record.setExpiresTime(rs.getDate("EXPIRES_TIME"));
//            record.setMoneyFee(rs.getLong("MONEY_FEE"));
//            record.setResultCode("0");
//        } catch (Exception ex) {
//            logger.error("ERROR parse EliteGroupPay");
//            logger.error(AppManager.logException(timeSt, ex));
//        }
//        return record;
//    }
//
//    @Override
//    public int[] deleteQueue(List<Record> listRecords) {
//        long timeStart = System.currentTimeMillis();
//        PreparedStatement ps = null;
//        Connection connection = null;
//        String batchId = "";
//        try {
//            connection = ConnectionPoolManager.getConnection(dbNameCofig);
//            ps = connection.prepareStatement(sqlDeleteMo);
//            for (Record rc : listRecords) {
//                EliteGroupInput sd = (EliteGroupInput) rc;
//                batchId = sd.getBatchId();
//                ps.setLong(1, sd.getMoId());
//                ps.addBatch();
//            }
//            return ps.executeBatch();
//        } catch (Exception ex) {
//            logger.error("ERROR deleteQueue VIP_SUB_DETAIL batchid " + batchId, ex);
//            logger.error(AppManager.logException(timeStart, ex));
//            return null;
//        } finally {
//            closeStatement(ps);
//            closeConnection(connection);
//            logTimeDb("Time to deleteQueue VIP_SUB_DETAIL, batchid " + batchId, timeStart);
//        }
//    }
//
//    @Override
//    public int[] insertQueueHis(List<Record> listRecords) {
//        if (listRecords.isEmpty()) {
//            return new int[0];
//        }
//
//        List<ParamList> listParam = new ArrayList<ParamList>();
//        String batchId = "";
//        long timeSt = System.currentTimeMillis();
//        try {
//            for (Record rc : listRecords) {
//                EliteGroupInput kv = (EliteGroupInput) rc;
//                batchId = kv.getBatchId();
//                ParamList paramList = new ParamList();
//                paramList.add(new Param("MO_HIS_ID", kv.getMoId(), Param.DataType.LONG, Param.IN));
//                paramList.add(new Param("MSISDN", kv.getMsisdn(), Param.DataType.STRING, Param.IN));
//                paramList.add(new Param("SUB_ID", null, Param.DataType.LONG, Param.IN));
//                paramList.add(new Param("PRODUCT_CODE", kv.getCommand(), Param.DataType.STRING, Param.IN));
//                paramList.add(new Param("SUB_TYPE", null, Param.DataType.LONG, Param.IN));
//                paramList.add(new Param("CHANNEL", kv.getChannel(), Param.DataType.STRING, Param.IN));
//                paramList.add(new Param("COMMAND", kv.getCommand(), Param.DataType.STRING, Param.IN));
//                paramList.add(new Param("PARAM", kv.getInput(), Param.DataType.STRING, Param.IN));
//                paramList.add(new Param("RECEIVE_TIME", kv.getReceiveTime(), Param.DataType.TIMESTAMP, Param.IN));
//                paramList.add(new Param("ACTION_TYPE", kv.getActionType(), Param.DataType.LONG, Param.IN));
//                paramList.add(new Param("PROCESS_TIME", "sysdate", Param.DataType.CONST, Param.IN));
//                paramList.add(new Param("ERR_CODE", kv.getResultCode(), Param.DataType.STRING, Param.IN));
//                paramList.add(new Param("ERR_OCS", null, Param.DataType.STRING, Param.IN));
//                paramList.add(new Param("FEE", kv.getMoneyFee(), Param.DataType.LONG, Param.IN));
//                paramList.add(new Param("NODE_NAME", kv.getNodeName(), Param.DataType.STRING, Param.IN));
//                paramList.add(new Param("CLUSTER_NAME", kv.getClusterName(), Param.DataType.STRING, Param.IN));
//                paramList.add(new Param("FEE_ACTION", null, Param.DataType.LONG, Param.IN));
//                paramList.add(new Param("PROMOTION_VALUE", null, Param.DataType.LONG, Param.IN));
//                paramList.add(new Param("CHANNEL_TYPE", kv.getChannelType(), Param.DataType.STRING, Param.IN));
//                paramList.add(new Param("DESCRIPTION", kv.getDesctiption(), Param.DataType.STRING, Param.IN));
//                listParam.add(paramList);
//            }
//            int[] res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "elite_group_input_his");
//            logTimeDb("Time to deleteQueue elite_group_input_his, batchid " + batchId + " total result: " + res.length, timeSt);
//            return res;
//        } catch (Exception ex) {
//            logger.error("ERROR deleteQueue elite_group_input_his batchid " + batchId, ex);
//            logger.error(AppManager.logException(timeSt, ex));
//            return null;
//        }
//    }
//
//    @Override
//    public int[] insertQueueOutput(List<Record> listRecords) {
//        return new int[0];
//    }
//
//    @Override
//    public int[] updateQueueInput(List<Record> listRecords) {
//        return new int[0];
//    }
//
//    @Override
//    public void processTimeoutRecord(List<String> ids) {
//        StringBuilder sb = new StringBuilder();
//        try {
////            The first delete queue timeout
//            deleteQueueTimeout(ids);
////            Save history
//            for (String sd : ids) {
//                sb.append(": ").append(sd);
//            }
//            logger.warn("Dispatcher not get reponse from agent, so processTimeoutRecord ID " + sb.toString());
//        } catch (Exception ex) {
//            logger.error("ERROR processTimeoutRecord ID " + sb.toString() + " " + ex.toString());
//        }
//    }
//
//    @Override
//    public void updateSqlMoParam(List<Record> lrc) {
//        throw new UnsupportedOperationException("Not supported yet.");
//    }
//
//    public int[] deleteQueueTimeout(List<String> listId) {
//        return new int[0];
//    }
//
//    public Float getMoneyFeeEliteGroupInput(String id) {
//        ResultSet rs1 = null;
//        Connection connection = null;
//        String sqlMo = " select money_fee from elite_group_input where mo_id =?  ";
//        PreparedStatement psMo = null;
//        Float kq = null;
//        try {
//            connection = getConnection("dbElite");
//            psMo = connection.prepareStatement(sqlMo);
//            psMo.setString(1, id);
//            rs1 = psMo.executeQuery();
//            while (rs1.next()) {
//                kq = rs1.getFloat("money_fee");
//                break;
//            }
//        } catch (Exception ex) {
//            logger.error("ERROR getMoneyFeeEliteGroupInput id " + id);
//            logger.error("ERROR getMoneyFeeEliteGroupInput ex " + ex.getMessage());
//        } finally {
//            closeResultSet(rs1);
//            closeStatement(psMo);
//            closeConnection(connection);
//        }
//        return kq;
//    }
//}
