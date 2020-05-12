///*
// * Copyright 2012 Viettel Telecom. All rights reserved.
// * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
// */
//package com.viettel.paybonus.database;
//
//import com.viettel.cluster.agent.integration.Record;
//import com.viettel.threadfw.manager.AppManager;
//import com.viettel.paybonus.obj.*;
//import com.viettel.threadfw.database.DbProcessorAbstract;
//import com.viettel.vas.util.PoolStore;
//import com.viettel.vas.util.obj.DataResources;
//import com.viettel.vas.util.obj.Param;
//import com.viettel.vas.util.obj.ParamList;
//import java.sql.ResultSet;
//import java.sql.SQLException;
//import java.sql.Statement;
//import java.util.ArrayList;
//import java.util.Date;
//import java.util.List;
//import java.util.ResourceBundle;
//import org.apache.log4j.Logger;
//import java.sql.Connection;
//import com.viettel.vas.util.ConnectionPoolManager;
//import java.sql.PreparedStatement;
//import java.sql.Timestamp;
//import java.text.SimpleDateFormat;
//
///**
// *
// * Thong tin phien ban
// *
// * @author HuyNQ13
// * @version 1.0
// * @since 24-03-2016
// */
//public class DbSalary extends DbProcessorAbstract {
//
////    private Logger logger;
//    private String loggerLabel = DbSalary.class.getSimpleName() + ": ";
////    private StringBuffer br = new StringBuffer();
//    private PoolStore poolStore;
//    private String dbNameCofig;
//    private String sqlDeleteMo = "delete vip_sub_detail where vip_sub_detail_id = ?";
//    private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
//
//    public DbSalary() throws SQLException, Exception {
//        this.logger = Logger.getLogger(loggerLabel);
//        dbNameCofig = ResourceBundle.getBundle("configPayBonus").getString("dbVipSubNameConfig");
//        poolStore = new PoolStore(dbNameCofig, logger);
//    }
//
//    public DbSalary(String sessionName, Logger logger) throws SQLException, Exception {
//        this.logger = logger;
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
//        SalaryInfo record = new SalaryInfo();
//        long timeSt = System.currentTimeMillis();
//        try {
//            record.setId(rs.getLong("SALARY_SALEMAN_INFO_ID"));
//            record.setStaffCode(rs.getString("STAFF_CODE"));
//            record.setStaffName(rs.getString("STAFF_NAME"));
//            record.setIsdn(rs.getString(VipSubDetail.ISDN));
//            record.setHisNextProcessTime(rs.getTimestamp(VipSubDetail.NEXT_PROCESS_TIME));
//            record.setCycleType(rs.getString(VipSubDetail.CYCLE_TYPE));
//            record.setMoneyAcc(rs.getString(VipSubDetail.MONEY_ACC));
//            record.setMoneyValue(rs.getString(VipSubDetail.MONEY_VALUE));
//            record.setSmsAcc(rs.getString(VipSubDetail.SMS_ACC));
//            record.setSmsValue(rs.getString(VipSubDetail.SMS_VALUE));
//            record.setSmsOutAcc(rs.getString(VipSubDetail.SMS_OUT_ACC));
//            record.setSmsOutValue(rs.getString(VipSubDetail.SMS_OUT_VALUE));
//            record.setDataAcc(rs.getString(VipSubDetail.DATA_ACC));
//            record.setDataValue(rs.getString(VipSubDetail.DATA_VALUE));
//            record.setVoiceAcc(rs.getString(VipSubDetail.VOICE_ACC));
//            record.setVoiceValue(rs.getString(VipSubDetail.VOICE_VALUE));
//            record.setVoiceOutAcc(rs.getString(VipSubDetail.VOICE_OUT_ACC));
//            record.setVoiceOutValue(rs.getString(VipSubDetail.VOICE_OUT_VALUE));
//            record.setCreateTime(rs.getTimestamp(VipSubDetail.CREATE_TIME));
//            record.setCreateUser(rs.getString(VipSubDetail.CREATE_USER));
//            record.setStatus(rs.getString(VipSubDetail.STATUS));
//            record.setResultCode("0");
//            record.setDescription("Processing");
//        } catch (Exception ex) {
//            logger.error("ERROR parse MoRecord");
//            logger.error(AppManager.logException(timeSt, ex));
//        }
//        return record;
//    }
//
//    @Override
//    public int[] deleteQueue(List<Record> listRecords) {
//        Connection connection = null;
//        PreparedStatement ps = null;
//        StringBuilder br = new StringBuilder();
//        String sql = "";
//        int[] res = new int[0];
//        long startTime = System.currentTimeMillis();
//        try {
//            connection = getConnection(dbNameCofig);
//            sql = "update SALARY_SALEMAN_INFO set RESCUE_CLOSE_TIME = sysdate"
//                    + " where SALARY_SALEMAN_INFO_ID = ?";
//            ps = connection.prepareStatement(sql);
//            for (Record rc : listRecords) {
//                ComplainInfo cp = (ComplainInfo) rc;
//                if (!"0".equals(cp.getResultCode())) {
//                    logger.warn("Ignore complain " + cp.getId() + " because resultcode is " + cp.getResultCode());
//                    continue;
//                } else {
//                    logger.info("Add batch for complain " + cp.getId() + " isdn " + cp.getRescueModelIsdn());
//                    ps.setLong(1, cp.getId());
//                    ps.addBatch();
//                }
//            }
//            res = ps.executeBatch();
//            logger.info("End updateComplain time " + (System.currentTimeMillis() - startTime));
//        } catch (Exception ex) {
//            br.setLength(0);
//            br.append(loggerLabel).append(new Date()).
//                    append("\nERROR updateComplain: ");
//            logger.error(br + ex.toString());
//        } finally {
//            closeStatement(ps);
//            closeConnection(connection);
//            return res;
//        }
//    }
//
//    @Override
//    public int[] insertQueueHis(List<Record> listRecords) {
//        List<ParamList> listParam = new ArrayList<ParamList>();
//        String batchId = "";
//        long timeSt = System.currentTimeMillis();
//        try {
//            for (Record rc : listRecords) {
//                VipSubDetail sd = (VipSubDetail) rc;
//                batchId = sd.getBatchId();
//                ParamList paramList = new ParamList();
//                paramList.add(new Param("VIP_SUB_PROCESS_LOG_ID", "vip_sub_process_log_seq.nextval", Param.DataType.CONST, Param.IN));
//                paramList.add(new Param(VipSubDetail.VIP_SUB_DETAIL_ID, sd.getVipSubDetailId(), Param.DataType.LONG, Param.IN));
//                paramList.add(new Param(VipSubDetail.ISDN, sd.getIsdn(), Param.DataType.STRING, Param.IN));
//                paramList.add(new Param(VipSubDetail.HIS_NEXT_PROCESS_TIME, sd.getHisNextProcessTime(), Param.DataType.TIMESTAMP, Param.IN));
//                paramList.add(new Param(VipSubDetail.NEXT_PROCESS_TIME, sd.getNextProcessTime(), Param.DataType.TIMESTAMP, Param.IN));
//                paramList.add(new Param("CLUSTER_NAME", sd.getClusterName(), Param.DataType.STRING, Param.IN));
//                paramList.add(new Param("NODE_NAME", sd.getNodeName(), Param.DataType.STRING, Param.IN));
//                paramList.add(new Param(VipSubDetail.RESULT_CODE, sd.getResultCode(), Param.DataType.STRING, Param.IN));
//                paramList.add(new Param(VipSubDetail.DESCRIPTION, sd.getDescription(), Param.DataType.STRING, Param.IN));
//                listParam.add(paramList);
//            }
//            int[] res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "vip_sub_process_log");
//            logTimeDb("Time to insertQueueHis vip_sub_process_log, batchid " + batchId + " total result: " + res.length, timeSt);
//            return res;
//        } catch (Exception ex) {
//            logger.error("ERROR insertQueueHis vip_sub_process_log batchid " + batchId, ex);
//            try {
//                int[] res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "salary_saleman_his");
//                logTimeDb("Time to retry insertQueueHis vip_sub_process_log, batchid " + batchId + " total result: " + res.length, timeSt);
//                return res;
//            } catch (Exception ex1) {
//                logger.error("ERROR retry insertQueueHis vip_sub_process_log batchid " + batchId, ex1);
//                logger.error(AppManager.logException(timeSt, ex));
//                return null;
//            }
//        }
//    }
//
//    @Override
//    public int[] insertQueueOutput(List<Record> listRecords) {
//        int[] res = new int[0];
//        return res;
//    }
//
//    @Override
//    public int[] updateQueueInput(List<Record> listRecords) {
//        throw new UnsupportedOperationException("Not supported yet.");
//    }
//
//    @Override
//    public void processTimeoutRecord(List<String> ids) {
//        List<ParamList> listParam = new ArrayList<ParamList>();
//        StringBuilder sb = new StringBuilder();
//        long timeSt = System.currentTimeMillis();
//        try {
////            The first delete queue timeout
//            deleteQueueTimeout(ids);
////            Save history
//            for (String sd : ids) {
//                sb.append(":" + sd);
//                ParamList paramList = new ParamList();
//                paramList.add(new Param(VipSubDetail.VIP_SUB_DETAIL_ID, Long.valueOf(sd), Param.DataType.LONG, Param.IN));
//                paramList.add(new Param(VipSubDetail.RESULT_CODE, "FW_99", Param.DataType.STRING, Param.IN));
//                paramList.add(new Param(VipSubDetail.DESCRIPTION, "FW_Timeout", Param.DataType.STRING, Param.IN));
//                paramList.add(new Param("log_time", "sysdate", Param.DataType.CONST, Param.IN));
//                listParam.add(paramList);
//            }
//            int[] res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "vip_sub_process_log");
//            logTimeDb("Time to processTimeoutRecord, insert vip_sub_process_log, total result: " + res.length, timeSt);
//            logger.warn("Dispatcher not get reponse from agent, so processTimeoutRecord ID " + sb.toString());
//        } catch (Exception ex) {
//            logger.error("ERROR processTimeoutRecord ID " + sb.toString());
//            try {
//                int[] res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "vip_sub_process_log");
//                logTimeDb("Time to retry processTimeoutRecord, insert vip_sub_process_log, total result: " + res.length, timeSt);
//            } catch (Exception ex1) {
//                logger.error("ERROR retry processTimeoutRecord ", ex1);
//                logger.error(AppManager.logException(timeSt, ex));
//            }
//        }
//    }
//
//    @Override
//    public void updateSqlMoParam(List<Record> lrc) {
//        throw new UnsupportedOperationException("Not supported yet.");
//    }
//
//    public boolean checkAlreadyProcessRecord(String isdn, String processDate) {
//        long timeSt = System.currentTimeMillis();
//        ResultSet rs1 = null;
//        Connection connection = null;
//        boolean result = false;
//        String sqlMo = "select isdn from vip_sub_process_log where isdn = ? and his_next_process_time = to_date(?,'yyyyMMddhh24miss')";
//        PreparedStatement psMo = null;
//        try {
//            connection = ConnectionPoolManager.getConnection(dbNameCofig);
//            psMo = connection.prepareStatement(sqlMo);
//            if (QUERY_TIMEOUT > 0) {
//                psMo.setQueryTimeout(QUERY_TIMEOUT);
//            }
//            psMo.setString(1, isdn);
//            psMo.setString(2, processDate);
//            rs1 = psMo.executeQuery();
//            while (rs1.next()) {
//                String soluong = rs1.getString("isdn");
//                if (soluong != null && soluong.trim().length() > 0) {
//                    result = true;
//                    break;
//                }
//            }
//            logTimeDb("Time to checkAlreadyProcessRecord msisdn " + isdn + " processDate " + processDate + " result: " + result, timeSt);
//        } catch (Exception ex) {
//            logger.error("ERROR checkAlreadyProcessRecord msisdn " + isdn);
//            logger.error(AppManager.logException(timeSt, ex));
//            result = false;
//        } finally {
//            closeResultSet(rs1);
//            closeStatement(psMo);
//            closeConnection(connection);
//        }
//        return result;
//    }
//
//    public int[] deleteQueueTimeout(List<String> listId) {
//        long timeStart = System.currentTimeMillis();
//        PreparedStatement ps = null;
//        Connection connection = null;
//        StringBuilder sf = new StringBuilder();
//        try {
//            dbNameCofig = ResourceBundle.getBundle("configPayBonus").getString("dbVipSubNameConfig");
//            connection = ConnectionPoolManager.getConnection(dbNameCofig);
//            ps = connection.prepareStatement(sqlDeleteMo);
//            sf.setLength(0);
//            for (String id : listId) {
//                ps.setLong(1, Long.valueOf(id));
//                ps.addBatch();
//                sf.append(id);
//                sf.append(", ");
//            }
//            return ps.executeBatch();
//        } catch (Exception ex) {
//            logger.error("ERROR deleteQueueTimeout VIP_SUB_DETAIL listId " + sf.toString(), ex);
//            logger.error(AppManager.logException(timeStart, ex));
//            return null;
//        } finally {
//            closeStatement(ps);
//            closeConnection(connection);
//            logTimeDb("Time to deleteQueueTimeout VIP_SUB_DETAIL, listId " + sf.toString(), timeStart);
//        }
//    }
//}
