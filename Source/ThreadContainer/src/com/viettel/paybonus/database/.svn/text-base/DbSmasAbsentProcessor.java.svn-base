/*
 * Copyright 2012 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.paybonus.database;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.threadfw.manager.AppManager;
import com.viettel.paybonus.obj.*;
import com.viettel.threadfw.database.DbProcessorAbstract;
import com.viettel.vas.util.ConnectionPoolManager;
import com.viettel.vas.util.PoolStore;
import com.viettel.vas.util.obj.Param;
import com.viettel.vas.util.obj.ParamList;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
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
public class DbSmasAbsentProcessor extends DbProcessorAbstract {

//    private Logger logger;
    private String loggerLabel = DbSmasAbsentProcessor.class.getSimpleName() + ": ";
//    private StringBuffer br = new StringBuffer();
    private PoolStore poolStore;
    private String dbNameCofig;

    public DbSmasAbsentProcessor() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
        dbNameCofig = "dbSmas";
        poolStore = new PoolStore(dbNameCofig, logger);
    }

    public DbSmasAbsentProcessor(String sessionName, Logger logger) throws SQLException, Exception {
        this.logger = logger;
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
        SmasAbsent record = new SmasAbsent();
        long timeSt = System.currentTimeMillis();
        try {
            record.setId(rs.getLong("absent_id"));
            record.setStudentId(rs.getLong("student_id"));
            record.setTeacherId(rs.getLong("teacher_id"));
            record.setClassId(rs.getLong("class_ID"));
            record.setSubjectId(rs.getLong("subject_ID"));
            record.setNotifyParent(rs.getLong("notify_parent"));
            record.setResultCode("0");
            record.setDescription("Processing");
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
            ps = connection.prepareStatement("update absent set notify_parent = 1 where absent_id = ?");
            for (Record rc : listRecords) {
                SmasAbsent sd = (SmasAbsent) rc;
                batchId = sd.getBatchId();
                ps.setLong(1, sd.getId());
                ps.addBatch();
            }
            return ps.executeBatch();
        } catch (Exception ex) {
            logger.error("ERROR update absent batchid " + batchId, ex);
            logger.error(AppManager.logException(timeStart, ex));
            return null;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to update absent, batchid " + batchId, timeStart);
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
            connection = getConnection(dbNameCofig);
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
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public String getTeacherNameById(long teacherId) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        String result = "";
        String sqlMo = "select teacher_name from teacher where teacher_id = ?";
        PreparedStatement psMo = null;
        try {
            connection = getConnection(dbNameCofig);
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setLong(1, teacherId);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                result = rs1.getString("teacher_name");
                break;
            }
            logTimeDb("Time to getTeacherNameById teacherId " + teacherId + " result phone " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getTeacherNameById teacherId " + teacherId);
            logger.error(AppManager.logException(timeSt, ex));
            result = null;
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return result;
    }

    public String getStudentNameById(long studentId) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        String result = "";
        String sqlMo = "select student_name from student where student_id = ?";
        PreparedStatement psMo = null;
        try {
            connection = getConnection(dbNameCofig);
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setLong(1, studentId);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                result = rs1.getString("student_name");
                break;
            }
            logTimeDb("Time to getTeacherNameById studentId " + studentId + " result phone " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getStudentNameById studentId " + studentId);
            logger.error(AppManager.logException(timeSt, ex));
            result = null;
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return result;
    }

    public String getParentPhoneByStudentId(long studentId) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        String result = "";
        String sqlMo = "select phone from parent where parent_id in "
                + " ((select father_id from student where student_id = ?)"
                + " union (select mother_id from student where student_id = ?))";
        PreparedStatement psMo = null;
        try {
            connection = getConnection(dbNameCofig);
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setLong(1, studentId);
            psMo.setLong(2, studentId);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                result = rs1.getString("phone");
                if (result != null && result.trim().length() > 0) {
                    break;
                }
            }
            logTimeDb("Time to getParentPhoneByStudentId studentId " + studentId + " result phone " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getParentPhoneByStudentId studentId " + studentId);
            logger.error(AppManager.logException(timeSt, ex));
            result = null;
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return result;
    }

    public String getShortCodeOfSchool(long schoolId) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        String result = "";
        String sqlMo = "select sms_short_code from school where school_id = ?";
        PreparedStatement psMo = null;
        try {
            connection = getConnection(dbNameCofig);
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setLong(1, schoolId);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                result = rs1.getString("sms_short_code");
                break;
            }
            logTimeDb("Time to getShortCodeOfSchool schoolId " + schoolId + " result shortcode " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getShortCodeOfSchool schoolId " + schoolId);
            logger.error(AppManager.logException(timeSt, ex));
            result = null;
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return result;
    }
}
