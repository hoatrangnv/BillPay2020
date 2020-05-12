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
public class DbSmasProcessor extends DbProcessorAbstract {

//    private Logger logger;
    private String loggerLabel = DbSmasProcessor.class.getSimpleName() + ": ";
//    private StringBuffer br = new StringBuffer();
    private PoolStore poolStore;
    private String dbNameCofig;

    public DbSmasProcessor() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
        dbNameCofig = "dbSmas";
        poolStore = new PoolStore(dbNameCofig, logger);
    }

    public DbSmasProcessor(String sessionName, Logger logger) throws SQLException, Exception {
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
        SmasAnnounce record = new SmasAnnounce();
        long timeSt = System.currentTimeMillis();
        try {
            record.setId(rs.getLong("announce_id"));
            record.setAnnounceId(rs.getLong("announce_id"));
            record.setAnnounceName(rs.getString("announce_name"));
            record.setContent(rs.getString("content"));
            record.setSenderId(rs.getLong("SENDER_ID"));
            record.setReceiverId(rs.getLong("RECEIVER_ID"));
            record.setCreateTime(rs.getTimestamp("CREATE_TIME"));
            record.setAnnounceType(rs.getLong("ANNOUNCE_TYPE"));
            record.setCommentType(rs.getLong("comment_type"));
            record.setSendType(rs.getLong("SEND_TYPE"));
            record.setSchoolId(rs.getLong("SCHOOL_ID"));
            record.setScheduleId(rs.getLong("schedule_id"));
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
            ps = connection.prepareStatement("delete announcement where announce_id = ?");
            for (Record rc : listRecords) {
                SmasAnnounce sd = (SmasAnnounce) rc;
                batchId = sd.getBatchId();
                ps.setLong(1, sd.getAnnounceId());
                ps.addBatch();
            }
            return ps.executeBatch();
        } catch (Exception ex) {
            logger.error("ERROR deleteQueue announcement batchid " + batchId, ex);
            logger.error(AppManager.logException(timeStart, ex));
            return null;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to deleteQueue announcement, batchid " + batchId, timeStart);
        }
    }

    @Override
    public int[] insertQueueHis(List<Record> listRecords) {
        List<ParamList> listParam = new ArrayList<ParamList>();
        String batchId = "";
        long timeSt = System.currentTimeMillis();
        try {
            for (Record rc : listRecords) {
                SmasAnnounce sd = (SmasAnnounce) rc;
                batchId = sd.getBatchId();
                ParamList paramList = new ParamList();
                paramList.add(new Param("announce_id", sd.getAnnounceId(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param("announce_name", sd.getAnnounceName(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("content", sd.getContent(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("sender_id", sd.getSenderId(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param("receiver_id", sd.getReceiverId(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param("announce_type", sd.getAnnounceType(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param("comment_type", sd.getCommentType(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param("send_type", sd.getSendType(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param("school_id", sd.getSchoolId(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param("schedule_id", sd.getScheduleId(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param("create_time", sd.getCreateTime(), Param.DataType.TIMESTAMP, Param.IN));
                paramList.add(new Param("send_time", "sysdate", Param.DataType.CONST, Param.IN));
                paramList.add(new Param("RESULT_CODE", sd.getResultCode(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("DESCRIPTION", sd.getDescription(), Param.DataType.STRING, Param.IN));
                listParam.add(paramList);
            }
            int[] res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "announcement_his");
            logTimeDb("Time to insertQueueHis announcement_his, batchid " + batchId + " total result: " + res.length, timeSt);
            return res;
        } catch (Exception ex) {
            logger.error("ERROR insertQueueHis announcement_his batchid " + batchId, ex);
            logger.error(AppManager.logException(timeSt, ex));
            return null;
        }
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

    public String getTeacherPhoneById(long teacherId) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        String result = "";
        String sqlMo = "select phone from teacher where teacher_id = ?";
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
                result = rs1.getString("phone");
                break;
            }
            logTimeDb("Time to getTeacherPhoneById teacherId " + teacherId + " result phone " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getTeacherPhoneById teacherId " + teacherId);
            logger.error(AppManager.logException(timeSt, ex));
            result = null;
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return result;
    }

    public String getTeacherPhoneByParentId(long parentId) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        String result = "";
        String sqlMo = "select phone from teacher where teacher_id = (\n"
                + " select head_teacher from from class where class_id in (\n"
                + " select class_id from student where father_id = ? or mother_id = ?\n"
                + " )\n"
                + " )";
        PreparedStatement psMo = null;
        try {
            connection = getConnection(dbNameCofig);
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setLong(1, parentId);
            psMo.setLong(2, parentId);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                result = rs1.getString("phone");
                break;
            }
            logTimeDb("Time to getTeacherPhoneByParentId parentId " + parentId + " result phone " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getTeacherPhoneByParentId parentId " + parentId);
            logger.error(AppManager.logException(timeSt, ex));
            result = null;
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return result;
    }

    public String getStudentPhoneById(long studentId) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        String result = "";
        String sqlMo = "select phone from student where student_id = ?";
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
                result = rs1.getString("phone");
                break;
            }
            logTimeDb("Time to getStudentPhoneById studentId " + studentId + " result phone " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getStudentPhoneById studentId " + studentId);
            logger.error(AppManager.logException(timeSt, ex));
            result = null;
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return result;
    }

    public String getParentPhoneById(long parentId) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        String result = "";
        String sqlMo = "select phone from parent where parent_id = ?";
        PreparedStatement psMo = null;
        try {
            connection = getConnection(dbNameCofig);
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setLong(1, parentId);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                result = rs1.getString("phone");
                break;
            }
            logTimeDb("Time to getParentPhoneById parentId " + parentId + " result phone " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getParentPhoneById parentId " + parentId);
            logger.error(AppManager.logException(timeSt, ex));
            result = null;
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return result;
    }

    public ArrayList<String> getAllStudentByClass(long classId) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        ArrayList<String> result = new ArrayList<String>();
        String sqlMo = "select phone from student where class_id = ?";
        PreparedStatement psMo = null;
        try {
            connection = getConnection(dbNameCofig);
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setLong(1, classId);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                String phone = rs1.getString("phone");
                if (phone != null && phone.trim().length() > 0) {
                    result.add(phone);
                }
            }
            logTimeDb("Time to getAllStudentByClass classId " + classId + " result size " + result.size(), timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getAllStudentByClass classId " + classId);
            logger.error(AppManager.logException(timeSt, ex));
            result = null;
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return result;
    }

    public ArrayList<String> getAllParentByClass(long classId) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        ArrayList<String> result = new ArrayList<String>();
        String sqlMo = "select phone from parent where parent_id in "
                + " (select father_id from student where class_id = ? UNION select mother_id from student where class_id = ?)";
        PreparedStatement psMo = null;
        try {
            connection = getConnection(dbNameCofig);
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setLong(1, classId);
            psMo.setLong(2, classId);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                String phone = rs1.getString("phone");
                if (phone != null && phone.trim().length() > 0) {
                    result.add(phone);
                }
            }
            logTimeDb("Time to getAllParentByClass classId " + classId + " result size " + result.size(), timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getAllParentByClass classId " + classId);
            logger.error(AppManager.logException(timeSt, ex));
            result = null;
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return result;
    }

    public ArrayList<String> getAllStudentBySchool(long schoolId) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        ArrayList<String> result = new ArrayList<String>();
        String sqlMo = "select phone from student where school_id = ?";
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
                String phone = rs1.getString("phone");
                if (phone != null && phone.trim().length() > 0) {
                    result.add(phone);
                }
            }
            logTimeDb("Time to getAllStudentBySchool schoolId " + schoolId + " result size " + result.size(), timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getAllStudentBySchool schoolId " + schoolId);
            logger.error(AppManager.logException(timeSt, ex));
            result = null;
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return result;
    }

    public ArrayList<String> getAllParentBySchool(long schoolId) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        ArrayList<String> result = new ArrayList<String>();
        String sqlMo = "select phone from parent where parent_id in "
                + " (select father_id from student where school_id = ? UNION select mother_id from student where school_id = ?)";
        PreparedStatement psMo = null;
        try {
            connection = getConnection(dbNameCofig);
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setLong(1, schoolId);
            psMo.setLong(2, schoolId);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                String phone = rs1.getString("phone");
                if (phone != null && phone.trim().length() > 0) {
                    result.add(phone);
                }
            }
            logTimeDb("Time to getAllParentBySchool schoolId " + schoolId + " result size " + result.size(), timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getAllParentBySchool schoolId " + schoolId);
            logger.error(AppManager.logException(timeSt, ex));
            result = null;
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return result;
    }

    public ArrayList<String> getAllTeacherBySchool(long schoolId) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        ArrayList<String> result = new ArrayList<String>();
        String sqlMo = "select phone from teacher where school_id = ?";
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
                String phone = rs1.getString("phone");
                if (phone != null && phone.trim().length() > 0) {
                    result.add(phone);
                }
            }
            logTimeDb("Time to getAllTeacherBySchool schoolId " + schoolId + " result size " + result.size(), timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getAllTeacherBySchool schoolId " + schoolId);
            logger.error(AppManager.logException(timeSt, ex));
            result = null;
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return result;
    }

    public ArrayList<String> getAllStudentByGroup(long schoolId, long groupId) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        ArrayList<String> result = new ArrayList<String>();
        String sqlMo = "select phone from student where school_id = ? and class_id in ("
                + " select class_id from class where class_name like '" + groupId + "%' )";
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
                String phone = rs1.getString("phone");
                if (phone != null && phone.trim().length() > 0) {
                    result.add(phone);
                }
            }
            logTimeDb("Time to getAllStudentByGroup schoolId " + schoolId + " result size " + result.size(), timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getAllStudentByGroup schoolId " + schoolId);
            logger.error(AppManager.logException(timeSt, ex));
            result = null;
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return result;
    }

    public ArrayList<String> getAllParentByGroup(long schoolId, long groupId) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        ArrayList<String> result = new ArrayList<String>();
        String sqlMo = "select phone from parent where school_id = ? and parent_id "
                + "in (select parent_id from student where class_id in "
                + " (select class_id from class where class_name like '" + groupId + "%' ))";
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
                String phone = rs1.getString("phone");
                if (phone != null && phone.trim().length() > 0) {
                    result.add(phone);
                }
            }
            logTimeDb("Time to getAllParentByGroup schoolId " + schoolId + " result size " + result.size(), timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getAllParentByGroup schoolId " + schoolId);
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
