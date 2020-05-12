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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
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
public class DbVipSubWarning extends DbProcessorAbstract {

//    private Logger logger;
    private String loggerLabel = DbVipSubWarning.class.getSimpleName() + ": ";
//    private StringBuffer br = new StringBuffer();
    private PoolStore poolStore;
    private String dbNameCofig;

    public DbVipSubWarning() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
        dbNameCofig = ResourceBundle.getBundle("configPayBonus").getString("dbVipSubNameConfig");
        poolStore = new PoolStore(dbNameCofig, logger);
    }

    public DbVipSubWarning(String sessionName, Logger logger) throws SQLException, Exception {
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
        VipSubDetail record = new VipSubDetail();
        long timeSt = System.currentTimeMillis();
        try {
            record.setId(rs.getLong(VipSubDetail.VIP_SUB_INFO_ID));
            record.setVipSubInfoId(rs.getLong(VipSubDetail.VIP_SUB_INFO_ID));
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
        List<ParamList> listParam = new ArrayList<ParamList>();
        StringBuilder sb = new StringBuilder();
        long timeSt = System.currentTimeMillis();
        try {
//            The first delete queue timeout
            deleteQueueTimeout(ids);
//            Save history
            for (String sd : ids) {
                sb.append(":" + sd);
                ParamList paramList = new ParamList();
                paramList.add(new Param(VipSubDetail.VIP_SUB_DETAIL_ID, Long.valueOf(sd), Param.DataType.LONG, Param.IN));
                paramList.add(new Param(VipSubDetail.RESULT_CODE, "FW_99", Param.DataType.STRING, Param.IN));
                paramList.add(new Param(VipSubDetail.DESCRIPTION, "FW_Timeout", Param.DataType.STRING, Param.IN));
                paramList.add(new Param("log_time", "sysdate", Param.DataType.CONST, Param.IN));
                listParam.add(paramList);
            }
            int[] res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "vip_sub_process_log");
            logTimeDb("Time to processTimeoutRecord, insert vip_sub_process_log, total result: " + res.length, timeSt);
            logger.warn("Dispatcher not get reponse from agent, so processTimeoutRecord ID " + sb.toString());
        } catch (Exception ex) {
            logger.error("ERROR processTimeoutRecord ID " + sb.toString());
            try {
                int[] res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "vip_sub_process_log");
                logTimeDb("Time to retry processTimeoutRecord, insert vip_sub_process_log, total result: " + res.length, timeSt);
            } catch (Exception ex1) {
                logger.error("ERROR retry processTimeoutRecord ", ex1);
                logger.error(AppManager.logException(timeSt, ex));
            }
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
        if (!msisdn.startsWith("258")) {
            msisdn = "258" + msisdn;
        }
        try {
            connection = getConnection("dbapp2");
            sql = "INSERT INTO mt (mt_id,msisdn,message,mo_his_id,retry_num,receive_time,channel) "
                    + "VALUES(mt_SEQ.nextval,?,?,null,0,sysdate,?)";
            ps = connection.prepareStatement(sql);
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

    public long updateWarning(Long vipSubInfoId) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int res = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(dbNameCofig);
            sql = "update vip_sub_detail set status_warn = 1 where vip_sub_info_id = ? ";
            ps = connection.prepareStatement(sql);
            logger.info("Start updateWarning vipSubInfoId " + vipSubInfoId);
            ps.setLong(1, vipSubInfoId);
            res = ps.executeUpdate();
            logger.info("End updateWarning vipSubInfoId " + vipSubInfoId + " time " + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR vipSubInfoId vipSubInfoId " + vipSubInfoId);
            logger.error(br + ex.toString());
            res = 0;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return res;
        }
    }

    public long updateWarningVipSubInfo(Long vipSubInfoId) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int res = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(dbNameCofig);
            sql = "update vip_sub_info set last_warning_time = sysdate where vip_sub_info_id = ? ";
            ps = connection.prepareStatement(sql);
            logger.info("Start updateWarningVipSubInfo vipSubInfoId " + vipSubInfoId);
            ps.setLong(1, vipSubInfoId);
            res = ps.executeUpdate();
            logger.info("End updateWarningVipSubInfo vipSubInfoId " + vipSubInfoId + " time " + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR vipSubInfoId vipSubInfoId " + vipSubInfoId);
            logger.error(br + ex.toString());
            res = 0;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return res;
        }
    }

    public long updateWarningFinish(Long vipSubInfoId) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int res = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(dbNameCofig);
            sql = "update vip_sub_process_log set status_warn = 1 where log_time > trunc(sysdate) and vip_sub_info_id = ? ";
            ps = connection.prepareStatement(sql);
            logger.info("Start updateWarningFinish vipSubInfoId " + vipSubInfoId);
            ps.setLong(1, vipSubInfoId);
            res = ps.executeUpdate();
            logger.info("End updateWarningFinish vipSubInfoId " + vipSubInfoId + " time " + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR updateWarningFinish vipSubInfoId " + vipSubInfoId);
            logger.error(br + ex.toString());
            res = 0;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return res;
        }
    }

    public long updateWarningCheckSubStatus(Long vipSubInfoId) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int res = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(dbNameCofig);
            sql = "update vip_sub_info set check_sub = 1 where vip_sub_info_id = ? ";
            ps = connection.prepareStatement(sql);
            logger.info("Start updateWarningCheckSubStatus vipSubInfoId " + vipSubInfoId);
            ps.setLong(1, vipSubInfoId);
            res = ps.executeUpdate();
            logger.info("End updateWarningCheckSubStatus vipSubInfoId " + vipSubInfoId + " time " + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR updateWarningFinish vipSubInfoId " + vipSubInfoId);
            logger.error(br + ex.toString());
            res = 0;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return res;
        }
    }

    public String getCustName(Long vipSubInfoId) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        ResultSet rs = null;
        String result = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(dbNameCofig);
            sql = "select cust_name from vip_sub_info where vip_sub_info_id = ?";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, vipSubInfoId);
            rs = ps.executeQuery();
            while (rs.next()) {
                String resultCheck = rs.getString("cust_name");
                if (resultCheck != null && resultCheck.trim().length() > 0) {
                    result = resultCheck;
                    break;
                }
            }
            logger.info("End getCustName vipSubInfoId " + vipSubInfoId + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR getCustName: ").
                    append(sql).append("\n")
                    .append(" vipSubInfoId ")
                    .append(vipSubInfoId)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            closeResultSet(rs);
            return result;
        }
    }

    public String[] getCustInfo(Long vipSubInfoId) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        ResultSet rs = null;
        String[] result = new String[2];
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(dbNameCofig);
            sql = "select cust_name,cust_tel from vip_sub_info where vip_sub_info_id = ?";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, vipSubInfoId);
            rs = ps.executeQuery();
            while (rs.next()) {
                result[0] = rs.getString("cust_name");
                result[1] = rs.getString("cust_tel");
                break;
            }
            logger.info("End getCustInfo vipSubInfoId " + vipSubInfoId + " result " + result[0] + "-" + result[1] + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR getCustInfo: ").
                    append(sql).append("\n")
                    .append(" vipSubInfoId ")
                    .append(vipSubInfoId);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            closeResultSet(rs);
            return result;
        }
    }

    public int getTotalSub(Long vipSubInfoId) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        ResultSet rs = null;
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(dbNameCofig);
            sql = "select count(*) totalsub from vip_sub_process_log where log_time > trunc(sysdate) and vip_sub_info_id = ? and status_warn is null";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, vipSubInfoId);
            rs = ps.executeQuery();
            while (rs.next()) {
                Integer resultCheck = rs.getInt("totalsub");
                if (resultCheck > 0) {
                    result = resultCheck;
                    break;
                }
            }
            logger.info("End getTotalSub vipSubInfoId " + vipSubInfoId + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR getTotalSub: ").
                    append(sql).append("\n")
                    .append(" vipSubInfoId ")
                    .append(vipSubInfoId)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            closeResultSet(rs);
            return result;
        }
    }

    public int getTotalSuccess(Long vipSubInfoId) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        ResultSet rs = null;
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(dbNameCofig);
            sql = "select count(*) totalsub from vip_sub_process_log where log_time > trunc(sysdate) and result_code = '0' and vip_sub_info_id = ? and status_warn is null";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, vipSubInfoId);
            rs = ps.executeQuery();
            while (rs.next()) {
                Integer resultCheck = rs.getInt("totalsub");
                if (resultCheck > 0) {
                    result = resultCheck;
                    break;
                }
            }
            logger.info("End getTotalSuccess vipSubInfoId " + vipSubInfoId + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR getTotalSuccess: ").
                    append(sql).append("\n")
                    .append(" vipSubInfoId ")
                    .append(vipSubInfoId)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            closeResultSet(rs);
            return result;
        }
    }

    public int getTotalFail(Long vipSubInfoId) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        ResultSet rs = null;
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(dbNameCofig);
            sql = "select count(*) totalsub from vip_sub_process_log where log_time > trunc(sysdate) and result_code <> '0' and vip_sub_info_id = ? and status_warn is null";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, vipSubInfoId);
            rs = ps.executeQuery();
            while (rs.next()) {
                Integer resultCheck = rs.getInt("totalsub");
                if (resultCheck > 0) {
                    result = resultCheck;
                    break;
                }
            }
            logger.info("End getTotalFail vipSubInfoId " + vipSubInfoId + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR getTotalFail: ").
                    append(sql).append("\n")
                    .append(" vipSubInfoId ")
                    .append(vipSubInfoId)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            closeResultSet(rs);
            return result;
        }
    }

    public String getListFail(Long vipSubInfoId) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        ResultSet rs = null;
        StringBuilder result = new StringBuilder();
        result.setLength(0);
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(dbNameCofig);
            sql = "select isdn from vip_sub_process_log where log_time > trunc(sysdate) and result_code <> '0' and vip_sub_info_id = ? and status_warn is null";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, vipSubInfoId);
            rs = ps.executeQuery();
            while (rs.next()) {
                String resultCheck = rs.getString("isdn");
                if (resultCheck != null && resultCheck.length() > 0) {
                    result.append(resultCheck);
                    result.append(";");
                }
            }
            logger.info("End getListFail vipSubInfoId " + vipSubInfoId + " result " + result.toString() + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR getListFail: ").
                    append(sql).append("\n")
                    .append(" vipSubInfoId ")
                    .append(vipSubInfoId)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            closeResultSet(rs);
            return result.toString();
        }
    }

    public String[] getResultCheckSubs(Long vipSubInfoId) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        ResultSet rs = null;
        String[] arrRs = new String[5];
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(dbNameCofig);
            sql = "	select \n"
                    + "(select count(1)  from vip_sub_detail where vip_sub_info_id =? and status <> 0)total_subs,\n"
                    + "(select count(1)  from vip_sub_detail where vip_sub_info_id =? and status <> 0 and sub_status =0)total_valid,\n"
                    + "(select count(1)  from vip_sub_detail where vip_sub_info_id =? and status <> 0 and sub_status <> 0)total_invalid,\n"
                    + "(select cust_name from  vip_sub_info where vip_sub_info_id =?) cust_name,\n"
                    + "(select create_user from  vip_sub_info where vip_sub_info_id =?) create_user\n"
                    + "from dual";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, vipSubInfoId);
            ps.setLong(2, vipSubInfoId);
            ps.setLong(3, vipSubInfoId);
            ps.setLong(4, vipSubInfoId);
            ps.setLong(5, vipSubInfoId);
            rs = ps.executeQuery();
            while (rs.next()) {
                arrRs[0] = rs.getString("total_subs");
                arrRs[1] = rs.getString("total_valid");
                arrRs[2] = rs.getString("total_invalid");
                arrRs[3] = rs.getString("cust_name");
                arrRs[4] = rs.getString("create_user");
            }
            logger.info("End getResultCheckSubs vipSubInfoId " + vipSubInfoId + " total_subs " + arrRs[0] + " total_valid " + arrRs[1]
                    + " total_invalid " + arrRs[2] + " cust_name " + arrRs[3] + " create_user " + arrRs[4]
                    + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR getTotalSuccess: ").
                    append(sql).append("\n")
                    .append(" vipSubInfoId ")
                    .append(vipSubInfoId);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            closeResultSet(rs);
            return arrRs;
        }
    }

    public String getContactOfStaff(String staffCode) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        ResultSet rs = null;
        long startTime = System.currentTimeMillis();
        String result = "";
        try {
            connection = getConnection("dbsm");
            sql = "select cellphone from vsa_v3.users where lower(user_name) =? and status =1 ";
            ps = connection.prepareStatement(sql);
            ps.setString(1, staffCode.trim().toLowerCase());
            rs = ps.executeQuery();
            while (rs.next()) {
                result = rs.getString("cellphone");
                break;
            }
            logger.info("End getContactOfStaff staff code  " + staffCode + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR getContactOfStaff: ").
                    append(sql).append("\n")
                    .append(" staffCode ")
                    .append(staffCode)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            closeResultSet(rs);
            return result;
        }
    }
}
