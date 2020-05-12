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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.List;
import java.util.ResourceBundle;
import org.apache.log4j.Logger;

/**
 *
 * Thong tin phien ban
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class DbRetryConnectKit extends DbProcessorAbstract {

    private String loggerLabel = DbRetryConnectKit.class.getSimpleName() + ": ";
    private PoolStore poolStore;
    private String dbNameCofig;
    private String DB_CM_PRE;

    public DbRetryConnectKit() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
        dbNameCofig = ResourceBundle.getBundle("configPayBonus").getString("dbNameConfig");
        poolStore = new PoolStore(dbNameCofig, logger);
        DB_CM_PRE = "cm_pre";
    }

    public DbRetryConnectKit(String sessionName, Logger logger) throws SQLException, Exception {
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
//count_process,node_name,cluster_name,duration,result_code,description,retry

    @Override
    public Record parse(ResultSet rs) {
        KitRetry record = new KitRetry();
        long timeSt = System.currentTimeMillis();
        try {
            record.setActionAuditId(rs.getLong("action_audit_id"));
            record.setIsdn(rs.getString("isdn"));
            record.setSerial(rs.getString("sim_serial"));
            record.setProcessDate(rs.getDate("date_process"));
        } catch (Exception ex) {
            logger.error("ERROR parse Kit");
            logger.error(AppManager.logException(timeSt, ex));
        }
        return record;
    }

    public int insertRetryConnectKitHis(KitRetry bn) {
        Connection connection = null;
        PreparedStatement ps = null;
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(dbNameCofig);
            sql = "INSERT INTO retry_connect_kit_his (action_audit_id,isdn,sim_serial,imsi_bccs,imsi_ocs,imsi_hrl,import_date,result_code,"
                    + "description,node_name,cluster_name,duration,process_date,command_1,command_1_result,command_2,command_2_result,command_3,command_3_result,case)"
                    + "values (?,?,?,?,?,?,?,?,?,?,?,?,sysdate,?,?,?,?,?,?,?)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, bn.getActionAuditId());
            ps.setString(2, bn.getIsdn());
            ps.setString(3, bn.getSerial());
            ps.setString(4, bn.getImsiBCCS());
            ps.setString(5, bn.getImsiOCS());
            ps.setString(6, bn.getImsiHLR());
            ps.setDate(7, bn.getProcessDate());
            ps.setString(8, bn.getResultCode());
            ps.setString(9, bn.getDescription());
            ps.setString(10, bn.getNodeName());
            ps.setString(11, bn.getClusterName());
            ps.setLong(12, bn.getDuration());
            ps.setString(13, bn.getCmd1());
            ps.setString(14, bn.getCmd1_result());
            ps.setString(15, bn.getCmd2());
            ps.setString(16, bn.getCmd2_result());
            ps.setString(17, bn.getCmd3());
            ps.setString(18, bn.getCmd3_result());
            ps.setString(19, bn.getCaseProcess());
            result = ps.executeUpdate();
            logger.info("End insertBonusSecondHis id " + bn.getId() + " isdn " + bn.getIsdn() + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertBonusSecondHis: ").
                    append(sql).append("\n")
                    .append(" id ")
                    .append(bn.getId())
                    .append(" isdn ")
                    .append(bn.getIsdn())
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
            return result;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
        }
        return result;
    }

    public int deleteRetryConnectKit(KitRetry bn) {
        Connection connection = null;
        PreparedStatement ps = null;
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(dbNameCofig);
            sql = "delete retry_connect_kit where action_audit_id =?";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, bn.getActionAuditId());
            result = ps.executeUpdate();
            logger.info("End deleteRetryConnectKit id " + bn.getId() + " isdn " + bn.getIsdn() + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR deleteRetryConnectKit: ").
                    append(sql).append("\n")
                    .append(" action_audit_id ")
                    .append(bn.getId())
                    .append(" isdn ")
                    .append(bn.getIsdn())
                    .append(" serial ")
                    .append(bn.getSerial())
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
            return result;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
        }
        return result;
    }

    public int updateRetryConnectKit(KitRetry bn) {
        Connection connection = null;
        PreparedStatement ps = null;
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(dbNameCofig);
            sql = "update retry_connect_kit set status =1 where action_audit_id =?";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, bn.getId());
            result = ps.executeUpdate();
            logger.info("End updateRetryConnectKit id " + bn.getId() + " isdn " + bn.getIsdn() + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR updateRetryConnectKit: ").
                    append(sql).append("\n")
                    .append(" action_audit_id ")
                    .append(bn.getId())
                    .append(" isdn ")
                    .append(bn.getIsdn())
                    .append(" serial ")
                    .append(bn.getSerial())
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
            return result;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
        }
        return result;
    }

    public String getImsiOnBCCS(String isdn) {
        Connection connection = null;
        PreparedStatement ps = null;
        String sql = "";
        ResultSet rs;
        String imsi = "";
        try {
            connection = getConnection(DB_CM_PRE);
            sql = "select imsi from sub_mb where isdn =? and status =2";
            ps = connection.prepareStatement(sql);
            ps.setString(1, isdn);
            rs = ps.executeQuery();
            while (rs.next()) {
                imsi = rs.getString("imsi");
            }
            return imsi;
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR getImsiBCCS: ").
                    append(sql).append("\n")
                    .append(" isdn ")
                    .append(isdn);

            logger.error(br + ex.toString());
            return "";
        } finally {
            closeStatement(ps);
            closeConnection(connection);
        }
    }

    public String getEKIbyImsi(String imsi) {
        Connection connection = null;
        PreparedStatement ps = null;
        String sql = "";
        ResultSet rs;
        String EKI = "";
        try {
            connection = getConnection(DB_CM_PRE);
            sql = "select EKI from sm.stock_sim where imsi = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, imsi);
            rs = ps.executeQuery();
            while (rs.next()) {
                EKI = rs.getString("EKI");
            }
            return EKI;
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR getEKIbyImsi: ").
                    append(sql).append("\n")
                    .append(" EKI ")
                    .append(EKI);

            logger.error(br + ex.toString());
            return "";
        } finally {
            closeStatement(ps);
            closeConnection(connection);
        }
    }

    public String getTPLID(String isdn, boolean isSim4G) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String tplId = null;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(DB_CM_PRE);
            String sql = "select * from tplid_connect_kit where prefix = substr(?,0,4)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, isdn);
            rs = ps.executeQuery();
            while (rs.next()) {
                if (isSim4G) {
                    tplId = rs.getString("tplid_4g");
                } else {
                    tplId = rs.getString("tplid");
                }

                logger.info("tplid " + tplId + " isdn " + isdn);
                break;
            }
            logger.info("End getTPLID: isdn" + isdn + " time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getTPLID isdn").append(isdn).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString() + " isdn " + isdn);
            tplId = null;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return tplId;
        }
    }

    @Override
    public void updateSqlMoParam(List<Record> lrc) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public int[] deleteQueueTimeout(List<String> listId) {
        return new int[0];
    }

    @Override
    public int[] deleteQueue(List<Record> listRecords) {
        return new int[0];
    }

    @Override
    public int[] insertQueueHis(List<Record> listRecords) {
        return new int[0];
    }

    @Override
    public int[] insertQueueOutput(List<Record> listRecords) {
        return new int[0];
    }

    @Override
    public int[] updateQueueInput(List<Record> listRecords) {
        return new int[0];
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

}
