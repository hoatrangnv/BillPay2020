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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.List;
import java.util.ResourceBundle;
import org.apache.log4j.Logger;
import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 *
 * Thong tin phien ban
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class DbAddPromotionSubOnairBTS extends DbProcessorAbstract {

    private String loggerLabel = DbAddPromotionSubOnairBTS.class.getSimpleName() + ": ";
    private PoolStore poolStore;
    private String dbNameCofig;
    private String DB_CM_PRE;

    public DbAddPromotionSubOnairBTS() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
        dbNameCofig = ResourceBundle.getBundle("configPayBonus").getString("dbNameConfig");
        poolStore = new PoolStore(dbNameCofig, logger);
        DB_CM_PRE ="cm_pre";
    }

    public DbAddPromotionSubOnairBTS(String sessionName, Logger logger) throws SQLException, Exception {
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
        BonusSubsBTSOnair record = new BonusSubsBTSOnair();
        long timeSt = System.currentTimeMillis();
        try {
            record.setId(rs.getLong("id"));
            record.setIsdn(rs.getString("isdn"));
            record.setCreateTime(rs.getDate("create_time"));
            record.setBtsAtt(rs.getString("bts_att"));
            record.setStatus(rs.getInt("status"));
        } catch (Exception ex) {
            logger.error("ERROR parse BonusSubsBTSOnair");
            logger.error(AppManager.logException(timeSt, ex));
        }
        return record;
    }

    public int insertPromotionOnairBTSHis(BonusSubsBTSOnair bn) {
        Connection connection = null;
        PreparedStatement ps = null;
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(dbNameCofig);
            sql = "INSERT INTO promotion_onair_subs_his (id,isdn,create_time,status,result_code,mb_data_added,description,date_process"
                    + ",bts_att,count_process,node_name,cluster_name,duration)"
                    + "values (?,?,?,1,?,?,?,sysdate,?,?,?,?,?)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, bn.getId());
            ps.setString(2, bn.getIsdn());
            ps.setDate(3, bn.getCreateTime());
            ps.setString(4, bn.getResultCode());
            ps.setLong(5, bn.getMbDataAdded());
            ps.setString(6, bn.getDescription());
            ps.setString(7, bn.getBtsAtt());
            ps.setLong(8, bn.getCountProcess()!=null?bn.getCountProcess():0);
            ps.setString(9, bn.getNodeName());
            ps.setString(10, bn.getClusterName());
            ps.setLong(11, bn.getDuration());
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


    public int deletePromotionOnairBT(BonusSubsBTSOnair bn) {
        Connection connection = null;
        PreparedStatement ps = null;
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(dbNameCofig);
            sql = "delete promotion_onair_subs where id =?";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, bn.getId());
            result = ps.executeUpdate();
            logger.info("End deleteBonusSecond id " + bn.getId() + " isdn " + bn.getIsdn() + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR deleteBonusSecond: ").
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
    
    
    public int updatePromotionOnair(BonusSubsBTSOnair bn) {
        Connection connection = null;
        PreparedStatement ps = null;
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(dbNameCofig);
            sql = "update promotion_onair_subs set status =1 where id =?";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, bn.getId());
            result = ps.executeUpdate();
            logger.info("End updatePromotionOnair id " + bn.getId() + " isdn " + bn.getIsdn() + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR updatePromotionOnair: ").
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
    
    
    public boolean checkTheFirstTimesReceivePromotion(BonusSubsBTSOnair bn) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        String sql = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(dbNameCofig);
            sql = "select * from promotion_onair_subs_his where isdn =? and result_code = 0 and mb_data_added > 0 ";
            ps = connection.prepareStatement(sql);
            ps.setString(1, bn.getIsdn());
            rs = ps.executeQuery();
            while (rs.next()){
                logger.info("End checkTheFirstTimeReceivePromotion  " + bn.getId() + " isdn " + bn.getIsdn() + " result " +  Boolean.FALSE + " time "
                    + (System.currentTimeMillis() - startTime));
                return Boolean.FALSE;
            }
            
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR checkTheFirstTimesReceivePromotion: ").
                    append(sql).append("\n")
                    .append(" id ")
                    .append(bn.getId())
                    .append(" isdn ")
                    .append(bn.getIsdn())
                    .append(" result ")
                    .append( Boolean.FALSE);
            logger.error(br + ex.toString());
            return Boolean.FALSE;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
        }
        return Boolean.TRUE;
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
