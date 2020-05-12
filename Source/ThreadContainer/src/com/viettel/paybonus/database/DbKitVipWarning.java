/*
 * Copyright 2012 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.paybonus.database;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.threadfw.manager.AppManager;
import com.viettel.paybonus.obj.*;
import com.viettel.threadfw.database.DbProcessorAbstract;
import static com.viettel.threadfw.database.DbProcessorAbstract.QUERY_TIMEOUT;
import com.viettel.vas.util.ConnectionPoolManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.List;
import org.apache.log4j.Logger;
import java.sql.Connection;
import com.viettel.vas.util.PoolStore;
import com.viettel.vas.util.obj.Param;
import com.viettel.vas.util.obj.ParamList;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashMap;

/**
 *
 * Thong tin phien ban
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class DbKitVipWarning extends DbProcessorAbstract {

//    private Logger logger;
    private String loggerLabel = DbKitVipWarning.class.getSimpleName() + ": ";
    private String dbNameCofig;
    private PoolStore poolStore;
//    private String callChangeCommAccount = "{call sm.change_comm_account(?,?,?,?,?,?,?,?,?,?,?)}";
    private String sqlDeleteMo = "update kit_vip_warning set last_warning_time = sysdate where isdn = ? and effect_date = (select max(a1.effect_date) from kit_vip_warning a1 where a1.isdn = ?)";

    public DbKitVipWarning() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
        dbNameCofig = "dbElite";
        poolStore = new PoolStore(dbNameCofig, logger);
    }

    public DbKitVipWarning(String sessionName, Logger logger) throws SQLException, Exception {
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
        KitWarning record = new KitWarning();
        long timeSt = System.currentTimeMillis();
        try {
            record.setIsdn(rs.getString("ISDN"));
            record.setPpCode(rs.getString("PRODUCT_CODE"));
            record.setEffectDate(rs.getString("EFFECT_DATE"));
            record.setProductCode(rs.getString("PRODUCT_CODE"));
        } catch (Exception ex) {
            logger.error("ERROR parse KitWarning");
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
            connection = ConnectionPoolManager.getConnection("dbElite");
            ps = connection.prepareStatement(sqlDeleteMo);
            for (Record rc : listRecords) {
                KitWarning sd = (KitWarning) rc;
                //if(checkAlreayProcessed(sd.getIsdn(), sd.getPpCode())){
                //continue;
                //}
                batchId = sd.getBatchId();
                ps.setString(1, sd.getIsdn());
                ps.setString(2, sd.getIsdn());
                ps.addBatch();
            }
            return ps.executeBatch();
        } catch (Exception ex) {
            logger.error("ERROR elite_warning_his elite_warning_his batchid " + batchId, ex);
            logger.error(AppManager.logException(timeStart, ex));
            return null;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to elite_warning_his elite_warning_his, batchid " + batchId, timeStart);
        }
    }

    @Override
    public int[] insertQueueHis(List<Record> listRecords) {
        List<ParamList> listParam = new ArrayList<ParamList>();
        String batchId = "";
        long timeSt = System.currentTimeMillis();
        try {
            for (Record rc : listRecords) {
                KitWarning sd = (KitWarning) rc;
                //if(checkAlreayProcessed(sd.getIsdn(), sd.getPpCode())){
                //continue;
                //}
                batchId = sd.getBatchId();
                ParamList paramList = new ParamList();
                paramList.add(new Param("id", "elite_warning_his_seq.nextval", Param.DataType.CONST, Param.IN));
                paramList.add(new Param("ISDN", sd.getIsdn(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("price_plan_code", sd.getPpCode(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("product_code", sd.getPpCode(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("status", 1, Param.DataType.INT, Param.IN));
                paramList.add(new Param("process_time", "sysdate", Param.DataType.CONST, Param.IN));
                paramList.add(new Param("effect_date", sd.getEffectDate(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("sms", sd.getSms(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("description", sd.getDescription(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("result_code", sd.getResultCode(), Param.DataType.STRING, Param.IN));
                listParam.add(paramList);
            }
            int[] res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "elite_warning_his");
            logTimeDb("Time to insertQueueHis elite_warning_his, batchid " + batchId + " total result: " + res.length, timeSt);
            return res;
        } catch (Exception ex) {
            logger.error("ERROR insertQueueHis elite_warning_his batchid " + batchId, ex);
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
                sb.append(": ").append(sd);
            }
            logger.warn("Dispatcher not get reponse from agent, so processTimeoutRecord ID " + sb.toString());
        } catch (Exception ex) {
            logger.error("ERROR processTimeoutRecord ID " + sb.toString() + " " + ex.toString());
        }
    }

    @Override
    public void updateSqlMoParam(List<Record> lrc) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public int[] deleteQueueTimeout(List<String> listId) {
//        long timeStart = System.currentTimeMillis();
//        PreparedStatement ps = null;
//        Connection connection = null;
//        StringBuilder sf = new StringBuilder();
//        try {
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
//            logger.error("ERROR deleteQueueTimeout SUB_PROFILE_INFO listId " + sf.toString(), ex);
//            logger.error(AppManager.logException(timeStart, ex));
//            return null;
//        } finally {
//            closeStatement(ps);
//            closeConnection(connection);
//            logTimeDb("Time to deleteQueueTimeout SUB_PROFILE_INFO, listId " + sf.toString(), timeStart);
//        }
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
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public Long getPriceProduct(String productCode) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        Long price = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "select * from product.product_connect_kit where upper(product_code) = upper(?)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, productCode);
            rs = ps.executeQuery();
            while (rs.next()) {
                price = rs.getLong("money_fee");;
                break;
            }
            logger.info("End getPriceProductConnectKit productCode " + productCode + ", price: " + price + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            ex.printStackTrace();
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR getPriceProductConnectKit: ").
                    append(sql).append("\n")
                    .append(" productCode ")
                    .append(productCode);
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return price;
        }
    }

    public String getProductName(String productCode) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        String productName = "";
        StringBuilder br = new StringBuilder();
        String sql = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "select * from product.product_connect_kit where status = 1 and upper(product_code) = upper(?)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, productCode);
            rs = ps.executeQuery();
            while (rs.next()) {
                productName = rs.getString("product_name_pt");;
                break;
            }
            logger.info("End getProductName productCode " + productCode + ", result: " + productName + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            ex.printStackTrace();
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR getProductName: ").
                    append(sql).append("\n")
                    .append(" productCode ")
                    .append(productCode);
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return productName;
        }
    }

    public String getProductCode(String isdn) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        String productCode = null;
        String sqlMo = "select * from sub_mb where isdn = ? and status = 2";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection("cm_pre");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setString(1, isdn);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                productCode = rs1.getString("product_code");
            }
            if (productCode == null) {
                productCode = "";
                logger.info("productCode is null - isdn: " + isdn);
            }
            logTimeDb("Time to getProductCode, productCode: " + productCode + " isdn " + isdn, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getProductCode, isdn " + isdn);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
            return productCode;
        }
    }
}
