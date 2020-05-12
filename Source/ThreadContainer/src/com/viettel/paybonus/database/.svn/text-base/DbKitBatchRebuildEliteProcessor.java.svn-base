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
import java.sql.PreparedStatement;
import java.util.ArrayList;

/**
 *
 * Thong tin phien ban
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class DbKitBatchRebuildEliteProcessor extends DbProcessorAbstract {

//    private Logger logger;
    private String loggerLabel = DbKitBatchRebuildEliteProcessor.class.getSimpleName() + ": ";
    private String dbNameCofig;
//    private PoolStore poolStore;
    private String sqlDeleteMo = "update kit_batch_info set expire_time_group = to_char(to_date(expire_time_group,'yyyyMMddhh24miss') + 1,'yyyyMMddhh24miss'),\n"
            + "count_process = (select nvl(count_process,0) + 1 from kit_batch_info where kit_batch_id = ?) where kit_batch_id = ?";

    public DbKitBatchRebuildEliteProcessor() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
        dbNameCofig = "cm_pre";
//        poolStore = new PoolStore(dbNameCofig, logger);
    }

    public DbKitBatchRebuildEliteProcessor(String sessionName, Logger logger) throws SQLException, Exception {
        this.logger = logger;
        dbNameCofig = sessionName;
//        poolStore = new PoolStore(sessionName, logger);
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
        KitBatchInfo record = new KitBatchInfo();
        long timeSt = System.currentTimeMillis();
        try {
            record.setKitBatchId(rs.getLong("kit_batch_id"));
            record.setCreateUser(rs.getString("create_user"));
            record.setUnitCode(rs.getString("unit_code"));
            record.setCustId(rs.getLong("cust_id"));
            record.setCreateTime(rs.getTimestamp("create_time"));
            record.setPayType(rs.getString("pay_type"));
            record.setBankName(rs.getString("bank_name"));
            record.setBankTransCode(rs.getString("bank_tran_code"));
            record.setBankTransAmount(rs.getString("bank_tran_amount"));
            record.setEmolaAccount(rs.getString("emola_account"));
            record.setEmolaVoucherCode(rs.getString("emola_voucher_code"));
            record.setAddMonth(rs.getString("add_month"));
            record.setChannelType(rs.getString("channel_type"));
            record.setExpireTimeGroup(rs.getString("expire_time_group"));
            record.setExtendFromKitBatchId(rs.getLong("extend_from_kit_batch_id"));
            record.setResultCode("0");
            record.setDescription("Start Processing");
        } catch (Exception ex) {
            logger.error("ERROR parse SubProfileInfo");
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
                KitBatchInfo sd = (KitBatchInfo) rc;
                batchId = sd.getBatchId();
                ps.setLong(1, sd.getKitBatchId());
                ps.setLong(2, sd.getKitBatchId());
                ps.addBatch();
            }
            return ps.executeBatch();
        } catch (Exception ex) {
            logger.error("ERROR updateQueue KIT_BATCH_INFO batchid " + batchId, ex);
            logger.error(AppManager.logException(timeStart, ex));
            return null;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to updateQueue KIT_BATCH_INFO, batchid " + batchId, timeStart);
        }
    }

    @Override
    public int[] insertQueueHis(List<Record> listRecords) {
        logger.info("KitBatchConnect No need to insertQueueHis ");
        int[] res = new int[0];
        return res;
    }

    @Override
    public int[] insertQueueOutput(List<Record> listRecords) {
        logger.info("KitBatchConnect No need to insertQueueOutput");
        int[] res = new int[0];
        return res;
    }

    @Override
    public int[] updateQueueInput(List<Record> listRecords) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateSqlMoParam(List<Record> lrc) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void processTimeoutRecord(List<String> ids) {
        StringBuilder sb = new StringBuilder();
        try {
            deleteQueueTimeout(ids);
            logger.warn("Dispatcher not get reponse from agent, so processTimeoutRecord ID " + sb.toString());
        } catch (Exception ex) {
            logger.error("ERROR processTimeoutRecord ID " + sb.toString());
        }
    }

    public int[] deleteQueueTimeout(List<String> listId) {
        long timeStart = System.currentTimeMillis();
        PreparedStatement ps = null;
        Connection connection = null;
        StringBuilder sf = new StringBuilder();
        try {
            connection = ConnectionPoolManager.getConnection(dbNameCofig);
            ps = connection.prepareStatement(sqlDeleteMo);
            sf.setLength(0);
            for (String id : listId) {
                ps.setLong(1, Long.valueOf(id));
                ps.setLong(2, Long.valueOf(id));
                ps.addBatch();
                sf.append(id);
                sf.append(", ");
            }
            return ps.executeBatch();
        } catch (Exception ex) {
            logger.error("ERROR deleteQueueTimeout KitBatchConnect listId " + sf.toString(), ex);
            logger.error(AppManager.logException(timeStart, ex));
            return null;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to deleteQueueTimeout KitBatchConnect, listId " + sf.toString(), timeStart);
        }
    }

    public List<KitBatch> getListKitBatchDetail(Long kitBatchId) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        List<KitBatch> lstKitBatch = new ArrayList<KitBatch>();
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            String sql = "select * from kit_batch_detail where kit_batch_id in (select kit_batch_id from kit_batch_info "
                    + "where kit_batch_id = ? or extend_from_kit_batch_id = ? and result_code = '0') and (product_code in \n"
                    + " (select product_code from product.product_connect_kit where vip_product = 1 and status = 1) "
                    + "or product_code in (select vas_code from product.product_add_on where status = 1)) and result_code = '0'";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, kitBatchId);
            ps.setLong(2, kitBatchId);
            rs = ps.executeQuery();
            while (rs.next()) {
                String serial = rs.getString("serial");
                String isdn = rs.getString("isdn");
                String productCode = rs.getString("product_code");
                String stateOfRecord = rs.getString("state_of_record");
                KitBatch kitBatch = new KitBatch();
                kitBatch.setSerial(serial);
                kitBatch.setKitBatchId(kitBatchId);
                kitBatch.setIsdn(isdn);
                kitBatch.setProductCode(productCode);
                kitBatch.setStateOfRecord(stateOfRecord);
                kitBatch.setCustId(rs.getLong("cust_id"));
                lstKitBatch.add(kitBatch);

            }
            logger.info("End getListKitBatchDetail: kitBatchId" + kitBatchId + " time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getListKitBatchDetail ").
                    append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return lstKitBatch;
        }
    }

    public List<KitBatch> getListKitBatchExtend(Long kitBatchId) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        List<KitBatch> lstKitBatch = new ArrayList<KitBatch>();
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            String sql = "select * from kit_batch_extend where kit_batch_id = ? and result_code = '0'";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, kitBatchId);
            rs = ps.executeQuery();
            while (rs.next()) {
                String isdn = rs.getString("isdn");
                KitBatch kitBatch = new KitBatch();
                kitBatch.setKitBatchId(kitBatchId);
                kitBatch.setIsdn(isdn);
                lstKitBatch.add(kitBatch);

            }
            logger.info("End getListKitBatchExtend: kitBatchId" + kitBatchId + " time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getListKitBatchExtend ").
                    append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return lstKitBatch;
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
            logTimeDb("Time to getProductCode: isdn " + isdn, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getProductCode isdn " + isdn);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
            return productCode;
        }
    }

    public boolean checkVipProductConnectKit(String productCode) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        boolean result = false;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "select * from product.product_connect_kit where vip_product = 1 and status = 1 and upper(product_code) = upper(?)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, productCode);
            rs = ps.executeQuery();
            while (rs.next()) {
                Long transId = rs.getLong("product_id");
                if (transId > 0) {
                    result = true;
                    break;
                }
            }
            logger.info("End checkVipProductConnectKit productCode " + productCode
                    + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR checkVipProductConnectKit: ").
                    append(sql).append("\n")
                    .append(" productCode ")
                    .append(productCode)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeResultSet(rs);
            closeConnection(connection);
            return result;
        }
    }

    public int insertKitBatchRebuildHis(Long kitBatchId, String isdn, String productCode, String pricePlanBonus, String oldExpireTime,
            String newExpireTime, String resultRmPrice, String resultAddPrice, String resultCode, String description, Long actionAuditId) {

        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "insert into kit_batch_rebuild_his (kit_batch_id, isdn, product_code, price_plan_bonus, old_expire_time, new_expire_time, \n"
                    + "result_rm_price, result_add_price, result_code, description, process_time, action_audit_id) \n"
                    + "values (?,?,?,?,?,?,?,?,?,?,sysdate,?)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, kitBatchId);
            ps.setString(2, isdn);
            ps.setString(3, productCode);
            ps.setString(4, pricePlanBonus != null ? pricePlanBonus : "");
            ps.setString(5, oldExpireTime != null ? oldExpireTime : "");
            ps.setString(6, newExpireTime != null ? newExpireTime : "");
            ps.setString(7, resultRmPrice != null ? resultRmPrice : "");
            ps.setString(8, resultAddPrice != null ? resultAddPrice : "");
            ps.setString(9, resultCode != null ? resultCode : "");
            ps.setString(10, description != null ? description : "");
            ps.setString(11, actionAuditId > 0 ? String.valueOf(actionAuditId) : "");

            result = ps.executeUpdate();
            logger.info("End insertKitBatchRebuildHis kitBatchId " + kitBatchId + " isdn: " + isdn + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertKitBatchRebuildHis: ").
                    append(sql).append("\n")
                    .append(" kitBatchId ")
                    .append(kitBatchId)
                    .append(" isdn ")
                    .append(isdn)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
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

    public int insertActionAudit(Long actionAuditId, Long kitBatchId, String des) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "INSERT INTO action_audit (ACTION_AUDIT_ID,ISSUE_DATETIME,ACTION_CODE,REASON_ID,SHOP_CODE,USER_NAME,"
                    + " PK_TYPE,PK_ID,IP,DESCRIPTION) "
                    + " VALUES(?,sysdate,'','','PROCESS','PROCESS', "
                    + "'4',?,'127.0.0.1',?)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, actionAuditId);
            ps.setLong(2, kitBatchId);
            ps.setString(3, des);
            result = ps.executeUpdate();
            logger.info("End insertActionAudit actionAuditId " + actionAuditId
                    + " kitBatchId " + kitBatchId + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertActionAudit: ").
                    append(sql).append("\n")
                    .append(" actionAuditId ")
                    .append(actionAuditId)
                    .append(" kitBatchId ")
                    .append(kitBatchId)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int updateExpireTimeGroup(Long kitBatchId, String expireTimeGroup) {
        Connection connection = null;
        PreparedStatement ps = null;
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "update cm_pre.kit_batch_info set expire_time_group = ?, count_process = -1 where kit_batch_id = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, expireTimeGroup);
            ps.setLong(2, kitBatchId);
            result = ps.executeUpdate();
            logger.info("End updateExpireTimeGroup kitBatchId " + kitBatchId + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            StringBuilder brBuilder = new StringBuilder();
            brBuilder.setLength(0);
            brBuilder.append(loggerLabel).append(new Date()).
                    append("\nERROR updateExpireTimeGroup: ").
                    append(sql).append("\n")
                    .append(" kitBatchId ")
                    .append(kitBatchId)
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
}
