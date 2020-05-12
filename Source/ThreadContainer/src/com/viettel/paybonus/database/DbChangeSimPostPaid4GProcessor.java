/*
 * Copyright 2012 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.paybonus.database;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.obj.Price;
import com.viettel.paybonus.obj.RequestChangeSim;
import com.viettel.paybonus.obj.SaleServices;
import com.viettel.paybonus.obj.SaleServicesPrice;
import com.viettel.paybonus.obj.StockModel;
import com.viettel.paybonus.obj.SubInfo;
import com.viettel.threadfw.manager.AppManager;
import com.viettel.threadfw.database.DbProcessorAbstract;
import static com.viettel.threadfw.database.DbProcessorAbstract.QUERY_TIMEOUT;
import com.viettel.vas.util.PoolStore;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.List;
import java.util.ResourceBundle;
import org.apache.log4j.Logger;
import java.sql.Connection;
import com.viettel.vas.util.ConnectionPoolManager;
import java.sql.PreparedStatement;

/**
 *
 * Thong tin phien ban
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class DbChangeSimPostPaid4GProcessor extends DbProcessorAbstract {

//    private Logger logger;
    private String loggerLabel = DbChangeSimPostPaid4GProcessor.class.getSimpleName() + ": ";
//    private StringBuffer br = new StringBuffer();
    private PoolStore poolStore;
    private String dbNameCofig;
    private String sqlDeleteMo = "update cm_pos.request_changesim_4g set status = 1, date_process = sysdate,"
            + " result_code = ?, description = ?, old_serial = ?, old_imsi = ?, duration = ?, action_audit_id = ? where id = ?";

    public DbChangeSimPostPaid4GProcessor() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
        dbNameCofig = ResourceBundle.getBundle("configPayBonus").getString("dbNameConfig");
        poolStore = new PoolStore(dbNameCofig, logger);
    }

    public DbChangeSimPostPaid4GProcessor(String sessionName, Logger logger) throws SQLException, Exception {
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
        RequestChangeSim record = new RequestChangeSim();
        long timeSt = System.currentTimeMillis();
        try {
            record.setId(rs.getLong("id"));
            record.setIsdn(rs.getString("isdn"));
            record.setIdNo(rs.getString("id_no"));
            record.setStatus(rs.getString("status"));
            record.setCreateTime(rs.getTimestamp("create_time"));
            record.setStaffCode(rs.getString("staff_code"));
            record.setNewSerial(rs.getString("new_serial"));
            record.setChannelType(rs.getString("channel_type"));
            record.setVasCode(rs.getString("vas_code"));
            record.setVoucherCode(rs.getString("voucher_code"));
            record.setResultCode("0");
            record.setDescription("Start Processing");
            record.setUssd_loc(rs.getString("ussd_loc"));
//            record.setTimeCheck(rs.getTimestamp("check_time"));
        } catch (Exception ex) {
            logger.error("ERROR parse RequestChangeSim4G");
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
            connection = ConnectionPoolManager.getConnection("cm_pos");
            ps = connection.prepareStatement(sqlDeleteMo);
            for (Record rc : listRecords) {
                RequestChangeSim sd = (RequestChangeSim) rc;
                batchId = sd.getBatchId();
                ps.setString(1, sd.getResultCode());
                ps.setString(2, sd.getDescription());
                ps.setString(3, sd.getOldSerial());
                ps.setString(4, sd.getOldImsi());
                ps.setLong(5, sd.getDuration());
                ps.setLong(6, sd.getActionAuditId());
                ps.setLong(7, sd.getId());
                ps.addBatch();
            }
            return ps.executeBatch();
        } catch (Exception ex) {
            logger.error("ERROR deleteQueue request_changesim_4g batchid " + batchId, ex);
            logger.error(AppManager.logException(timeStart, ex));
            return null;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to deleteQueue request_changesim_4g, batchid " + batchId, timeStart);
        }
    }

    @Override
    public int[] insertQueueHis(List<Record> listRecords) {
        logger.info("ChangeSim4G No need to insertQueueHis ");
        int[] res = new int[0];
        return res;
    }

    @Override
    public int[] insertQueueOutput(List<Record> listRecords) {
        logger.info("ChangeSim4G No need to insertQueueOutput");
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
            deleteQueueTimeout(ids);
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
        long timeStart = System.currentTimeMillis();
        PreparedStatement ps = null;
        Connection connection = null;
        StringBuilder sf = new StringBuilder();
        try {
            connection = ConnectionPoolManager.getConnection(dbNameCofig);
            ps = connection.prepareStatement(sqlDeleteMo);
            sf.setLength(0);
            for (String id : listId) {
                ps.setString(1, "FW_99");
                ps.setString(2, "FW_Timeout");
                ps.setString(3, "FW_Timeout");
                ps.setString(4, "FW_Timeout");
                ps.setLong(5, 0);
                ps.setLong(6, 0);
                ps.setLong(7, Long.valueOf(id));
                ps.addBatch();
                sf.append(id);
                sf.append(", ");
            }
            return ps.executeBatch();
        } catch (Exception ex) {
            logger.error("ERROR deleteQueueTimeout RequestChangeSim4G listId " + sf.toString(), ex);
            logger.error(AppManager.logException(timeStart, ex));
            return null;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to deleteQueueTimeout RequestChangeSim4G, listId " + sf.toString(), timeStart);
        }
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

    //LinhNBV start modified on September 04 2017: Get isdn of channel
    public String getTelByStaffCode(String staffCode) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        String tel = null;
        String sqlMo = " select cellphone from vsa_v3.users where user_name = ? and status = 1";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection("dbsm");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setString(1, staffCode.toLowerCase());
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                tel = rs1.getString("cellphone");
            }
            if (tel == null) {
                tel = "";
                logger.info("tel is null - staff_code: " + staffCode);
            }
            logTimeDb("Time to getTelByStaffCode: " + staffCode, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getTelByStaffCode " + staffCode);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return tel;
    }

    public int updateStockSim(Long status, Long hlrStatus, String serial, String isdn) {
        Connection connection = null;
        PreparedStatement ps = null;

        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "update stock_sim set status = ?, hlr_status = ?, hlr_reg_date = sysdate where serial = to_number(?)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, status);
            ps.setLong(2, hlrStatus);
            ps.setString(3, serial);

            result = ps.executeUpdate();
            logger.info("End updateStockSimToWaitingConnect serial " + serial + " isdn " + isdn + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            StringBuilder brBuilder = new StringBuilder();
            brBuilder.setLength(0);
            brBuilder.append(loggerLabel).append(new Date()).
                    append("\nERROR updateStockSimToWaitingConnect: ").
                    append(sql).append("\n")
                    .append(" serial ")
                    .append(serial)
                    .append(" isdn ")
                    .append(isdn)
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

    public String[] getImsiEkiSimBySerial(String serial, String isdn) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder btBuilder = new StringBuilder();
        String[] result = new String[2];
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            String sql = "select imsi, eki from stock_sim where to_number(serial) = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, serial);
            rs = ps.executeQuery();
            while (rs.next()) {
                result[0] = rs.getString("imsi");
                result[1] = rs.getString("eki");
                logger.info("checkSerialSimIsSale " + rs.getString("imsi") + " isdn " + isdn);
            }
            logger.info("End checkSerialSimIsSale serial " + serial + " isdn " + isdn
                    + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            btBuilder.setLength(0);
            btBuilder.append(loggerLabel).append(new Date()).append("\nERROR checkSerialSimIsSale ---- serial ").append(serial).append(" Message: ").
                    append(ex.getMessage());
            logger.error(btBuilder + ex.toString());
        } finally {
            closeStatement(ps);
            closeResultSet(rs);
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

    public String getShopIdStaffIdByStaffCode(String staffCode) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        StringBuilder result = new StringBuilder();
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            String sql = "select * from staff where lower(staff_code) = lower(?) and status = 1";
            ps = connection.prepareStatement(sql);
            ps.setString(1, staffCode);
            rs = ps.executeQuery();
            while (rs.next()) {
                Long shopId = rs.getLong("shop_id");
                Long staffId = rs.getLong("staff_id");
                result = new StringBuilder();
                result.append(shopId).append("|").append(staffId);
                break;
            }
            logger.info("End getShopIdStaffIdByStaffCode: " + staffCode + " time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getShopIdStaffIdByStaffCode ").append(staffCode).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return result.toString();
        }
    }

    public String getShopCode(Long shopId) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String shopCode = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            String sql = "select * from shop where shop_id = ? and status = 1";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, shopId);
            rs = ps.executeQuery();
            while (rs.next()) {
                shopCode = rs.getString("shop_code");
            }
            logger.info("End getShopCode: " + shopCode + " time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getShopCode ").append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return shopCode;
        }
    }

    public Long getStockModelIdBySerial(String serial) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        Long stockModelId = null;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            String sql = "select * from stock_sim where serial = to_number(?)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, serial);
            rs = ps.executeQuery();
            while (rs.next()) {
                stockModelId = rs.getLong("stock_model_id");
                break;
            }
            logger.info("End getStockModelIdBySerial: serial " + serial + " time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getStockModelIdBySerial serial ").append(serial).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
            stockModelId = null;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return stockModelId;
        }
    }

    public int insertActionAudit(Long actionAuditId, String isdn, String serial, String des,
            Long subId, String shopCode, String userName) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pos");
            sql = "INSERT INTO action_audit (ACTION_AUDIT_ID,ISSUE_DATETIME,ACTION_CODE,REASON_ID,SHOP_CODE,USER_NAME,"
                    + " PK_TYPE,PK_ID,IP,DESCRIPTION) "
                    + " VALUES(?,sysdate,'11',2086,?,?, "
                    + "'3',?,'127.0.0.1',?)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, actionAuditId);
            ps.setString(2, shopCode);
            ps.setString(3, userName);
            ps.setLong(4, subId);
            ps.setString(5, des);
            result = ps.executeUpdate();
            logger.info("End insertActionAudit isdn " + isdn + " serial " + serial
                    + " subId " + subId + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertActionAudit: ").
                    append(sql).append("\n")
                    .append(" isdn ")
                    .append(isdn)
                    .append(" serial ")
                    .append(serial)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public boolean isSim4G(String serial, Long stockModelId) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        boolean result = false;
        long startTime = System.currentTimeMillis();
        long transId = 0;
        try {
            connection = getConnection("dbsm");
            sql = "select * from sm.stock_sim where serial = to_number(?) and stock_model_id = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, serial);
            ps.setLong(2, stockModelId);
            rs = ps.executeQuery();
            while (rs.next()) {
                transId = rs.getLong("stock_model_id");
                if (transId > 0) {
                    result = true;
                    break;
                }
            }
            logger.info("End isSim4G serial " + serial
                    + " result " + result + " transId " + transId + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR isSim4G: ").
                    append(sql).append("\n")
                    .append(" serial ")
                    .append(serial)
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

    public SubInfo getSubscriberInfo(String isdn) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        SubInfo subInfo = null;
        String sqlMo = "select * from sub_mb where isdn = ? and status = 2";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection("cm_pos");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setString(1, isdn);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                String actStatus = rs1.getString("act_status");
                String imsi = rs1.getString("imsi");
                String serial = rs1.getString("serial");
                Long subId = rs1.getLong("sub_id");
                String productCode = rs1.getString("product_code");

                subInfo = new SubInfo();
                subInfo.setProductCode(productCode);
                subInfo.setActStatus(actStatus);
                subInfo.setImsi(imsi);
                subInfo.setSerial(serial);
                subInfo.setSubId(subId);
            }
            logTimeDb("Time to getSubscriberInfo isdn " + isdn, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getSubscriberInfo isdn " + isdn);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
            return subInfo;
        }
    }

    /**
     * Get EKI of SIM by IMSI
     *
     * @param imsi
     * @return
     */
    public String getEKI(String imsi) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        String sql = "select eki from sm.stock_sim where imsi = ? ";
        PreparedStatement ps = null;
        try {
            connection = ConnectionPoolManager.getConnection("cm_pre");
            ps = connection.prepareStatement(sql);
            if (QUERY_TIMEOUT > 0) {
                ps.setQueryTimeout(QUERY_TIMEOUT);
            }
            ps.setString(1, imsi);
            rs1 = ps.executeQuery();
            while (rs1.next()) {
                return rs1.getString("eki");
            }
            logTimeDb("Time to getEKI isdn " + imsi, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getEKI isdn " + imsi);
            logger.error(AppManager.logException(timeSt, ex));
            return "";
        } finally {
            closeResultSet(rs1);
            closeStatement(ps);
            closeConnection(connection);
        }
        return "";
    }

    public String getTPLID(String isdn, boolean isSim4G) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String tplId = null;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
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

    public int updateSubMbChangeSim(Long subId, String isdn, String newImsi, String newSerial) {
        Connection connection = null;
        PreparedStatement ps = null;

        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pos");
            sql = "update sub_mb set imsi = ?, serial = ?, change_datetime = sysdate where sub_id = ? and status = 2 and isdn = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, newImsi);
            ps.setString(2, newSerial);
            ps.setLong(3, subId);
            ps.setString(4, isdn);

            result = ps.executeUpdate();
            logger.info("End updateSubMbChangeSim subId " + subId + " newSerial " + newSerial + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            StringBuilder brBuilder = new StringBuilder();
            brBuilder.setLength(0);
            brBuilder.append(loggerLabel).append(new Date()).
                    append("\nERROR updateSubMbChangeSim: ").
                    append(sql).append("\n")
                    .append(" subId ")
                    .append(subId)
                    .append(" newSerial ")
                    .append(newSerial)
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

    public boolean checkSubSimMb(Long subId, String oldImsi) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        boolean result = false;
        long startTime = System.currentTimeMillis();
        long transId = 0;
        try {
            connection = getConnection("cm_pos");
            sql = "select * from Sub_Sim_Mb where status = 1 AND sub_Id = ? AND imsi = ? ";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, subId);
            ps.setString(2, oldImsi);
            rs = ps.executeQuery();
            while (rs.next()) {
                transId = rs.getLong("SUB_ID");
                if (transId > 0) {
                    result = true;
                    break;
                }
            }
            logger.info("End checkSubSimMb subId " + subId
                    + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR checkSubSimMb: ").
                    append(sql).append("\n")
                    .append(" subId ")
                    .append(subId)
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

    public int updateSubSimMb(Long subId, String oldImsi) {
        Connection connection = null;
        PreparedStatement ps = null;

        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pos");
            sql = "update sub_sim_mb set end_datetime = sysdate, status = 0 where sub_Id = ? AND imsi = ?";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, subId);
            ps.setString(2, oldImsi);
            result = ps.executeUpdate();
            logger.info("End updateSubSimMb subId " + subId + " oldImsi " + oldImsi + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            StringBuilder brBuilder = new StringBuilder();
            brBuilder.setLength(0);
            brBuilder.append(loggerLabel).append(new Date()).
                    append("\nERROR updateSubSimMb: ").
                    append(sql).append("\n")
                    .append(" subId ")
                    .append(subId)
                    .append(" oldImsi ")
                    .append(oldImsi)
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

    public int insertSubSimMb(Long subId, String newImsi) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pos");
            sql = "INSERT INTO Sub_Sim_Mb \n"
                    + "VALUES(?,?,sysdate,NULL,1,sub_sim_mb_id_seq.nextval,NULL)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, subId);
            ps.setString(2, newImsi);
            result = ps.executeUpdate();
            logger.info("End insertSubSimMb subId " + subId + " newImsi " + newImsi
                    + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertSubSimMb: ").
                    append(sql).append("\n")
                    .append(" subId ")
                    .append(subId)
                    .append(" newImsi ")
                    .append(newImsi)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int getSimStatus(String serial) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        boolean result = false;
        long startTime = System.currentTimeMillis();
        int simStatus = 9;
        try {
            connection = getConnection("dbsm");
            sql = "select status from sm.stock_sim where serial = to_number(?)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, serial);
            rs = ps.executeQuery();
            while (rs.next()) {
                simStatus = rs.getInt("status");
            }
            logger.info("End getSimStatus serial " + serial
                    + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR getSimStatus: ").
                    append(sql).append("\n")
                    .append(" serial ")
                    .append(serial)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeResultSet(rs);
            closeConnection(connection);
            return simStatus;
        }
    }

//    public boolean checkIdNoOfSubscriber(String isdn, String idNo) {
//        ResultSet rs = null;
//        Connection connection = null;
//        PreparedStatement ps = null;
//        boolean result = false;
//        StringBuilder br = new StringBuilder();
//        String sql = "";
//        long startTime = System.currentTimeMillis();
//        try {
//            connection = getConnection("cm_pre");
//            sql = "select * from sub_mb where isdn = ? and status= 2 "
//                    + "and cust_id in (select cust_id from customer where lower(id_no) = lower(?))";
//            ps = connection.prepareStatement(sql);
//            if (isdn.startsWith("258")) {
//                isdn = isdn.substring(3);
//            }
//            ps.setString(1, isdn);
//            ps.setString(2, idNo);
//
//            rs = ps.executeQuery();
//            while (rs.next()) {
//                String otpResult = rs.getString("isdn");
//                if (otpResult != null && otpResult.length() > 0) {
//                    result = true;
//                    break;
//                }
//            }
//            logger.info("End checkIdNoOfSubscriber isdn " + isdn + "id_no: " + idNo + " result " + result + " time "
//                    + (System.currentTimeMillis() - startTime));
//        } catch (Exception ex) {
//            br.setLength(0);
//            br.append(loggerLabel).append(new Date()).
//                    append("\nERROR checkIdNoOfSubscriber: ").
//                    append(sql).append("\n")
//                    .append(" isdn ")
//                    .append(isdn)
//                    .append(" id_no ")
//                    .append(idNo)
//                    .append(" result ")
//                    .append(result);
//            logger.error(br + ex.toString());
//        } finally {
//            closeResultSet(rs);
//            closeStatement(ps);
//            closeConnection(connection);
//            return result;
//        }
//    }
//    public boolean checkCorrectProfile(String isdn) {
//        ResultSet rs = null;
//        Connection connection = null;
//        PreparedStatement ps = null;
//        boolean result = false;
//        StringBuilder br = new StringBuilder();
//        String sql = "";
//        long startTime = System.currentTimeMillis();
//        try {
//            connection = getConnection("cm_pre");
//            sql = "select a.isdn from cm_pre.sub_profile_info a, cm_pre.sub_mb b where a.isdn = b.isdn and b.status = 2 \n"
//                    + "and a.cust_id = b.cust_id and a.isdn = ? and a.check_status = 1";
//            ps = connection.prepareStatement(sql);
//            ps.setString(1, isdn);
//            rs = ps.executeQuery();
//            while (rs.next()) {
//                String otpResult = rs.getString("isdn");
//                if (otpResult != null && otpResult.length() > 0) {
//                    result = true;
//                    break;
//                }
//            }
//            logger.info("End checkCorrectProfile isdn " + isdn + " result " + result + " time "
//                    + (System.currentTimeMillis() - startTime));
//        } catch (Exception ex) {
//            br.setLength(0);
//            br.append(loggerLabel).append(new Date()).
//                    append("\nERROR checkCorrectProfile: ").
//                    append(sql).append("\n")
//                    .append(" isdn ")
//                    .append(isdn)
//                    .append(" result ")
//                    .append(result);
//            logger.error(br + ex.toString());
//        } finally {
//            closeResultSet(rs);
//            closeStatement(ps);
//            closeConnection(connection);
//            return result;
//        }
//    }
//    public boolean checkCorrectOldProfile(String isdn) {
//        ResultSet rs = null;
//        Connection connection = null;
//        PreparedStatement ps = null;
//        boolean result = false;
//        StringBuilder br = new StringBuilder();
//        String sql = "";
//        long startTime = System.currentTimeMillis();
//        try {
//            connection = getConnection("cm_pre");
//            sql = "SELECT Action_Profile_ID AS actionProfileId, isdn_account as isdn FROM profile.Action_Profile WHERE isdn_account = ? AND check_info = '0' AND check_status = 1";
//            ps = connection.prepareStatement(sql);
//            ps.setString(1, isdn);
//            rs = ps.executeQuery();
//            while (rs.next()) {
//                String otpResult = rs.getString("isdn");
//                if (otpResult != null && otpResult.length() > 0) {
//                    result = true;
//                    break;
//                }
//            }
//            logger.info("End checkCorrectOldProfile isdn " + isdn + " result " + result + " time "
//                    + (System.currentTimeMillis() - startTime));
//        } catch (Exception ex) {
//            br.setLength(0);
//            br.append(loggerLabel).append(new Date()).
//                    append("\nERROR checkCorrectOldProfile: ").
//                    append(sql).append("\n")
//                    .append(" isdn ")
//                    .append(isdn)
//                    .append(" result ")
//                    .append(result);
//            logger.error(br + ex.toString());
//        } finally {
//            closeResultSet(rs);
//            closeStatement(ps);
//            closeConnection(connection);
//            return result;
//        }
//    }
}
