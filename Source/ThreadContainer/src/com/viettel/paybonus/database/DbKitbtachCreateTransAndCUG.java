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
import com.viettel.vas.util.obj.DataResources;
import com.viettel.vas.util.obj.Param;
import com.viettel.vas.util.obj.ParamList;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.List;
import org.apache.log4j.Logger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

/**
 *
 * Thong tin phien ban
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class DbKitbtachCreateTransAndCUG extends DbProcessorAbstract {

    private String loggerLabel = DbKitbtachCreateTransAndCUG.class.getSimpleName() + ": ";
    private PoolStore poolStore;
    private String dbNameCofig;

    public DbKitbtachCreateTransAndCUG() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
        dbNameCofig = "cm_pre";
        poolStore = new PoolStore(dbNameCofig, logger);
    }

    public DbKitbtachCreateTransAndCUG(String sessionName, Logger logger) throws SQLException, Exception {
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
        KitBatchGroup record = new KitBatchGroup();
        long timeSt = System.currentTimeMillis();
        try {
            record.setId(rs.getLong("kit_batch_id"));
            record.setKitBatchId(rs.getInt("kit_batch_id"));
            record.setTransId(rs.getString("trans_id"));
            record.setEnterpriseWallet(rs.getString("enterprise_wallet"));
            record.setCreateUser(rs.getString("create_user"));
            record.setActionType(rs.getInt("action_type"));
        } catch (Exception ex) {
            logger.error("ERROR parse KitBatchGroup");
            logger.error(AppManager.logException(timeSt, ex));
        }
        return record;
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
        long timeStart = System.currentTimeMillis();
        PreparedStatement ps = null;
        Connection connection = null;
        String batchId = "";
        String sqlDeleteMo = "update group_management_log set status =?, last_update_time = sysdate where trans_id = ? ";
        try {
            connection = ConnectionPoolManager.getConnection("cm_pre");
            ps = connection.prepareStatement(sqlDeleteMo);
            for (Record rc : listRecords) {
                KitBatchGroup pn = (KitBatchGroup) rc;
                ps.setInt(1, 1);
                ps.setString(2, pn.getTransId());
                ps.addBatch();
            }
            return ps.executeBatch();
        } catch (Exception ex) {
            logger.error("ERROR update group_management_log  batchid " + batchId, ex);
            logger.error(AppManager.logException(timeStart, ex));
            return null;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to update group_management_log, batchid " + batchId, timeStart);
        }
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

    public Long getSequence(String sequenceName, String connName) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        Long sequenceId = 0L;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(connName);
            String sql = " select " + sequenceName + ".nextval as sequence_id from dual";
            ps = connection.prepareStatement(sql);
            rs = ps.executeQuery();
            while (rs.next()) {
                sequenceId = rs.getLong("sequence_id");
            }
            logger.info("End getSequence with idNo: " + sequenceName + "time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getSequence: ------ idNo: ")
                    .append(sequenceName).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return sequenceId;
        }
    }

    public String getShopIdStaffIdByStaffCode(String staffCode) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        StringBuilder result = null;
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
            logger.info("End getIsdnWalletByStaffCode: " + staffCode + " time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getIsdnWalletByStaffCode ").append(staffCode).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
            result = null;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return result.toString();
        }
    }

    public int insertSaleTrans(Long saleTransId, Long shopId, Long staffId, Long saleServiceId, Long saleServicePriceId, Double amountTax,
            Long subId, String isdn, Long reasonId, String ewalletRequestId, int payMethod, String bankTransCode, double discount, double vasAmount) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "INSERT INTO sm.sale_trans (SALE_TRANS_ID,SALE_TRANS_DATE,SALE_TRANS_TYPE,STATUS,CHECK_STOCK,INVOICE_USED_ID,"
                    + "INVOICE_CREATE_DATE,SHOP_ID,STAFF_ID,PAY_METHOD,SALE_SERVICE_ID,SALE_SERVICE_PRICE_ID,AMOUNT_SERVICE,"
                    + "AMOUNT_MODEL,DISCOUNT,PROMOTION,AMOUNT_TAX,AMOUNT_NOT_TAX,VAT,TAX,SUB_ID,ISDN,CUST_NAME,CONTRACT_NO,"
                    + "TEL_NUMBER,COMPANY,ADDRESS,TIN,NOTE,DESTROY_USER,DESTROY_DATE,APPROVER_USER,APPROVER_DATE,REASON_ID,"
                    + "TELECOM_SERVICE_ID,TRANSFER_GOODS,SALE_TRANS_CODE,STOCK_TRANS_ID,CREATE_STAFF_ID,RECEIVER_ID,SYN_STATUS,"
                    + "RECEIVER_TYPE,IN_TRANS_ID,FROM_SALE_TRANS_ID,DAILY_SYN_STATUS,CURRENCY,CHANNEL,SALE_TRANS,SERIAL_STATUS,"
                    + "INVOICE_DESTROY_ID,SALE_PROGRAM,SALE_PROGRAM_NAME,PARENT_MASTER_AGENT_ID,PAYMENT_PAPERS_CODE,AMOUNT_PAYMENT,"
                    + "LAST_UPDATE,CLEAR_DEBIT_STATUS,CLEAR_DEBIT_TIME,CLEAR_DEBIT_USER,CLEAR_DEBIT_REQUEST_ID)  "
                    + " VALUES(?,sysdate,'4','2',NULL,NULL,NULL,?,?,'1',?,?,NULL,NULL,?,NULL, "
                    + "?,?,17,?,?,?,NULL,NULL,?,NULL,NULL,\n"
                    + "NULL,NULL,NULL,NULL,NULL,NULL,?,1,NULL,?,NULL,NULL,NULL,'0',NULL,NULL,NULL,0,'MT',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,\n"
                    + "sysdate,?,?,?,?)";//'SYSTEM_AUTO_CN_KIT'
            ps = connection.prepareStatement(sql);
            ps.setLong(1, saleTransId);
            ps.setLong(2, shopId);
            ps.setLong(3, staffId);
            if (saleServiceId > 0) {
                ps.setLong(4, saleServiceId);
            } else {
                ps.setString(4, "");
            }
            if (saleServicePriceId > 0) {
                ps.setLong(5, saleServicePriceId);
            } else {
                ps.setString(5, "");
            }
            if (discount > 0) {
                ps.setDouble(6, discount / 1.17);
            } else {
                ps.setString(6, "");
            }
            ps.setDouble(7, amountTax);
            Double amountNotTax = amountTax / 1.17;
            ps.setDouble(8, amountNotTax);
            Double tax = amountTax - amountNotTax;
            ps.setDouble(9, tax);
            ps.setLong(10, subId);
            ps.setString(11, isdn);
            ps.setString(12, isdn);
            ps.setLong(13, reasonId);
            String prefix = "";
            if (vasAmount > 0 && amountTax == 0) {
                prefix = "EMOLA0000";
            } else {
                prefix = "SS0000";
            }
            String saleTransCode = prefix + String.format("%0" + 9 + "d", saleTransId);
            ps.setString(14, saleTransCode);
            if ((ewalletRequestId != null && ewalletRequestId.length() > 0) || amountTax == 0) {
                ps.setLong(15, 1L);
                ps.setTimestamp(16, new Timestamp(new Date().getTime()));
                ps.setString(17, "SYSTEM_AUTO_CUSTOMER_ELITE");
                ps.setString(18, ewalletRequestId);
            } else {
                ps.setString(15, "");
                ps.setNull(16, java.sql.Types.DATE);
                ps.setString(17, "");
                ps.setString(18, "");

            }
            result = ps.executeUpdate();
            logger.info("End insertSaleTrans saleTransId " + saleTransId + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertSaleTrans: ").
                    append(sql).append("\n")
                    .append(" saleTransId ")
                    .append(saleTransId)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int insertSaleTransDetail(Long saleTransDetailId, Long saleTransId, String stockModelId, String priceId, String saleServiceId, String saleServicePriceId,
            String stockTypeId, String stockTypeName, String stockModelCode, String stockModelName, String saleServicesCode, String saleServicesName, String accountModelCode,
            String accountModelName, String saleServicesPriceVat, String priceVat, String price, String saleServicesPrice, Double amountTax, Double discountAmout, Long quantity) {

        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "INSERT INTO sm.sale_trans_detail (SALE_TRANS_DETAIL_ID,SALE_TRANS_ID,SALE_TRANS_DATE,STOCK_MODEL_ID,STATE_ID,PRICE_ID,QUANTITY,DISCOUNT_ID,"
                    + "TRANSFER_GOOD,PROMOTION_ID,PROMOTION_AMOUNT,NOTE,UPDATE_STOCK_TYPE,USER_DELIVER,DELIVER_DATE,USER_UPDATE,DELIVER_STATUS,SALE_SERVICES_ID,"
                    + "SALE_SERVICES_PRICE_ID,STOCK_TYPE_ID,STOCK_TYPE_CODE,STOCK_TYPE_NAME,STOCK_MODEL_CODE,STOCK_MODEL_NAME,SALE_SERVICES_CODE,SALE_SERVICES_NAME,"
                    + "ACCOUNTING_MODEL_CODE,ACCOUNTING_MODEL_NAME,CURRENCY,VAT_AMOUNT,SALE_SERVICES_PRICE_VAT,PRICE_VAT,PRICE,SALE_SERVICES_PRICE,AMOUNT,"
                    + "DISCOUNT_AMOUNT,AMOUNT_BEFORE_TAX,AMOUNT_TAX,AMOUNT_AFTER_TAX)\n"
                    + "VALUES(?,?,sysdate,?,1,?,?,NULL,NULL,NULL,NULL,NULL,0,NULL,NULL,NULL,NULL,"
                    + "?,?,?,NULL,?,?,?,?,?,?,?,'MT',?,?,?,?,?,?,?,?,?,?)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, saleTransDetailId);
            ps.setLong(2, saleTransId);
            ps.setString(3, stockModelId);
            ps.setString(4, priceId);
            ps.setLong(5, quantity);
            ps.setString(6, saleServiceId);
            ps.setString(7, saleServicePriceId);
            ps.setString(8, stockTypeId);
            ps.setString(9, stockTypeName);
            ps.setString(10, stockModelCode);
            ps.setString(11, stockModelName);
            ps.setString(12, saleServicesCode);
            ps.setString(13, saleServicesName);
            ps.setString(14, accountModelCode);
            ps.setString(15, accountModelName);
            Double amountNotTax = amountTax / 1.17;
            Double tax = amountTax - amountNotTax;
            ps.setDouble(16, tax);
            ps.setString(17, saleServicesPriceVat);
            ps.setString(18, priceVat);
            ps.setString(19, price);
            ps.setString(20, saleServicesPrice);
            ps.setDouble(21, amountTax);
            if (discountAmout > 0) {
                ps.setDouble(22, discountAmout / 1.17);
                ps.setDouble(23, (amountTax - discountAmout) / 1.17);
                ps.setDouble(24, (amountTax - discountAmout) - (amountTax - discountAmout) / 1.17);
                ps.setDouble(25, amountTax - discountAmout);
            } else {
                ps.setString(22, "");
                ps.setDouble(23, amountNotTax);
                ps.setDouble(24, tax);
                ps.setDouble(25, amountTax);
            }

            result = ps.executeUpdate();
            logger.info("End insertSaleTrans saleTransId " + saleTransId + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertSaleTrans: ").
                    append(sql).append("\n")
                    .append(" saleTransId ")
                    .append(saleTransId)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int insertLogMakeSaleTransFail(Long saleTransId, String isdn, String serial, Long shopId, Long staffId, Long saleServicesId, Long saleServicesPriceId,
            Long reasonId, Long saleTransDetailId, Long saleTransSerialId, String tableName) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "insert into kit_make_sale_trans_fail values (kit_make_sale_trans_fail_seq.nextval,?,?,?,?,?,?,?,?,?,?,?,sysdate)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, saleTransId);
            ps.setString(2, isdn);
            ps.setString(3, serial);
            ps.setLong(4, shopId);
            ps.setLong(5, staffId);
            ps.setLong(6, saleServicesId);
            ps.setLong(7, saleServicesPriceId);
            ps.setLong(8, reasonId);
            ps.setLong(9, saleTransDetailId);
            ps.setLong(10, saleTransSerialId);
            ps.setString(11, tableName);
            result = ps.executeUpdate();
            logger.info("End insertLogMakeSaleTransFail isdn " + isdn + " serial " + serial
                    + " saleTransId " + saleTransId + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertLogMakeSaleTransFail: ").
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

    public int sendSms(String msisdn, String message, String channel) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("bockd");
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

    public List<KitBatchGroup> getKitBatchGroup(long batchId, String transId) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        StringBuilder result = null;
        long startTime = System.currentTimeMillis();
        List<KitBatchGroup> listKitBatch = new ArrayList<KitBatchGroup>();
        try {
            connection = getConnection("cm_pre");
            String sql = "select * from GROUP_MANAGEMENT_LOG where kit_batch_id =? and trans_id=?";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, batchId);
            ps.setString(2, transId);
            rs = ps.executeQuery();
            while (rs.next()) {
                KitBatchGroup record = new KitBatchGroup();
                record.setId(rs.getLong("mo_id"));
                record.setBatchId(rs.getString("kit_batch_id"));
                record.setTransId(rs.getString("trans_id"));
                record.setIsdn(rs.getString("isdn"));
                record.setProductCode(rs.getString("product_code"));
                record.setProductCode(rs.getString("product_code"));
                record.setEnterpriseWallet(rs.getString("enterprise_wallet"));
                record.setCreateUser(rs.getString("create_user"));
                record.setPrepaidMonth(rs.getInt("prepaid_month"));
                record.setPrice(rs.getDouble("price"));
                record.setDiscount(rs.getDouble("discount"));
                record.setDescription(rs.getString("description"));
                record.setResultCode(rs.getString("result_code"));
                record.setActionType(rs.getInt("action_type"));
                record.setStatus(rs.getInt("status"));
                listKitBatch.add(record);
            }
            logger.info("End getKitBatchGroup: " + listKitBatch.size() + " time: "
                    + (System.currentTimeMillis() - startTime));
            return listKitBatch;
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getKitBatchGroup batchId ").append(batchId).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
            return null;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
        }
    }

    public String getCUGInformation(Long kitBatchId) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        String cugName = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "select distinct nvl(cug_name,'N/A') as cug_name from cm_pre.kit_batch_detail where kit_batch_id = ?  and cug_id is not null";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, kitBatchId);
            rs = ps.executeQuery();
            while (rs.next()) {
                cugName = rs.getString("cug_name");
                break;
            }
            logger.info("End getCUGInformation kitBatchId " + kitBatchId
                    + " cugName " + cugName + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR checkElitePackage ---- kitBatchId ").
                    append(kitBatchId).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeResultSet(rs);
            closeConnection(connection);
            return cugName;
        }
    }

    public List<KitBatch> getListKitBatchDetailElite(Long kitBatchId) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        List<KitBatch> lstKitBatch = new ArrayList<KitBatch>();
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            String sql = "select * from kit_batch_detail where result_code = '0' and kit_batch_id in (select kit_batch_id from kit_batch_info "
                    + "where kit_batch_id = ? or extend_from_kit_batch_id = ? and result_code = '0') and (product_code in \n"
                    + " (select product_code from product.product_connect_kit where vip_product = 1 and status = 1) "
                    + "or product_code in (select vas_code from product.product_add_on where status = 1))";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, kitBatchId);
            ps.setLong(2, kitBatchId);
            rs = ps.executeQuery();
            while (rs.next()) {
                Long tmpKitBatchId = rs.getLong("kit_batch_id");
                String serial = rs.getString("serial");
                String isdn = rs.getString("isdn");
                String productCode = rs.getString("product_code");
                String stateOfRecord = rs.getString("state_of_record");
                KitBatch kitBatch = new KitBatch();
                kitBatch.setSerial(serial);
                kitBatch.setKitBatchId(tmpKitBatchId);
                kitBatch.setIsdn(isdn);
                kitBatch.setProductCode(productCode);
                kitBatch.setStateOfRecord(stateOfRecord);
                kitBatch.setCustId(rs.getLong("cust_id"));
                lstKitBatch.add(kitBatch);

            }
            logger.info("End getListKitBatchDetailElite: kitBatchId" + kitBatchId + " time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getListKitBatchDetailElite ").
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
                Long tmpKitBatchId = rs.getLong("kit_batch_id");
                String isdn = rs.getString("isdn");
                KitBatch kitBatch = new KitBatch();
                kitBatch.setKitBatchId(tmpKitBatchId);
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

    public String getExpireTimeCUG(Long kitBatchId) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        String expireTime = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "select * from cm_pre.kit_batch_info where kit_batch_id = ?";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, kitBatchId);
            rs = ps.executeQuery();
            while (rs.next()) {
                expireTime = rs.getString("expire_time_group");
                break;
            }
            logger.info("End getExpireTimeCUG kitBatchId " + kitBatchId
                    + " expireTime " + expireTime + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getExpireTimeCUG ---- kitBatchId ").
                    append(kitBatchId).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeResultSet(rs);
            closeConnection(connection);
            return expireTime;
        }
    }

    public int insertKitBatchExtend(Long kitBatchInfoId, String isdn, Long cugId, String cugName,
            String resultCode, String description, String staffCode) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "insert into kit_batch_extend (kit_batch_id, isdn, cug_id, cug_name, result_code, description, "
                    + "staff_code, extend_time)\n"
                    + "values (?,?,?,?,?,?,?,sysdate)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, kitBatchInfoId);
            ps.setString(2, isdn);
            ps.setLong(3, cugId);
            ps.setString(4, cugName);
            ps.setString(5, resultCode);
            ps.setString(6, description);
            ps.setString(7, staffCode);
            result = ps.executeUpdate();
            logger.info("End insertKitBatchExtend kitBatchInfoId " + kitBatchInfoId
                    + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertKitBatchExtend: ").
                    append(sql).append("\n")
                    .append(" kitBatchInfoId ")
                    .append(kitBatchInfoId);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int updateKitBatchExtend(Long kitBatchInfoId, String isdn, Long cugId, String cugName,
            String resultCode, String description, String staffCode) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "update kit_batch_extend\n" +
                "set cug_id = ?, cug_name = ?, result_code = ?, description = ?, staff_code = ?\n" +
                "where kit_batch_id = ? and isdn = ?";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, cugId);
            ps.setString(2, cugName);
            ps.setString(3, resultCode);
            ps.setString(4, description);
            ps.setString(5, staffCode);
            ps.setLong(6, kitBatchInfoId);
            ps.setString(7, isdn);
            result = ps.executeUpdate();
            logger.info("End updateKitBatchExtend kitBatchInfoId " + kitBatchInfoId
                    + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR updateKitBatchExtend: ").
                    append(sql).append("\n")
                    .append(" kitBatchInfoId ")
                    .append(kitBatchInfoId);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public long getKitBatchExtendId(Long kitBatchId) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        long kitBatchExtId = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "select * from cm_pre.kit_batch_info where kit_batch_id = ?";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, kitBatchId);
            rs = ps.executeQuery();
            while (rs.next()) {
                kitBatchExtId = rs.getLong("extend_from_kit_batch_id");
                break;
            }
            logger.info("End getKitBatchExtendId kitBatchId " + kitBatchId
                    + " kitBatchExtId " + kitBatchExtId + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getKitBatchExtendId ---- kitBatchId ").
                    append(kitBatchId).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeResultSet(rs);
            closeConnection(connection);
            return kitBatchExtId;
        }
    }

    public int updateGroupEliteKitBatchDetail(Long kitBatchId, String isdn, String resultCode,
            String cugId, String cugName, String ownerGroup) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder brBuilder = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "update kit_batch_detail set cug_id = ?, cug_name = ?, owner = ?, result_add_group = ?\n"
                    + "where kit_batch_id = ? and isdn = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, cugId);
            ps.setString(2, cugName);
            ps.setString(3, ownerGroup != null ? ownerGroup : "");
            ps.setString(4, resultCode != null ? resultCode : "");
            ps.setLong(5, kitBatchId);
            ps.setString(6, isdn);
            result = ps.executeUpdate();
            logger.info("End updateGroupEliteKitBatchDetail kitBatchId " + kitBatchId + ", isdn: " + isdn + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            brBuilder.setLength(0);
            brBuilder.append(loggerLabel).append(new Date()).
                    append("\nERROR updateGroupEliteKitBatchDetail: ").
                    append(sql).append("\n")
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

    public int updateExpireTimeKitBatchInfo(Long kitBatchId, String expireTime) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder brBuilder = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "update kit_batch_info set expire_time_group = ?, group_status = 1 \n"
                    + "where kit_batch_id = ? and extend_from_kit_batch_id is null";
            ps = connection.prepareStatement(sql);
            ps.setString(1, expireTime);
            ps.setLong(2, kitBatchId);
            result = ps.executeUpdate();
            logger.info("End updateExpireTimeKitBatchInfo kitBatchId " + kitBatchId + ", expireTime: " + expireTime + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            brBuilder.setLength(0);
            brBuilder.append(loggerLabel).append(new Date()).
                    append("\nERROR updateExpireTimeKitBatchInfo: ").
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
