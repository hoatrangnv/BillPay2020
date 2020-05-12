
/*
 * Copyright 2012 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.paybonus.database;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.obj.ActionLogPr;
import com.viettel.paybonus.obj.Customer;
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
import java.text.SimpleDateFormat;

/**
 *
 * Thong tin phien ban
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class DbChangeSim4GProcessor extends DbProcessorAbstract {

//    private Logger logger;
    private String loggerLabel = DbChangeSim4GProcessor.class.getSimpleName() + ": ";
//    private StringBuffer br = new StringBuffer();
    private PoolStore poolStore;
    private String dbNameCofig;
    private String sqlDeleteMo = "update cm_pre.request_changesim_4g set status = 1, date_process = sysdate,"
            + " result_code = ?, description = ?, old_serial = ?, old_imsi = ?, duration = ?, action_audit_id = ? where id = ?";

    public DbChangeSim4GProcessor() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
        dbNameCofig = ResourceBundle.getBundle("configPayBonus").getString("dbNameConfig");
        poolStore = new PoolStore(dbNameCofig, logger);
    }

    public DbChangeSim4GProcessor(String sessionName, Logger logger) throws SQLException, Exception {
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
            connection = ConnectionPoolManager.getConnection("cm_pre");
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

    public boolean checkMovitelStaff(String staffCode) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        boolean result = false;
        try {
            connection = ConnectionPoolManager.getConnection(dbNameCofig);
            ps = connection.prepareStatement(
                    //LinhNBV start modified on April 12 2018: Add condition to check profile of channel correct or not
                    "select staff_code from sm.staff where status = 1 and channel_type_id = 14 and lower(staff_code) = lower(?)");
            if (QUERY_TIMEOUT > 0) {
                ps.setQueryTimeout(QUERY_TIMEOUT);
            }
            ps.setString(1, staffCode.toUpperCase());
            rs = ps.executeQuery();
            while (rs.next()) {
                String channel = rs.getString("staff_code");
                if (channel != null && channel.trim().length() > 0) {
                    result = true;
                }
                break;
            }
            logTimeDb("Time to checkMovitelStaff " + " result: " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR checkMovitelStaff defaul return false");
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
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

    public String getSaleServiceCode(Long reasonId, String productCode, String isdn) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        String saleServiceCode = null;
        String sqlMo = "Select * from Mapping m, Reason r \n"
                + "where m.reason_Id = r.reason_Id and r.status = 1 and m.status = 1 \n"
                + "and m.channel is null and m.reason_Id = ? and (m.product_Code = ? or m.product_Code is null)";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection("cm_pre");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setLong(1, reasonId);
            psMo.setString(2, productCode);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                saleServiceCode = rs1.getString("sale_service_code");
                logger.info("sale_service_code " + saleServiceCode
                        + " base on productCode " + productCode + " reasonId " + reasonId + " isdn " + isdn);
            }
            logTimeDb("Time to getSaleServiceCode: " + saleServiceCode
                    + " base on productCode " + productCode + " reasonId " + reasonId + " isdn " + isdn, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getSaleServiceCode " + " base on productCode " + productCode + " reasonId " + reasonId + " isdn " + isdn);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
            return saleServiceCode;
        }
    }

    public SaleServices getSaleService(String saleServiceCode, String isdn) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        SaleServices saleService = null;
        String sqlMo = "select * from sm.sale_services where code = ? and status = 1";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection("dbsm");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setString(1, saleServiceCode);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                Long saleServiceId = rs1.getLong("sale_services_id");
                String name = rs1.getString("name");
                String accountModelCode = rs1.getString("accounting_model_code");
                String accountModelName = rs1.getString("accounting_model_name");
                saleService = new SaleServices();
                saleService.setSaleServicesId(saleServiceId);
                saleService.setName(name);
                saleService.setAccountModelCode(accountModelCode);
                saleService.setAccountModelName(accountModelName);
                logger.info("Result getSaleService  " + saleServiceCode
                        + " saleServiceId " + saleServiceId + " isdn " + isdn);
                break;
            }
            logTimeDb("Time to getSaleService: " + saleServiceCode, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getSaleService " + saleServiceCode + " isdn " + isdn);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
            return saleService;
        }
    }

    public SaleServicesPrice getSaleServicesPrice(Long saleServicesId, String isdn) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        SaleServicesPrice saleServicesPrice = null;
        String sqlMo = "select * from Sale_Services_Price where sale_Services_Id = ? \n"
                + "and sta_Date <= to_date(to_char(trunc(sysdate),'MM/DD/YYYY')||' 23:59:59','MM/DD/YYYY HH24:MI:SS')\n"
                + "and (((end_Date >= to_date(to_char(trunc(sysdate),'MM/DD/YYYY')||' 00:00:00','MM/DD/YYYY HH24:MI:SS')) and (end_Date is not null)) or (end_Date is null)) \n"
                + "and status = 1 and price_Policy = 1";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection("dbsm");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setLong(1, saleServicesId);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                Long saleServicesPriceId = rs1.getLong("sale_services_price_id");
                Double price = rs1.getDouble("price");
                Double vat = rs1.getDouble("vat");
                saleServicesPrice = new SaleServicesPrice();
                saleServicesPrice.setSaleServicesPriceId(saleServicesPriceId);
                saleServicesPrice.setPrice(price);
                saleServicesPrice.setVat(vat);
                logger.info("Result getSaleServicesPrice saleServicesId "
                        + saleServicesId + " saleServicesPriceId " + saleServicesPriceId + " isdn " + isdn);
                break;
            }
            logTimeDb("Time to getSaleServicesPrice: " + saleServicesId + " isdn " + isdn, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getSaleServicesPrice " + saleServicesId + " isdn " + isdn);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
            return saleServicesPrice;
        }
    }

    public Price getPrice(Long stockModelId, Long saleServicesId) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        Price priceObj = null;
        String sqlMo = "select * from price where 1 = 1 and price_id = (select price_id from Sale_Services_Detail where stock_model_id = ? and status= 1 \n"
                + "and sale_Services_Model_Id in (select sale_Services_Model_Id from Sale_Services_Model where sale_Services_Id = ? and stock_Type_Id = 4 )) \n"
                + "and status = 1 and trunc(sta_date) <= trunc(sysdate) and (end_Date is null or trunc(end_Date) >= trunc(sysdate))";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection("dbsm");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setLong(1, stockModelId);
            psMo.setLong(2, saleServicesId);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                Long priceId = rs1.getLong("price_id");
                Double price = rs1.getDouble("price");
                priceObj = new Price();
                priceObj.setPriceId(priceId);
                priceObj.setStockModelId(stockModelId);
                priceObj.setPrice(price);
                logger.info("Result getPrice " + saleServicesId + " stockModelId " + stockModelId + " priceId " + priceId);
                break;
            }
            logTimeDb("Time to getPrice: " + saleServicesId + " stockModelId " + stockModelId, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getPrice " + saleServicesId + " stockModelId " + stockModelId);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return priceObj;
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

    public int insertSaleTrans(Long saleTransId, Long shopId, Long staffId, Long saleServiceId, Long saleServicePriceId, Double amountTax,
            Long subId, String isdn, Long reasonId, String ewalletRequestId) {
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
                    + " VALUES(?,sysdate,'4','2',NULL,NULL,NULL,?,?,'1',?,?,NULL,NULL,NULL,NULL, "
                    + "?,?,17,?,?,?,NULL,NULL,?,NULL,NULL,\n"
                    + "NULL,NULL,NULL,NULL,NULL,NULL,?,1,NULL,?,NULL,NULL,NULL,'0',NULL,NULL,NULL,0,'MT',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,\n"
                    + "sysdate,NULL,NULL,NULL,?)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, saleTransId);
            ps.setLong(2, shopId);
            ps.setLong(3, staffId);
            ps.setLong(4, saleServiceId);
            ps.setLong(5, saleServicePriceId);
            ps.setDouble(6, amountTax);
            Double amountNotTax = amountTax / 1.17;
            ps.setDouble(7, amountNotTax);
            Double tax = amountTax - amountNotTax;
            ps.setDouble(8, tax);
            ps.setLong(9, subId);
            ps.setString(10, isdn);
            ps.setString(11, isdn);
            ps.setLong(12, reasonId);
            String saleTransCode = "SS0000" + String.format("%0" + 9 + "d", saleTransId);
            ps.setString(13, saleTransCode);
            ps.setString(14, ewalletRequestId);
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

    public int insertSaleTransDetail(Long saleTransDetailId, Long saleTransId, String stockModelId, String priceId, Long saleServiceId, String saleServicePriceId,
            String stockTypeId, String stockTypeName, String stockModelCode, String stockModelName, String saleServicesCode, String saleServicesName, String accountModelCode,
            String accountModelName, String saleServicesPriceVat, String priceVat, String price, String saleServicesPrice, Double amountTax, Double discountAmout) {

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
                    + "VALUES(?,?,sysdate,?,1,?,1,NULL,NULL,NULL,NULL,NULL,0,NULL,NULL,NULL,NULL,"
                    + "?,?,?,NULL,?,?,?,?,?,?,?,'MT',?,?,?,?,?,?,?,?,?,?)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, saleTransDetailId);
            ps.setLong(2, saleTransId);
            ps.setString(3, stockModelId);
            ps.setString(4, priceId);
            ps.setLong(5, saleServiceId);
            ps.setString(6, saleServicePriceId);
            ps.setString(7, stockTypeId);
            ps.setString(8, stockTypeName);
            ps.setString(9, stockModelCode);
            ps.setString(10, stockModelName);
            ps.setString(11, saleServicesCode);
            ps.setString(12, saleServicesName);
            ps.setString(13, accountModelCode);
            ps.setString(14, accountModelName);
            Double amountNotTax = amountTax / 1.17;
            Double tax = amountTax - amountNotTax;
            ps.setDouble(15, tax);
            ps.setString(16, saleServicesPriceVat);
            ps.setString(17, priceVat);
            ps.setString(18, price);
            ps.setString(19, saleServicesPrice);
            ps.setDouble(20, amountTax);
            if (discountAmout > 0) {
                ps.setDouble(21, discountAmout);
                ps.setDouble(22, 0);
                ps.setDouble(23, 0);
                ps.setDouble(24, 0);
            } else {
                ps.setString(21, "");
                ps.setDouble(22, amountNotTax);
                ps.setDouble(23, tax);
                ps.setDouble(24, amountTax);
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

    public int insertSaleTransSerial(Long saleTransSerialId, Long saleTransDetailId, Long stockModelId, String isdn) {

        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "INSERT INTO sm.sale_trans_serial \n"
                    + "VALUES(?,?,?,sysdate,NULL,NULL,NULL,?,?,1)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, saleTransSerialId);
            ps.setLong(2, saleTransDetailId);
            ps.setLong(3, stockModelId);
            ps.setString(4, isdn);
            ps.setString(5, isdn);

            result = ps.executeUpdate();
            logger.info("End insertSaleTransSerial saleTransDetailId " + saleTransDetailId + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertSaleTransSerial: ").
                    append(sql).append("\n")
                    .append(" saleTransDetailId ")
                    .append(saleTransDetailId)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
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

    public StockModel findStockModelById(Long stockModelId) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        StockModel stockModel = null;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            String sql = "select * from sm.stock_model where stock_model_id = ? and status = 1";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, stockModelId);
            rs = ps.executeQuery();
            while (rs.next()) {
                String stockModelCode = rs.getString("stock_model_code");
                String name = rs.getString("name");
                String accountModelCode = rs.getString("accounting_model_code");
                String accountModelName = rs.getString("accounting_model_name");
                stockModel = new StockModel();
                stockModel.setStockModelCode(stockModelCode);
                stockModel.setName(name);
                stockModel.setAccountingModelCode(accountModelCode);
                stockModel.setAccountingModelName(accountModelName);
                break;
            }
            logger.info("End findStockModelById: " + stockModelId + " time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR findStockModelById ").append(stockModelId).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
            stockModel = null;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return stockModel;
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
            connection = getConnection("cm_pre");
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
            connection = ConnectionPoolManager.getConnection("cm_pre");
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
                Long custId = rs1.getLong("cust_id");
                String productCode = rs1.getString("product_code");

                subInfo = new SubInfo();
                subInfo.setProductCode(productCode);
                subInfo.setActStatus(actStatus);
                subInfo.setImsi(imsi);
                subInfo.setSerial(serial);
                subInfo.setSubId(subId);
                subInfo.setCustId(custId);
                subInfo.setIsdnSub(rs1.getString("isdn"));
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

    public int cancelSaleTrans(String destroyUser, Long saleTransId) {
        Connection connection = null;
        PreparedStatement ps = null;

        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "update sale_trans set status = 4, destroy_user = ?, destroy_date = sysdate where sale_trans_id = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, destroyUser);
            ps.setLong(2, saleTransId);

            result = ps.executeUpdate();
            logger.info("End cancelSaleTrans destroyUser " + destroyUser + " saleTransId " + saleTransId + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            StringBuilder brBuilder = new StringBuilder();
            brBuilder.setLength(0);
            brBuilder.append(loggerLabel).append(new Date()).
                    append("\nERROR cancelSaleTrans: ").
                    append(sql).append("\n")
                    .append(" destroyUser ")
                    .append(destroyUser)
                    .append(" saleTransId ")
                    .append(saleTransId)
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

    public int updateSubMbChangeSim(Long subId, String isdn, String newImsi, String newSerial) {
        Connection connection = null;
        PreparedStatement ps = null;

        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
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
            connection = getConnection("cm_pre");
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
            connection = getConnection("cm_pre");
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
            connection = getConnection("cm_pre");
            sql = "INSERT INTO cm_pre.Sub_Sim_Mb \n"
                    + "VALUES(?,?,sysdate,NULL,1,seq_sub_sim_mb.nextval,NULL)";
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

    public String getIsdnWalletByStaffCode(String staffCode) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        String isdnWallet = null;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "select isdn_wallet from staff where lower(staff_code) = lower(?) and status = 1";
            ps = connection.prepareStatement(sql);
            ps.setString(1, staffCode);
            rs = ps.executeQuery();
            while (rs.next()) {
                isdnWallet = rs.getString("isdn_wallet");
                break;
            }
            logger.info("End getIsdnWalletByStaffCode: " + staffCode + " time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getIsdnWalletByStaffCode ").append(staffCode).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
            isdnWallet = null;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return isdnWallet;
        }
    }

    public int insertEwalletConnectKitLog(Long transactionType, String requestId, String amount,
            String voucherCode, String staffCode, String request, String response, String errCode, String description, String orgRequestId,
            String isdn) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "insert into EWALLET_CONNECT_KIT_LOG values (EWALLET_CONNECT_KIT_LOG_SEQ.nextval,?,?,?,?,?,sysdate,?,?,?,?,?)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, transactionType);
            ps.setString(2, requestId);
            ps.setString(3, amount);
            ps.setString(4, voucherCode);
            ps.setString(5, staffCode);
            ps.setString(6, request);
            ps.setString(7, response);
            ps.setString(8, errCode);
            ps.setString(9, description);
            ps.setString(10, orgRequestId);
            result = ps.executeUpdate();
            logger.info("End insertEwalletConnectKitLog staffCode " + staffCode + " isdn " + isdn + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertEwalletConnectKitLog: ").
                    append(sql).append("\n")
                    .append(" staffCode ")
                    .append(staffCode)
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

    public int insertMO(String msisdn, String vasCode, String connName, String param, String actionType, String channel) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        if (!msisdn.startsWith("258")) {
            msisdn = "258" + msisdn;
        }
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(connName);
            sql = "INSERT INTO mo (MO_ID,MSISDN,COMMAND,PARAM,RECEIVE_TIME,ACTION_TYPE,CHANNEL,CHANNEL_TYPE) \n"
                    + "VALUES(mo_seq.nextval,?,?,?,sysdate,?,?,'MBCCS')";
            ps = connection.prepareStatement(sql);
            ps.setString(1, msisdn);
            ps.setString(2, vasCode);
            ps.setString(3, param);
            ps.setString(4, actionType);
            ps.setString(5, channel);
            result = ps.executeUpdate();
            logger.info("End insertMO msisdn " + msisdn + " vasCode " + vasCode + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertMO: ").
                    append(sql).append("\n")
                    .append(" msisdn ")
                    .append(msisdn)
                    .append(" vasCode ")
                    .append(vasCode)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }
//    public int updateStockTotal(Long ownerId, Long stockModelId) {
//        Connection connection = null;
//        PreparedStatement ps = null;
//        
//        String sql = "";
//        int result = 0;
//        long startTime = System.currentTimeMillis();
//        try {
//            connection = getConnection("dbsm");
//            sql = "UPDATE   stock_total\n"
//                    + "   SET   quantity = quantity - 1,\n"
//                    + "         quantity_issue = quantity_issue - 1,\n"
//                    + "         modified_date = SYSDATE\n"
//                    + " WHERE       1 = 1\n"
//                    + "         AND owner_type = 2\n"
//                    + "         AND owner_id = ?\n"
//                    + "         AND stock_model_id = ?\n"
//                    + "         AND state_id = 1\n"
//                    + "         AND status = 1\n"
//                    + "         AND quantity >= 1\n"
//                    + "         AND quantity_issue >= 1";
//            ps = connection.prepareStatement(sql);
//            ps.setLong(1, ownerId);
//            ps.setLong(2, stockModelId);
//            
//            result = ps.executeUpdate();
//            logger.info("End updateStockTotal ownerId " + ownerId + " stockModelId " + stockModelId + " result " + result + " time "
//                    + (System.currentTimeMillis() - startTime));
//        } catch (Exception ex) {
//            StringBuilder brBuilder = new StringBuilder();
//            brBuilder.setLength(0);
//            brBuilder.append(loggerLabel).append(new Date()).
//                    append("\nERROR updateStockTotal: ").
//                    append(sql).append("\n")
//                    .append(" ownerId ")
//                    .append(ownerId)
//                    .append(" stockModelId ")
//                    .append(stockModelId)
//                    .append(" result ")
//                    .append(result);
//            logger.error(brBuilder + ex.toString());
//            result = -1;
//        } finally {
//            closeStatement(ps);
//            closeConnection(connection);
//            return result;
//        }
//    }
//    public boolean checkStockTotalOfStaff(Long ownerId, Long stockModelId) {
//        Connection connection = null;
//        PreparedStatement ps = null;
//        ResultSet rs = null;
//        StringBuilder br = new StringBuilder();
//        String sql = "";
//        boolean result = false;
//        long startTime = System.currentTimeMillis();
//        long transId = 0;
//        try {
//            connection = getConnection("dbsm");
//            sql = "select * from stock_total where owner_id = ? and owner_type = 2 and stock_model_id = ? "
//                    + "and state_id = 1 and status = 1 and quantity > 0 and quantity_issue > 0";
//            ps = connection.prepareStatement(sql);
//            ps.setLong(1, ownerId);
//            ps.setLong(2, stockModelId);
//            rs = ps.executeQuery();
//            while (rs.next()) {
//                transId = rs.getLong("stock_model_id");
//                if (transId > 0) {
//                    result = true;
//                    break;
//                }
//            }
//            logger.info("End checkStockTotalOfStaff ownerId " + ownerId
//                    + " result " + result + " time "
//                    + (System.currentTimeMillis() - startTime));
//        } catch (Exception ex) {
//            br.setLength(0);
//            br.append(loggerLabel).append(new Date()).
//                    append("\nERROR checkStockTotalOfStaff: ").
//                    append(sql).append("\n")
//                    .append(" ownerId ")
//                    .append(ownerId)
//                    .append(" result ")
//                    .append(result);
//            logger.error(br + ex.toString());
//        } finally {
//            closeStatement(ps);
//            closeResultSet(rs);
//            closeConnection(connection);
//            return result;
//        }
//    }

    public boolean checkIdNoOfSubscriber(String isdn, String idNo) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        boolean result = false;
        StringBuilder br = new StringBuilder();
        String sql = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "select * from sub_mb where isdn = ? and status= 2 "
                    + "and cust_id in (select cust_id from customer where lower(id_no) = lower(?))";
            ps = connection.prepareStatement(sql);
            if (isdn.startsWith("258")) {
                isdn = isdn.substring(3);
            }
            ps.setString(1, isdn);
            ps.setString(2, idNo);

            rs = ps.executeQuery();
            while (rs.next()) {
                String otpResult = rs.getString("isdn");
                if (otpResult != null && otpResult.length() > 0) {
                    result = true;
                    break;
                }
            }
            logger.info("End checkIdNoOfSubscriber isdn " + isdn + "id_no: " + idNo + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR checkIdNoOfSubscriber: ").
                    append(sql).append("\n")
                    .append(" isdn ")
                    .append(isdn)
                    .append(" id_no ")
                    .append(idNo)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public boolean checkCorrectProfile(String isdn) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        boolean result = false;
        StringBuilder br = new StringBuilder();
        String sql = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "select a.isdn from cm_pre.sub_profile_info a, cm_pre.sub_mb b where a.isdn = b.isdn and b.status = 2 \n"
                    + "and a.cust_id = b.cust_id and a.isdn = ? and a.check_status = 1";
            ps = connection.prepareStatement(sql);
            ps.setString(1, isdn);
            rs = ps.executeQuery();
            while (rs.next()) {
                String otpResult = rs.getString("isdn");
                if (otpResult != null && otpResult.length() > 0) {
                    result = true;
                    break;
                }
            }
            logger.info("End checkCorrectProfile isdn " + isdn + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR checkCorrectProfile: ").
                    append(sql).append("\n")
                    .append(" isdn ")
                    .append(isdn)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public boolean checkCorrectOldProfile(String isdn) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        boolean result = false;
        StringBuilder br = new StringBuilder();
        String sql = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "SELECT Action_Profile_ID AS actionProfileId, isdn_account as isdn FROM profile.Action_Profile WHERE isdn_account = ? AND check_info = '0' AND check_status = 1";
            ps = connection.prepareStatement(sql);
            ps.setString(1, isdn);
            rs = ps.executeQuery();
            while (rs.next()) {
                String otpResult = rs.getString("isdn");
                if (otpResult != null && otpResult.length() > 0) {
                    result = true;
                    break;
                }
            }
            logger.info("End checkCorrectOldProfile isdn " + isdn + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR checkCorrectOldProfile: ").
                    append(sql).append("\n")
                    .append(" isdn ")
                    .append(isdn)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public Customer getCustomerInfo(long custId) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        long startTime = System.currentTimeMillis();
        Customer sub = null;
        try {
            connection = getConnection("cm_pre");
            sql = "SELECT * FROM CM_PRE.CUSTOMER WHERE CUST_ID =?";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, custId);
            rs = ps.executeQuery();
            while (rs.next()) {
                sub = new Customer();
                sub.setCustId(custId);
                sub.setSubName(retNull(rs.getString("name")));
                sub.setGender(retNull(rs.getString("sex")));
                sub.setBirthDate(retNull(convertDateToString(rs.getDate("birth_date"))));
                sub.setIdType(retNull(rs.getString("id_type")));
                sub.setIdNo(retNull(rs.getString("id_no")));
                sub.setAddress(retNull(rs.getString("address")));
            }
            logger.info("End getCustomerInfo custId " + custId + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR getCustomerInfo: ").
                    append(sql).append("\n")
                    .append(" custId ")
                    .append(custId);
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return sub;
        }
    }

    public int saveActionLogPrWS(ActionLogPr log) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;

        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = " INSERT INTO ACTION_LOG_PR (ID,ISDN,CREATE_DATE,SHOP_CODE,USER_NAME,REQUEST,RESPONSE,RESPONSE_CODE, SERIAL,IMSI) "
                    + "VALUES(SEQ_ACTION_LOG_PR.NEXTVAL,?,SYSDATE,?,?,?,?,?,?,?) ";
            ps = connection.prepareStatement(sql);
            ps.setString(1, log.getIsdn());
            ps.setString(2, log.getShopCode());
            ps.setString(3, log.getUserName());
            ps.setString(4, log.getRequest());
            ps.setString(5, log.getResponse());
            ps.setString(6, log.getResponseCode());
            ps.setString(7, log.getSerial());
            ps.setString(8, log.getImsi());
            result = ps.executeUpdate();
            logger.info("End insertACTION_LOG_PR msisdn " + log.getIsdn() + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertACTION_LOG_PR: ").
                    append(sql).append("\n")
                    .append(" msisdn ")
                    .append(log.getIsdn())
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public static String convertDateToString(Date date) throws Exception {
        if (date == null) {
            return null;
        }
        SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy");
        try {
            return dateFormat.format(date);
        } catch (Exception e) {
            throw e;
        }
    }

    public static String retNull(Object ob) {
        if (ob == null) {
            return "";
        } else {
            return ob.toString().trim();
        }

    }
}
