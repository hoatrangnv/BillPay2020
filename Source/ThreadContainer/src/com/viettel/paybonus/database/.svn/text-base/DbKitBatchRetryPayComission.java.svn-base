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
import com.viettel.vas.util.PoolStore;
import com.viettel.vas.util.obj.Param;
import com.viettel.vas.util.obj.ParamList;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
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
public class DbKitBatchRetryPayComission extends DbProcessorAbstract {

//    private Logger logger;
    private String loggerLabel = DbKitBatchRetryPayComission.class.getSimpleName() + ": ";
//    private StringBuffer br = new StringBuffer();
    private PoolStore poolStore;
    private String dbNameCofig;
    private String sqlDeleteMo = "update cm_pre.kit_batch_info set status = 1, process_time = sysdate, "
            + "result_code = ?, description = ?, duration = ?, total_success = ?, total_fail = ?  where kit_batch_id = ?";

    public DbKitBatchRetryPayComission() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
//        dbNameCofig = ResourceBundle.getBundle("configPayBonus").getString("dbNameConfig");
        dbNameCofig = "cm_pre";
        poolStore = new PoolStore(dbNameCofig, logger);
    }

    public DbKitBatchRetryPayComission(String sessionName, Logger logger) throws SQLException, Exception {
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
            record.setGroupName(rs.getString("group_name") != null ? rs.getString("group_name") : "N/A");
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
                ps.setString(1, sd.getResultCode());
                ps.setString(2, sd.getDescription());
                ps.setLong(3, sd.getDuration());
                ps.setLong(4, sd.getTotalSuccess());
                ps.setLong(5, sd.getTotalFail());
                ps.setLong(6, sd.getKitBatchId());
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
                ps.setString(1, "FW_99");
                ps.setString(2, "FW_Timeout");
                ps.setLong(3, 0L);
                ps.setLong(4, 0);
                ps.setLong(5, 0);
                ps.setLong(6, Long.valueOf(id));
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

    public List<KitBatch> getListKitBatchConnectSuccess(Long kitBatchId) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        List<KitBatch> lstKitBatch = new ArrayList<KitBatch>();
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            String sql = "select * from kit_batch_detail where kit_batch_id = ? and result_code = '0'";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, kitBatchId);
            rs = ps.executeQuery();
            while (rs.next()) {
                String serial = rs.getString("serial");
                String isdn = rs.getString("isdn");
                String productCode = rs.getString("product_code");
                String handsetSerial = rs.getString("handset_serial");
                KitBatch kitBatch = new KitBatch();
                kitBatch.setSerial(serial);
                kitBatch.setKitBatchId(kitBatchId);
                kitBatch.setIsdn(isdn);
                kitBatch.setProductCode(productCode);
                kitBatch.setHandsetSerial(handsetSerial);
                kitBatch.setCustId(rs.getLong("cust_id"));
                lstKitBatch.add(kitBatch);

            }
            logger.info("End getListKitBatchDetail: kitBatchId" + kitBatchId + " size " + lstKitBatch.size() + " time: "
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

    public int updateKitBatchDetail(Long kitBatchId, String serial, String isdn, String resultCode, String description,
            String nodeName, String moneyProduct, String moneyIsdn) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder brBuilder = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "update kit_batch_detail set result_code = ?, description = ?, node_name = ?, money_product = ?, money_isdn = ?, process_time = sysdate \n"
                    + "where kit_batch_id = ? and serial = ? and isdn = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, resultCode);
            ps.setString(2, description);
            ps.setString(3, nodeName != null ? nodeName : "");
            ps.setString(4, moneyProduct != null ? moneyProduct : "");
            ps.setString(5, moneyIsdn != null ? moneyIsdn : "");
            ps.setLong(6, kitBatchId);
            ps.setString(7, serial);
            ps.setString(8, isdn);
            result = ps.executeUpdate();
            logger.info("End updateKitBatchDetail kitBatchId " + kitBatchId + ", serial: " + serial + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            brBuilder.setLength(0);
            brBuilder.append(loggerLabel).append(new Date()).
                    append("\nERROR updateKitBatchDetail: ").
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

    public String getShopCodeByStaffCode(String staffCode) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String shopCode = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            String sql = "select * from sm.shop where shop_id = (select shop_id from sm.staff where upper(staff_code) = upper(?) and status = 1)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, staffCode);
            rs = ps.executeQuery();
            while (rs.next()) {
                shopCode = rs.getString("shop_code");
            }
            logger.info("End getShopCodeByStaffCode with staffCode: " + staffCode + "time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getShopCodeByStaffCode: ------ staffCode: ").
                    append(staffCode).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return shopCode;
        }
    }

    @Override
    public void updateSqlMoParam(List<Record> lrc) {
        throw new UnsupportedOperationException("Not supported yet.");
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

    public int insertEwalletLog(EwalletLog log) {

        ParamList paramList = new ParamList();
        long timeSt = System.currentTimeMillis();
        try {
            paramList.add(new Param("ACTION_AUDIT_ID", log.getAtionAuditId(), Param.DataType.LONG, Param.IN));
            paramList.add(new Param("STAFF_CODE", log.getStaffCode(), Param.DataType.STRING, Param.IN));
            paramList.add(new Param("CHANNEL_TYPE_ID", log.getChannelTypeId(), Param.DataType.INT, Param.IN));
            paramList.add(new Param("MOBILE", log.getMobile(), Param.DataType.STRING, Param.IN));
            paramList.add(new Param("TRANS_ID", log.getTransId(), Param.DataType.STRING, Param.IN));
            paramList.add(new Param("ACTION_CODE", log.getActionCode(), Param.DataType.STRING, Param.IN));
            paramList.add(new Param("AMOUNT", log.getAmount(), Param.DataType.LONG, Param.IN));
            paramList.add(new Param("FUNCTION_NAME", log.getFunctionName(), Param.DataType.STRING, Param.IN));
            paramList.add(new Param("URL", log.getUrl(), Param.DataType.STRING, Param.IN));
            paramList.add(new Param("USERNAME", log.getUserName(), Param.DataType.STRING, Param.IN));
            paramList.add(new Param("REQUEST", log.getRequest(), Param.DataType.STRING, Param.IN));
            paramList.add(new Param("RESPONSE", log.getRespone(), Param.DataType.STRING, Param.IN));
            paramList.add(new Param("DURATION", log.getDuration(), Param.DataType.LONG, Param.IN));
            paramList.add(new Param("ERROR_CODE", log.getErrorCode(), Param.DataType.STRING, Param.IN));
            paramList.add(new Param("DESCRIPTION", log.getDescription(), Param.DataType.STRING, Param.IN));
            paramList.add(new Param("BONUS_TYPE", 4L, Param.DataType.LONG, Param.IN));
            PoolStore.PoolResult prs = poolStore.insertTable(paramList, "PROFILE.EWALLET_LOG");
            logTimeDb("Time to insertEwalletLog isdn " + log.getMobile() + " result insert: " + (prs == PoolStore.PoolResult.SUCCESS ? 0 : -1), timeSt);
            return prs == PoolStore.PoolResult.SUCCESS ? 0 : -1;
        } catch (Exception ex) {
            logger.error("ERROR insertEwalletLog default return -1: isdn " + log.getMobile());
            logger.error(AppManager.logException(timeSt, ex));
            return -1;
        }
    }

    public Long getReasonIdByProductCode(String productCode, String isdn) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        Long reasonId = null;
        String sqlMo = "select * from Reason r where r.status = 1 and r.type = '00'\n"
                + "and r.reason_Id in (select m.reason_Id from Mapping m where (upper(m.product_Code) = upper(?) or m.product_Code is null) \n"
                + "and m.tel_Service_Id = 1 and m.status = 1 \n"
                + "and ( m.end_Date is null or m.end_Date >= trunc(sysdate))\n"
                + ")\n"
                + "order by NLSSORT(r.code,'NLS_SORT=vietnamese')";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection("cm_pre");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setString(1, productCode);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                reasonId = rs1.getLong("reason_id");
                logger.info("Get reason_i " + reasonId + " base on productCode " + productCode + " for isdn " + isdn);
            }
            logTimeDb("Time to getReasonIdByProductCode: " + reasonId
                    + " base on productCode " + productCode + " for isdn " + isdn, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getReasonIdByProductCode " + reasonId);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
            return reasonId;
        }
    }

    public String getSaleServiceCode(Long reasonId, String productCode, String isdn) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        String saleServiceCode = null;
        String sqlMo = "Select * from Mapping m, Reason r \n"
                + "where m.reason_Id = r.reason_Id and r.status = 1 and m.status = 1 \n"
                + "and m.channel is null and m.reason_Id = ? and (upper(m.product_Code) = upper(?) or m.product_Code is null)";
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

    public Price getPrice(String isdn, Long saleServicesId) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        Price priceObj = null;
        String sqlMo = "select * from price where 1 = 1 and price_id = (select price_id from Sale_Services_Detail where stock_model_id = (select stock_model_id from stock_isdn_mobile where isdn = ?) and status= 1 \n"
                + "and sale_Services_Model_Id in (select sale_Services_Model_Id from Sale_Services_Model where sale_Services_Id = ? and stock_Type_Id = 1 )) \n"
                + "and status = 1 and trunc(sta_date) <= trunc(sysdate) and (end_Date is null or trunc(end_Date) >= trunc(sysdate))";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection("dbsm");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setString(1, isdn);
            psMo.setLong(2, saleServicesId);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                Long priceId = rs1.getLong("price_id");
                Long stockModelId = rs1.getLong("stock_model_id");
                Double price = rs1.getDouble("price");
                priceObj = new Price();
                priceObj.setPriceId(priceId);
                priceObj.setStockModelId(stockModelId);
                priceObj.setPrice(price);
                logger.info("Result getPrice " + saleServicesId + " isdn " + isdn + " priceId " + priceId);
                break;
            }
            logTimeDb("Time to getPrice: " + saleServicesId + " isdn " + isdn, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getPrice " + saleServicesId + " isdn " + isdn);
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

    public String getIsdnWalletByStaffCode(String staffCode) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        String isdnWallet = "";
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
            isdnWallet = "";
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return isdnWallet;
        }
    }

    public int insertActionAudit(Long actionAuditId, String des, long subId, String shopCode, String userName) {
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
                    + " VALUES(?,sysdate,'00',4432,?,?, "
                    + "'1',?,'127.0.0.1',?)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, actionAuditId);
            ps.setString(2, shopCode);
            ps.setString(3, userName);
            ps.setLong(4, subId);
            ps.setString(5, des);
            result = ps.executeUpdate();
            logger.info("End insertActionAudit actionAuditId " + actionAuditId
                    + " subId " + subId + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertActionAudit: ").
                    append(sql).append("\n")
                    .append(" actionAuditId ")
                    .append(actionAuditId)
                    .append(" subId ")
                    .append(subId)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public Comission getComissionStaff(String staffCode, String productCode, String isdn) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        Comission comission = null;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbProduct");
            String sql = "select * from product.product_connect_kit where upper(product_code) = upper(?) and status = 1 and vip_product= 1";
            ps = connection.prepareStatement(sql);
            ps.setString(1, productCode);
            rs = ps.executeQuery();
            while (rs.next()) {
                long bonusCenter = rs.getLong("bonus_center");
                long bonusCtv = rs.getLong("bonus_ctv");
                long bonusChannel = rs.getLong("bonus_channel");
                long bonusCtvCenter = rs.getLong("bonus_ctv_center");
                long bonusChannelCtv = rs.getLong("bonus_channel_ctv");
                long bonusChanelCenter = rs.getLong("bonus_channel_center");
                comission = new Comission();
                comission.setBonusCenter(bonusCenter);
                comission.setBonusCtv(bonusCtv);
                comission.setBonusChannel(bonusChannel);
                comission.setBonusCtvCenter(bonusCtvCenter);
                comission.setBonusChannelCtv(bonusChannelCtv);
                comission.setBonusChannelCenter(bonusChanelCenter);
                break;
            }
            logger.info("End getBonusDirectorCenter: staffCode " + staffCode + " isdn " + isdn + " time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getStaffType ").append(staffCode).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
            comission = null;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return comission;
        }
    }
}
