/*
 * Copyright 2012 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.vas.wsfw.database;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.smsfw.manager.AppManager;
import com.viettel.smsfw.database.DbProcessorAbstract;
import static com.viettel.smsfw.database.DbProcessorAbstract.QUERY_TIMEOUT;
import com.viettel.vas.util.ConnectionPoolManager;
import com.viettel.vas.util.PoolStore;
import com.viettel.vas.wsfw.object.Revenue;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.ResourceBundle;
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
public class DbChannelProcessor extends DbProcessorAbstract {

    private String loggerLabel = DbChannelProcessor.class.getSimpleName() + ": ";
    private PoolStore poolStore;
    private String dbNameCofig;

    public DbChannelProcessor() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
        dbNameCofig = ResourceBundle.getBundle("vas").getString("dbBockd");
        poolStore = new PoolStore(dbNameCofig, logger);
    }

    public DbChannelProcessor(String sessionName, Logger logger) throws SQLException, Exception {
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

    private void closePrepareStatement(PreparedStatement ps) {
        try {
            if (ps != null) {
                ps.close();
                ps = null;
            }
        } catch (Exception e) {
            ps = null;
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
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int[] deleteQueue(List<Record> listRecords) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int[] insertQueueHis(List<Record> listRecords) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int[] insertQueueOutput(List<Record> listRecords) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void processTimeoutRecord(List<String> ids) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public String validateInfo(String staffCode) {
        long timeSt = System.currentTimeMillis();
        Connection connection = null;
        ResultSet rs1 = null;
        String result = "";
        String sql = "SELECT IMG_PATH,IMG_URL FROM STAFF_LOCATION WHERE UPPER(STAFF_CODE)=? ";
        PreparedStatement ps = null;
        try {
            connection = ConnectionPoolManager.getConnection("dbBockd");
            ps = connection.prepareStatement(sql);
            if (QUERY_TIMEOUT > 0) {
                ps.setQueryTimeout(QUERY_TIMEOUT);
            }
            ps.setString(1, staffCode);
            rs1 = ps.executeQuery();
            while (rs1.next()) {
                result = rs1.getString("IMG_PATH") + "/" + rs1.getString("IMG_URL");
                continue;
            }
        } catch (Exception ex) {
            logger.error("ERROR get with staff code " + staffCode);
            logger.error(AppManager.logException(timeSt, ex));
            result = "";
        } finally {
            closeStatement(ps);
            closeConnection(connection);
        }
        return result;
    }

//    public List<Revenue> getSaleRevenue(String user) {
//        long timeSt = System.currentTimeMillis();
//        ArrayList<Revenue> lstRevenue = new ArrayList<Revenue>();
//        Connection connection = null;
//        ResultSet rs1 = null;        
//        String sql = "  SELECT   shop_id_lv3,\n"
//                + "           shop_code_lv3,\n"
//                + "           shop_name_lv3,\n"
//                + "           shop_id_lv4,\n"
//                + "           shop_code_lv4,\n"
//                + "           shop_name_lv4,\n"
//                + "           shop_id_lv5,\n"
//                + "           shop_code_lv5,\n"
//                + "           shop_name_lv5,\n"
//                + "           sale_trans_type,\n"
//                + "           sale_trans_type_name,\n"
//                + "           stock_type_id,\n"
//                + "           stock_type_name,\n"
//                + "           stock_model_id,\n"
//                + "           stock_model_code,\n"
//                + "           stock_model_name,\n"
//                + "           accounting_model_code,\n"
//                + "           accounting_model_name,\n"
//                + "           price,\n"
//                + "           price_vat,\n"
//                + "           SUM (quantity) AS quantity,\n"
//                + "           SUM (amount) AS amount,\n"
//                + "           SUM (NVL (discount_amount, 0)) AS discount_amount,\n"
//                + "           SUM (amount_before_tax) AS amount_before_tax,\n"
//                + "           SUM (amount_tax) AS amount_tax,\n"
//                + "           SUM (amount_after_tax) AS amount_after_tax, \n"
//                + "            trunc(ADD_MONTHS(sysdate,-1)) as cycleRev, "
//                + "            to_char(sysdate,'yyyy-MM-dd hh24:mi:ss') transTime"
//                + "    FROM   v_revenue\n"
//                + "   WHERE       1 = 1\n"
//                + "           AND sale_trans_date >= trunc(ADD_MONTHS(sysdate,-1)) \n"
//                + "           AND sale_trans_date < trunc(sysdate, 'MON') \n"
//                + "           AND sale_trans_date_detail >= trunc(ADD_MONTHS(sysdate,-1)) \n"
//                + "           AND sale_trans_date_detail < trunc(sysdate, 'MON') \n"
//                + "           AND (   shop_id_lv2 = 7282\n"
//                + "                OR shop_id_lv3 = 7282\n"
//                + "                OR shop_id_lv4 = 7282\n"
//                + "                OR shop_id_lv5 = 7282)\n"
//                + "           AND (channel_type_id = -1\n"
//                + "                OR channel_type_id IN\n"
//                + "                          (SELECT   channel_type_id\n"
//                + "                             FROM   channel_type\n"
//                + "                            WHERE   is_vt_unit = 1 AND object_type = 2))\n"
//                + "           AND (   sale_trans_status = -1\n"
//                + "                OR sale_trans_status = 3\n"
//                + "                OR sale_trans_status = 2)\n"
//                + "           AND (price_vat = -1 OR price_vat > 0)\n"
//                + " GROUP BY   shop_id_lv3,\n"
//                + "           shop_code_lv3,\n"
//                + "           shop_name_lv3,\n"
//                + "           shop_id_lv4,\n"
//                + "           shop_code_lv4,\n"
//                + "           shop_name_lv4,\n"
//                + "           shop_id_lv5,\n"
//                + "           shop_code_lv5,\n"
//                + "           shop_name_lv5,\n"
//                + "           sale_trans_type,\n"
//                + "           sale_trans_type_name,\n"
//                + "           stock_type_id,\n"
//                + "           stock_type_name,\n"
//                + "           stock_model_id,\n"
//                + "           stock_model_code,\n"
//                + "           stock_model_name,\n"
//                + "           accounting_model_code,\n"
//                + "           accounting_model_name,\n"
//                + "           price,\n"
//                + "           price_vat\n"
//                + " ORDER BY   shop_code_lv3,\n"
//                + "           shop_code_lv4,\n"
//                + "           shop_code_lv5,\n"
//                + "           sale_trans_type,\n"
//                + "           stock_type_id,\n"
//                + "           stock_model_code \n"
//                + "\n"
//                + "           \n"
//                + "           \n"
//                + "           \n"
//                + "           ";
//        PreparedStatement ps = null;
//        try {
//            connection = ConnectionPoolManager.getConnection("dbsm");
//            ps = connection.prepareStatement(sql);
//            if (QUERY_TIMEOUT > 0) {
//                ps.setQueryTimeout(QUERY_TIMEOUT);
//            }
//            rs1 = ps.executeQuery();
//            while (rs1.next()) {
//                Revenue rev = new Revenue();
//                rev.setAmount(rs1.getString("amount"));
//                rev.setAmountAfterTax(rs1.getString("amount_after_tax"));
//                rev.setAmountBeforeTax(rs1.getString("amount_before_tax"));
//                rev.setAmountTax(rs1.getString("amount_tax"));
//                rev.setCycle(rs1.getString("cycleRev"));
//                rev.setDiscount(rs1.getString("discount_amount"));
//                rev.setPrice(rs1.getString("price"));
//                rev.setQuantity(rs1.getString("quantity"));
//                rev.setRevenueType("Sale");
//                rev.setSaleTransTypeName(rs1.getString("sale_trans_type_name"));
//                rev.setStockTypeName(rs1.getString("stock_type_name"));
//                rev.setStockModelCode(rs1.getString("stock_model_code"));
//                rev.setStockModelName(rs1.getString("stock_model_name"));
//                rev.setTransTime(rs1.getString("transTime"));
//                rev.setUnitCodeLever1(rs1.getString("shop_code_lv3"));
//                rev.setUnitCodeLever2(rs1.getString("shop_code_lv4"));
//                rev.setUnitCodeLever3(rs1.getString("shop_code_lv5"));
//                lstRevenue.add(rev);
//            }
//        } catch (Exception ex) {
//            logger.error("ERROR getRevenue user " + user);
//            logger.error(AppManager.logException(timeSt, ex));            
//        } finally {
//            closeResultSet(rs1);
//            closeStatement(ps);
//            closeConnection(connection);
//            return lstRevenue;
//        }
//    }
}
