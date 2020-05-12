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
import com.viettel.vas.wsfw.object.RvnService;
import com.viettel.vas.wsfw.object.Shop;
import com.viettel.vas.wsfw.object.StockModel;
import com.viettel.vas.wsfw.object.StockType;
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
public class DbSyncRevenueBccs extends DbProcessorAbstract {

    private String loggerLabel = DbSyncRevenueBccs.class.getSimpleName() + ": ";
    private PoolStore poolStore;
    private String dbNameCofig;
    private final String MONEY_RATE = "1";
    private final String CURRENTCY = "MT";
    private final String VAT = "17";

    public DbSyncRevenueBccs() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
        dbNameCofig = ResourceBundle.getBundle("vas").getString("dbsm");
        poolStore = new PoolStore(dbNameCofig, logger);
    }

    public DbSyncRevenueBccs(String sessionName, Logger logger) throws SQLException, Exception {
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

    public List<Revenue> getBccsSaleRevenueCloseCycle(String starOfCycle, String endCycle, int revType) {
        long timeSt = System.currentTimeMillis();
        List<Revenue> lstRevenue = new ArrayList<Revenue>();
        Connection connection = null;
        ResultSet rs1 = null;
        String sqlViewDetail = "SELECT   0 AS is_service,\n"
                + "         tmp.shop_id_lv2,\n"
                + "         tmp.shop_id_lv3,\n"
                + "         tmp.shop_code_lv3,\n"
                + "         tmp.shop_name_lv3,\n"
                + "         tmp.shop_id_lv4,\n"
                + "         tmp.shop_code_lv4,\n"
                + "         tmp.shop_name_lv4,\n"
                + "         tmp.shop_id_lv5,\n"
                + "         tmp.shop_code_lv5,\n"
                + "         tmp.shop_name_lv5,\n"
                + "         sf.staff_id,\n"
                + "         sf.staff_code AS staff_code,\n"
                + "         sf.name AS staff_name,\n"
                + "         sf.channel_type_id,\n"
                + "         st.trans_time,\n"
                + "         std.sale_trans_date AS sale_trans_date_detail,\n"
                + "         st.trans_type,\n"
                + "         sit.name AS sale_trans_type_name,\n"
                + "         st.status AS sale_trans_status,\n"
                + "         st.shop_id,\n"
                + "         std.stock_type_id,\n"
                + "         std.stock_type_name,\n"
                + "         std.stock_model_id,\n"
                + "         std.stock_model_code,\n"
                + "         std.stock_model_name,\n"
                + "         std.accounting_model_code,\n"
                + "         std.accounting_model_name,\n"
                + "         std.quantity,\n"
                + "         std.price,\n"
                + "         std.price_vat,\n"
                + "         std.amount,\n"
                + "         nvl(std.discount_amount,0) discount_amount,\n"
                + "         std.amount_before_tax,\n"
                + "         std.amount_tax,\n"
                + "         std.amount_after_tax,\n"
                + "         st.revenue_type,\n"
                + "         st.tel_service_id,\n"
                + "			(select a1.service_id from vtict_report.rvn_service_dept_map a1,vtict_report.rvn_service a2 ,sm.stock_model a3 \n" +
				"            where   a3.stock_model_id = std.stock_model_id and lower(a1.goods_code) = lower(a3.stock_model_code) and a1.service_id= a2.id and a2.service_type_id =1 and a1.status =1 and a2.status =1  and rownum <2) as rnv_service_id \n"
                + "  FROM   sm.stock_owner_tmp tmp,\n"
                + "         sm.SYN_BCCS_EPR_DATA st,\n"
                + "         sm.sale_trans_detail std,\n"
                + "         sm.sale_invoice_type sit,\n"
                + "         sm.staff sf\n"
                + " WHERE       1 = 1\n"
                + "         AND tmp.owner_type = 1\n"
                + "         AND tmp.owner_id = st.shop_id\n"
                + "         AND st.staff_id = sf.staff_id\n"
                + "         AND st.trans_id = std.sale_trans_id\n"
                + "         AND st.trans_type = sit.sale_trans_type\n"
                + "         AND std.stock_model_id IS NOT NULL\n"
                + "         AND st.sta_cycle = to_date('" + starOfCycle + "','dd-MM-yyyy')\n "
                + "UNION ALL\n"
                + "SELECT   1 AS is_service,\n"
                + "         tmp.shop_id_lv2,\n"
                + "         tmp.shop_id_lv3,\n"
                + "         tmp.shop_code_lv3,\n"
                + "         tmp.shop_name_lv3,\n"
                + "         tmp.shop_id_lv4,\n"
                + "         tmp.shop_code_lv4,\n"
                + "         tmp.shop_name_lv4,\n"
                + "         tmp.shop_id_lv5,\n"
                + "         tmp.shop_code_lv5,\n"
                + "         tmp.shop_name_lv5,\n"
                + "         sf.staff_id,\n"
                + "         sf.staff_code AS staff_code,\n"
                + "         sf.name AS staff_name,\n"
                + "         sf.channel_type_id,\n"
                + "         st.trans_time,\n"
                + "         std.sale_trans_date AS sale_trans_date_detail,\n"
                + "         st.trans_type,\n"
                + "         sit.name AS sale_trans_type_name,\n"
                + "         st.status AS sale_trans_status,\n"
                + "         st.shop_id,\n"
                + "         -1 AS stock_type_id,\n"
                + "         'Sale Service' AS stock_type_name,\n"
                + "         std.sale_services_id AS stock_model_id,\n"
                + "         std.sale_services_code AS stock_model_code,\n"
                + "         std.sale_services_name AS stock_model_name,\n"
                + "         std.accounting_model_code,\n"
                + "         std.accounting_model_name,\n"
                + "         std.quantity,\n"
                + "         std.sale_services_price AS price,\n"
                + "         std.sale_services_price_vat AS price_vat,\n"
                + "         std.amount,\n"
                + "         nvl(std.discount_amount,0) discount_amount,\n"
                + "         std.amount_before_tax,\n"
                + "         std.amount_tax,\n"
                + "         std.amount_after_tax,\n"
                + "         st.revenue_type,\n"
                + "         st.tel_service_id,\n"
                + "			(select a1.service_id from vtict_report.rvn_service_dept_map a1,vtict_report.rvn_service a2\n" +
				"            where lower(a1.goods_code) = lower(std.sale_services_code) and a1.service_id= a2.id and a2.service_type_id =1 and a1.status =1 and a2.status =1  and rownum <2) as rnv_service_id \n"
                + "  FROM   sm.stock_owner_tmp tmp,\n"
                + "         sm.SYN_BCCS_EPR_DATA st,\n"
                + "         sm.sale_trans_detail std,\n"
                + "         sm.sale_invoice_type sit,\n"
                + "         sm.staff sf\n"
                + " WHERE       1 = 1\n"
                + "         AND tmp.owner_type = 1\n"
                + "         AND tmp.owner_id = st.shop_id\n"
                + "         AND st.staff_id = sf.staff_id\n"
                + "         AND st.trans_id = std.sale_trans_id\n"
                + "         AND st.trans_type = sit.sale_trans_type\n"
                + "         AND std.stock_model_id IS NULL"
                + "         AND st.sta_cycle = to_date('" + starOfCycle + "','dd-MM-yyyy')\n "
                + "";

        String mainSql = "  SELECT   shop_id_lv3,\n"
                + "           shop_code_lv3,\n"
                + "           shop_name_lv3,\n"
                + "           shop_id_lv4,\n"
                + "           shop_code_lv4,\n"
                + "           shop_name_lv4,\n"
                + "           shop_id_lv5,\n"
                + "           shop_code_lv5,\n"
                + "           shop_name_lv5,\n"
                + "           trans_type,\n"
                + "           sale_trans_type_name,\n"
                + "           stock_type_id,\n"
                + "           stock_type_name,\n"
                + "           stock_model_id,\n"
                + "           stock_model_code,\n"
                + "           stock_model_name,\n"
                + "           accounting_model_code,\n"
                + "           accounting_model_name,\n"
                + "           price,\n"
                + "           price_vat,\n"
                + "           SUM (quantity) AS quantity,\n"
                + "           SUM (amount) AS amount,\n"
                + "           SUM (NVL (discount_amount, 0)) AS discount_amount,\n"
                + "           SUM (amount_before_tax) AS amount_before_tax,\n"
                + "           SUM (amount_tax) AS amount_tax,\n"
                + "           SUM (amount_after_tax) AS amount_after_tax,\n"
                + "           'Unit' as unit,\n"
                + "           decode(tel_service_id,1,53,5,89,12,94,15,84,tel_service_id ) tel_service_id,\n"
                + "           decode(rnv_service_id,269,265,266,265,256,265,263,265,304,265,255,265,rnv_service_id) rnv_service_id,\n"
                + "           (select a1.name from vtict_report.rvn_service a1 where  a1.id = (decode(rnv_service_id,269,265,266,265,256,265,263,265,304,265,255,265,rnv_service_id)) ) as rnv_service_name\n"
                + "            "
                + "    FROM   (\n" + sqlViewDetail + ")\n"
                + "   WHERE       1 = 1\n"
                + "           AND trans_time >= to_date('" + starOfCycle + " 00:00:00','dd-MM-yyyy HH24:MI:SS') \n"
                + "           AND trans_time <=  to_date('" + endCycle + " 23:59:59','dd-MM-yyyy HH24:MI:SS') \n"
                + "           AND sale_trans_date_detail >= to_date('" + starOfCycle + " 00:00:00','dd-MM-yyyy HH24:MI:SS') \n"
                + "           AND sale_trans_date_detail <=  to_date('" + endCycle + " 23:59:59','dd-MM-yyyy HH24:MI:SS') \n"
                + "           AND (   shop_id_lv2 = 7282\n"
                + "                OR shop_id_lv3 = 7282\n"
                + "                OR shop_id_lv4 = 7282\n"
                + "                OR shop_id_lv5 = 7282)\n"
                + "           AND (channel_type_id = -1\n"
                + "                OR channel_type_id IN\n"
                + "                          (SELECT   channel_type_id\n"
                + "                             FROM   channel_type\n"
                + "                            WHERE   is_vt_unit in (1,2) AND object_type = 2))\n"
                + "           AND (   sale_trans_status = -1\n"
                + "                OR sale_trans_status = 3\n"
                + "                OR sale_trans_status = 2)\n"
                + "           AND (price_vat = -1 OR price_vat > 0"
                + "			  ) AND revenue_type =" + revType + " \n"
                + " GROUP BY   shop_id_lv3,\n"
                + "           shop_code_lv3,\n"
                + "           shop_name_lv3,\n"
                + "           shop_id_lv4,\n"
                + "           shop_code_lv4,\n"
                + "           shop_name_lv4,\n"
                + "           shop_id_lv5,\n"
                + "           shop_code_lv5,\n"
                + "           shop_name_lv5,\n"
                + "           trans_type,\n"
                + "           sale_trans_type_name,\n"
                + "           stock_type_id,\n"
                + "           stock_type_name,\n"
                + "           stock_model_id,\n"
                + "           stock_model_code,\n"
                + "           stock_model_name,\n"
                + "           accounting_model_code,\n"
                + "           accounting_model_name,\n"
                + "           price,\n"
                + "           price_vat,\n"
                + "			  rnv_service_id,\n"
                + "           tel_service_id\n"
                + " ORDER BY   shop_code_lv3,\n"
                + "           shop_code_lv4,\n"
                + "           shop_code_lv5,\n"
                + "           trans_type,\n"
                + "           stock_type_id,\n"
                + "           stock_model_code \n"
                + "           ";
        PreparedStatement ps = null;
        try {
            connection = ConnectionPoolManager.getConnection("dbsm");
            ps = connection.prepareStatement(mainSql);
            if (QUERY_TIMEOUT > 0) {
                ps.setQueryTimeout(150);
            }
            rs1 = ps.executeQuery();
            while (rs1.next()) {
                Revenue rev = new Revenue();
                rev.setAmount(retNull(rs1.getString("amount")));
                rev.setAmountAfterTax(retNull(rs1.getString("amount_after_tax")));
                rev.setAmountBeforeTax(retNull(rs1.getString("amount_before_tax")));
                rev.setAmountTax(retNull(rs1.getString("amount_tax")));
                rev.setCycle(starOfCycle);
                rev.setDiscount(retNull(rs1.getString("discount_amount")));
                rev.setPrice(retNull(rs1.getString("price")));
                rev.setQuantity(retNull(rs1.getString("quantity")));
                rev.setRevenueType(retNull(revType));
                rev.setSaleTransTypeName(retNull(rs1.getString("sale_trans_type_name")));
                rev.setStockTypeName(retNull(rs1.getString("stock_type_name")));
                rev.setStockModelCode(retNull(rs1.getString("stock_model_code")));
                rev.setStockModelName(retNull(rs1.getString("stock_model_name")));
                rev.setMoneyRate(MONEY_RATE);
                rev.setCurrency(retNull(CURRENTCY));
                rev.setVat(retNull(VAT));
                rev.setRvnServiceId(retNull(rs1.getString("rnv_service_id")));
                rev.setRvnServiceName(retNull(rs1.getString("rnv_service_name")));
                rev.setUnit(retNull(rs1.getString("unit")));
                rev.setTelecomService(retNull(rs1.getString("tel_service_id")));
                rev.setUnitCodeLever1(retNull(rs1.getString("shop_code_lv3")));
                rev.setUnitCodeLever2(retNull(rs1.getString("shop_code_lv4")));
                rev.setUnitCodeLever3(retNull(rs1.getString("shop_code_lv5")));
                lstRevenue.add(rev);
            }
            return lstRevenue;
        } catch (Exception ex) {
            logger.error("ERROR getRevenue sale");
            logger.error(AppManager.logException(timeSt, ex));
            return null;
        } finally {
            closeResultSet(rs1);
            closeStatement(ps);
            closeConnection(connection);
        }
    }

    public List<Revenue> getBccsSaleRevenueAdjusted(String starOfCycle, String endCycle, int revType) {
        long timeSt = System.currentTimeMillis();
        List<Revenue> lstRevenue = new ArrayList<Revenue>();
        Connection connection = null;
        ResultSet rs1 = null;
        String sqlViewDetail = ""
                + "SELECT   0 AS is_service,\n"
                + "         tmp.shop_id_lv2,\n"
                + "         tmp.shop_id_lv3,\n"
                + "         tmp.shop_code_lv3,\n"
                + "         tmp.shop_name_lv3,\n"
                + "         tmp.shop_id_lv4,\n"
                + "         tmp.shop_code_lv4,\n"
                + "         tmp.shop_name_lv4,\n"
                + "         tmp.shop_id_lv5,\n"
                + "         tmp.shop_code_lv5,\n"
                + "         tmp.shop_name_lv5,\n"
                + "         sf.staff_id,\n"
                + "         sf.staff_code AS staff_code,\n"
                + "         sf.name AS staff_name,\n"
                + "         sf.channel_type_id,\n"
                + "         st.sale_trans_date,\n"
                + "         std.sale_trans_date AS sale_trans_date_detail,\n"
                + "         st.sale_trans_type,\n"
                + "         sit.name AS sale_trans_type_name,\n"
                + "         st.status AS sale_trans_status,\n"
                + "         st.pay_method,\n"
                + "         st.telecom_service_id,\n"
                + "         st.shop_id,\n"
                + "         st.receiver_id,\n"
                + "         st.receiver_type,\n"
                + "         std.stock_type_id,\n"
                + "         std.stock_type_name,\n"
                + "         std.stock_model_id,\n"
                + "         std.stock_model_code,\n"
                + "         std.stock_model_name,\n"
                + "         std.accounting_model_code,\n"
                + "         std.accounting_model_name,\n"
                + "         std.quantity,\n"
                + "         std.price,\n"
                + "         std.price_vat,\n"
                + "         decode(adjust.type_adjusted,1,nvl(std.amount,0),-nvl(std.amount,0)) amount,\n"
                + "         decode(adjust.type_adjusted,1,nvl(std.discount_amount,0),-nvl(std.discount_amount,0)) AS discount_amount,\n"
                + "         decode(adjust.type_adjusted,1,nvl(std.amount_before_tax,0),-nvl(std.amount_before_tax,0)) amount_before_tax ,\n"
                + "         decode(adjust.type_adjusted,1,nvl(std.amount_tax,0),-nvl(std.amount_tax,0)) amount_tax,\n"
                + "         decode(adjust.type_adjusted,1,nvl(std.amount_after_tax,0),-nvl(std.amount_after_tax,0)) amount_after_tax ,\n"
                + "         st.sale_program,"
                + "			st.sale_program_name,\n"
                + "         adjust.type_adjusted,\n"
                + "			st.last_update,\n"
                + "         (select a1.service_id from vtict_report.rvn_service_dept_map a1,vtict_report.rvn_service a2\n" +
				"            where lower(a1.goods_code) = lower(stock_model_code) and a1.service_id= a2.id and a2.service_type_id =1 and a1.status =1 and a2.status =1  and rownum <2) as rnv_service_id\n"
                + "  FROM   stock_owner_tmp tmp,\n"
                + "         sm.sale_trans st,\n"
                + "         sm.sale_trans_detail std,\n"
                + "         sale_invoice_type sit,\n"
                + "         staff sf,\n"
                + "         (select b.sale_trans_id,'0' as type_adjusted from sm.syn_bccs_epr_data a, sm.sale_trans b where a.trans_id = b.sale_trans_id  \n"
                + "        and b.sale_trans_date >=to_date('" + starOfCycle + " 00:00:00','dd-MM-yyyy HH24:MI:SS') and b.sale_trans_date < to_date('" + endCycle + " 23:59:59','dd-MM-yyyy HH24:MI:SS') and a.status <> 4 and  b.status=4\n"
                + "        union all\n"
                + "        select b.sale_trans_id,'1' as type_adjusted from sm.syn_bccs_epr_data a, sm.sale_trans b where a.trans_id(+) = b.sale_trans_id  and b.sale_trans_date >=to_date('" + starOfCycle + " 00:00:00','dd-MM-yyyy HH24:MI:SS') and b.sale_trans_date < to_date('" + endCycle + " 23:59:59','dd-MM-yyyy HH24:MI:SS')\n"
                + "        and b.status <> 4 and a.trans_id is null) adjust\n"
                + " WHERE       1 = 1\n"
                + "          AND st.sale_trans_date >= to_date('" + starOfCycle + " 00:00:00','dd-MM-yyyy HH24:MI:SS')\n"
                + "        AND st.sale_trans_date <  to_date('" + endCycle + " 23:59:59','dd-MM-yyyy HH24:MI:SS')\n"
                + "        AND std.sale_trans_date >= to_date('" + starOfCycle + " 00:00:00','dd-MM-yyyy HH24:MI:SS')\n"
                + "        AND std.sale_trans_date <  to_date('" + endCycle + " 23:59:59','dd-MM-yyyy HH24:MI:SS')\n"
                + "         AND tmp.owner_type = 1\n"
                + "         AND tmp.owner_id = st.shop_id\n"
                + "         AND st.staff_id = sf.staff_id\n"
                + "         AND st.sale_trans_id = std.sale_trans_id\n"
                + "         AND st.sale_trans_type = sit.sale_trans_type\n"
                + "         AND std.stock_model_id IS NOT NULL\n"
                + "         AND st.sale_trans_id = adjust.sale_trans_id\n"
                + "         \n"
                + "        \n"
                + "UNION ALL\n"
                + "SELECT   1 AS is_service,\n"
                + "         tmp.shop_id_lv2,\n"
                + "         tmp.shop_id_lv3,\n"
                + "         tmp.shop_code_lv3,\n"
                + "         tmp.shop_name_lv3,\n"
                + "         tmp.shop_id_lv4,\n"
                + "         tmp.shop_code_lv4,\n"
                + "         tmp.shop_name_lv4,\n"
                + "         tmp.shop_id_lv5,\n"
                + "         tmp.shop_code_lv5,\n"
                + "         tmp.shop_name_lv5,\n"
                + "         sf.staff_id,\n"
                + "         sf.staff_code AS staff_code,\n"
                + "         sf.name AS staff_name,\n"
                + "         sf.channel_type_id,\n"
                + "         st.sale_trans_date,\n"
                + "         std.sale_trans_date AS sale_trans_date_detail,\n"
                + "         st.sale_trans_type,\n"
                + "         sit.name AS sale_trans_type_name,\n"
                + "         st.status AS sale_trans_status,\n"
                + "         st.pay_method,\n"
                + "         st.telecom_service_id,\n"
                + "         st.shop_id,\n"
                + "         NULL AS receiver_id,\n"
                + "         NULL AS receiver_type,\n"
                + "         -1 AS stock_type_id,\n"
                + "         'Sale Service' AS stock_type_name,\n"
                + "         std.sale_services_id AS stock_model_id,\n"
                + "         std.sale_services_code AS stock_model_code,\n"
                + "         std.sale_services_name AS stock_model_name,\n"
                + "         std.accounting_model_code,\n"
                + "         std.accounting_model_name,\n"
                + "          std.quantity,\n"
                + "         std.price,\n"
                + "         std.price_vat,\n"
                + "         decode(adjust.type_adjusted,1,nvl(std.amount,0),-nvl(std.amount,0)) amount,\n"
                + "         decode(adjust.type_adjusted,1,nvl(std.discount_amount,0),-nvl(std.discount_amount,0)) AS discount_amount,\n"
                + "         decode(adjust.type_adjusted,1,nvl(std.amount_before_tax,0),-nvl(std.amount_before_tax,0)) amount_before_tax ,\n"
                + "         decode(adjust.type_adjusted,1,nvl(std.amount_tax,0),-nvl(std.amount_tax,0)) amount_tax,\n"
                + "         decode(adjust.type_adjusted,1,nvl(std.amount_after_tax,0),-nvl(std.amount_after_tax,0)) amount_after_tax ,\n"
                + "         st.sale_program, "
                + "			st.sale_program_name,"
                + "			adjust.type_adjusted,\n"
                + "			st.last_update,\n"
                + "         (select a1.service_id from vtict_report.rvn_service_dept_map a1,vtict_report.rvn_service a2\n" +
				"            where lower(a1.goods_code) = lower(stock_model_code) and a1.service_id= a2.id and a2.service_type_id =1 and a1.status =1 and a2.status =1  and rownum <2) as rnv_service_id\n"
                + "  FROM   stock_owner_tmp tmp,\n"
                + "         sm.sale_trans st,\n"
                + "         sm.sale_trans_detail std,\n"
                + "         sale_invoice_type sit,\n"
                + "         staff sf,\n"
                + "         (select b.sale_trans_id,'0' as type_adjusted from sm.syn_bccs_epr_data a, sm.sale_trans b where a.trans_id = b.sale_trans_id  \n"
                + "        and b.sale_trans_date >=to_date('" + starOfCycle + " 00:00:00','dd-MM-yyyy HH24:MI:SS') and b.sale_trans_date < to_date('" + endCycle + " 23:59:59','dd-MM-yyyy HH24:MI:SS') and a.status <> 4 and  b.status=4 \n"
                + "        union all\n"
                + "        select b.sale_trans_id,'1' as type_adjusted from sm.syn_bccs_epr_data a, sm.sale_trans b where a.trans_id(+) = b.sale_trans_id  and b.sale_trans_date >=to_date('" + starOfCycle + " 00:00:00','dd-MM-yyyy HH24:MI:SS') and b.sale_trans_date < to_date('" + endCycle + " 23:59:59','dd-MM-yyyy HH24:MI:SS')\n"
                + "        and b.status <> 4 and a.trans_id is null) adjust\n"
                + " WHERE       1 = 1\n"
                + "        AND st.sale_trans_date >= to_date('" + starOfCycle + " 00:00:00','dd-MM-yyyy HH24:MI:SS')\n"
                + "        AND st.sale_trans_date <  to_date('" + endCycle + " 23:59:59','dd-MM-yyyy HH24:MI:SS')\n"
                + "        AND std.sale_trans_date >= to_date('" + starOfCycle + " 00:00:00','dd-MM-yyyy HH24:MI:SS')\n"
                + "        AND std.sale_trans_date <  to_date('" + endCycle + " 23:59:59','dd-MM-yyyy HH24:MI:SS')\n"
                + "         AND tmp.owner_type = 1\n"
                + "         AND tmp.owner_id = st.shop_id\n"
                + "         AND st.staff_id = sf.staff_id\n"
                + "         AND st.sale_trans_id = std.sale_trans_id\n"
                + "         AND st.sale_trans_type = sit.sale_trans_type\n"
                + "         AND std.stock_model_id IS NULL\n"
                + "         AND st.sale_trans_id = adjust.sale_trans_id\n"
                + "        \n"
                + "          \n"
                + "";

        String mainSql = "  SELECT   shop_id_lv3,\n"
                + "           shop_code_lv3,\n"
                + "           shop_name_lv3,\n"
                + "           shop_id_lv4,\n"
                + "           shop_code_lv4,\n"
                + "           shop_name_lv4,\n"
                + "           shop_id_lv5,\n"
                + "           shop_code_lv5,\n"
                + "           shop_name_lv5,\n"
                + "           sale_trans_type,\n"
                + "           sale_trans_type_name,\n"
                + "           stock_type_id,\n"
                + "           stock_type_name,\n"
                + "           stock_model_id,\n"
                + "           stock_model_code,\n"
                + "           stock_model_name,\n"
                + "           accounting_model_code,\n"
                + "           accounting_model_name,\n"
                + "           price,\n"
                + "           price_vat,\n"
                + "           SUM (quantity) AS quantity,\n"
                + "           SUM (amount) AS amount,\n"
                + "           SUM (NVL (discount_amount, 0)) AS discount_amount,\n"
                + "           SUM (amount_before_tax) AS amount_before_tax,\n"
                + "           SUM (amount_tax) AS amount_tax,\n"
                + "           SUM (amount_after_tax) AS amount_after_tax, \n"
                + "           telecom_service_id as tel_service_id,"
                + "			  type_adjusted,\n"
                + "           'Unit' as unit,\n"
                + "			  rnv_service_id,"
                + "			to_char(last_update, 'MM-yyyy') adjustTime,\n"
                + "			  (select a1.name from vtict_report.rvn_service a1 where  a1.id = rnv_service_id ) as rnv_service_name"
                + "    FROM   (" + sqlViewDetail + ")\n"
                + "   WHERE       1 = 1\n"
                + "           AND sale_trans_date >= to_date('" + starOfCycle + " 00:00:00','dd-MM-yyyy HH24:MI:SS') \n"
                + "           AND sale_trans_date <  to_date('" + endCycle + " 23:59:59','dd-MM-yyyy HH24:MI:SS') \n"
                + "           AND sale_trans_date_detail >= to_date('" + starOfCycle + " 00:00:00','dd-MM-yyyy HH24:MI:SS') \n"
                + "           AND sale_trans_date_detail <  to_date('" + endCycle + " 23:59:59','dd-MM-yyyy HH24:MI:SS') \n"
                + "           AND (   shop_id_lv2 = 7282\n"
                + "                OR shop_id_lv3 = 7282\n"
                + "                OR shop_id_lv4 = 7282\n"
                + "                OR shop_id_lv5 = 7282)\n"
                + "           AND (channel_type_id = -1\n"
                + "                OR channel_type_id IN\n"
                + "                          (SELECT   channel_type_id\n"
                + "                             FROM   channel_type\n"
                + "                            WHERE   is_vt_unit in (1,2) AND object_type = 2))\n"
                + " GROUP BY   "
                + "           to_char(last_update, 'MM-yyyy'), \n"
                + "shop_id_lv3,\n"
                + "           shop_code_lv3,\n"
                + "           shop_name_lv3,\n"
                + "           shop_id_lv4,\n"
                + "           shop_code_lv4,\n"
                + "           shop_name_lv4,\n"
                + "           shop_id_lv5,\n"
                + "           shop_code_lv5,\n"
                + "           shop_name_lv5,\n"
                + "           sale_trans_type,\n"
                + "           sale_trans_type_name,\n"
                + "           stock_type_id,\n"
                + "           stock_type_name,\n"
                + "           stock_model_id,\n"
                + "           stock_model_code,\n"
                + "           stock_model_name,\n"
                + "           accounting_model_code,\n"
                + "           accounting_model_name,\n"
                + "           price,\n"
                + "           price_vat,"
                + "           type_adjusted,\n"
                + "			  rnv_service_id,\n"
                + "           telecom_service_id\n"
                + " ORDER BY   shop_code_lv3,\n"
                + "           shop_code_lv4,\n"
                + "           shop_code_lv5,\n"
                + "           sale_trans_type,\n"
                + "           stock_type_id,\n"
                + "           stock_model_code \n"
                + "           ";

        PreparedStatement ps = null;
        try {
            connection = ConnectionPoolManager.getConnection("dbsm");
            ps = connection.prepareStatement(mainSql);
            if (QUERY_TIMEOUT > 0) {
                ps.setQueryTimeout(QUERY_TIMEOUT);
            }
            rs1 = ps.executeQuery();
            while (rs1.next()) {
                Revenue rev = new Revenue();
                rev.setAmount(retNull(rs1.getString("amount")));
                rev.setAmountAfterTax(retNull(rs1.getString("amount_after_tax")));
                rev.setAmountBeforeTax(retNull(rs1.getString("amount_before_tax")));
                rev.setAmountTax(retNull(rs1.getString("amount_tax")));
                rev.setCycle(starOfCycle);
                rev.setDiscount(retNull(rs1.getString("discount_amount")));
                rev.setPrice(retNull(rs1.getString("price")));
                rev.setQuantity(retNull(rs1.getString("quantity")));
                rev.setRevenueType(retNull(revType));
                rev.setSaleTransTypeName(retNull(rs1.getString("sale_trans_type_name")));
                rev.setStockTypeName(retNull(rs1.getString("stock_type_name")));
                rev.setStockModelCode(retNull(rs1.getString("stock_model_code")));
                rev.setStockModelName(retNull(rs1.getString("stock_model_name")));
                rev.setAdjustType(retNull(rs1.getString("type_adjusted")));
                rev.setMoneyRate(MONEY_RATE);
                rev.setCurrency(retNull(CURRENTCY));
                rev.setVat(retNull(VAT));
                rev.setRvnServiceId(retNull(rs1.getString("rnv_service_id")));
                rev.setRvnServiceName(retNull(rs1.getString("rnv_service_name")));
                rev.setUnit(retNull(rs1.getString("unit")));
                rev.setTelecomService(retNull(rs1.getString("tel_service_id")));
                rev.setUnitCodeLever1(retNull(rs1.getString("shop_code_lv3")));
                rev.setUnitCodeLever2(retNull(rs1.getString("shop_code_lv4")));
                rev.setUnitCodeLever3(retNull(rs1.getString("shop_code_lv5")));
//                rev.setSyncTime(new SimpleDateFormat("dd-MM-yyyy HH:mm:ss").format(new Date()));
                rev.setSyncTime(retNull(rs1.getString("adjustTime")));
                lstRevenue.add(rev);
            }
            return lstRevenue;
        } catch (Exception ex) {
            logger.error("ERROR getRevenue sale adjusted");
            logger.error(AppManager.logException(timeSt, ex));
            return null;
        } finally {
            closeResultSet(rs1);
            closeStatement(ps);
            closeConnection(connection);
        }
    }

    public List<Revenue> getBccsPaymentRevenueCloseCycle(String starOfCycle, String endCycle, int revType) {
        long timeSt = System.currentTimeMillis();
        List<Revenue> lstRevenue = new ArrayList<Revenue>();
        Connection connection = null;
        ResultSet rs1 = null;
        String mainSql = ""
                + "SELECT  sum(totalinvoice) totalinvoice, sum(amountaftertax) amount_after_tax,sum(amountbeforetax )amount_before_tax,sum(amounttax) amount_tax,servicename,servicetypes,sale_trans_type_name,\n"
                + "collectiongroupid,tax,shop_id_lv2,shop_id_lv3,shop_code_lv3,shop_name_lv3,shop_id_lv4,shop_code_lv4,shop_name_lv4,shop_id_lv5,shop_code_lv5,shop_name_lv5,'Unit' as unit\n"
                + "FROM (\n"
                + "SELECT   rs.totalinvoice totalinvoice,\n"
                + "                     rs.amountaftertax amountaftertax,\n"
                + "                     rs.amountbeforetax amountbeforetax,\n"
                + "                     rs.amounttax amounttax,\n"
                + "                     rs.servicename,\n"
                + "                     decode(rs.servicetypes,'L','89','M','53','W','94','F','84') as servicetypes ,\n"
                + "                      sale_trans_type_name,\n"
                + "                     cg.collection_group_id collectiongroupid,\n"
                + "                     rs.tax tax,\n"
                + "                     shop_id_lv2,\n"
                + "                     shop_id_lv3,\n"
                + "                     shop_code_lv3,\n"
                + "                     shop_name_lv3,\n"
                + "                     shop_id_lv4,\n"
                + "                     shop_code_lv4,\n"
                + "                     shop_name_lv4,\n"
                + "                     shop_id_lv5,\n"
                + "                     shop_code_lv5,\n"
                + "                     shop_name_lv5\n"
                + "                     \n"
                + "              FROM   (  SELECT   SUM (amountaftertax) amountaftertax,\n"
                + "                                 SUM (amountbeforetax) amountbeforetax,\n"
                + "                                 SUM (amounttax) amounttax,\n"
                + "                                 tax,\n"
                + "                                 COUNT (1) totalinvoice,\n"
                + "                                 collection_staff_id,\n"
                + "                                 shop_id,\n"
                + "                                 servicename,\n"
                + "                                 servicetypes,\n"
                + "                                 sale_trans_type_name,\n"
                + "                                shop_id_lv2,\n"
                + "                                shop_id_lv3,\n"
                + "                                shop_code_lv3,\n"
                + "                                shop_name_lv3,\n"
                + "                                shop_id_lv4,\n"
                + "                                shop_code_lv4,\n"
                + "                                shop_name_lv4,\n"
                + "                                shop_id_lv5,\n"
                + "                                shop_code_lv5,\n"
                + "                                shop_name_lv5\n"
                + "                          FROM   (SELECT   pi.amount AS amountaftertax,\n"
                + "                                           ROUND ( pi.amount_tax * 100  / (100 + pi.tax), 2) + pi.amount_not_tax AS amountbeforetax,\n"
                + "                                           (pi.amount - pi.amount_not_tax - ROUND ( pi.amount_tax * 100 / (100 + pi.tax),  2)) AS amounttax,\n"
                + "                                           pi.tax tax,\n"
                + "                                           pi.payment_id totalinvoice,\n"
                + "                                           pc.staff_id AS collection_staff_id,\n"
                + "                                           pc.shop_id,\n"
                + "                                           (SELECT   service_name  FROM   payment.service_type@DBL_PAYMENT st WHERE   co.service_types = st.service_types) AS servicename,\n"
                + "                                           (SELECT   service_types  FROM   payment.service_type@DBL_PAYMENT st WHERE   co.service_types = st.service_types) AS servicetypes,\n"
                + "                                           pt.name as sale_trans_type_name,\n"
                + "                                             tmp.shop_id_lv2,\n"
                + "                                             tmp.shop_id_lv3,\n"
                + "                                             tmp.shop_code_lv3,\n"
                + "                                             tmp.shop_name_lv3,\n"
                + "                                             tmp.shop_id_lv4,\n"
                + "                                             tmp.shop_code_lv4,\n"
                + "                                             tmp.shop_name_lv4,\n"
                + "                                             tmp.shop_id_lv5,\n"
                + "                                             tmp.shop_code_lv5,\n"
                + "                                             tmp.shop_name_lv5\n"
                + "                                    FROM           payment.payment_invoice@DBL_PAYMENT pi\n"
                + "                                               JOIN\n"
                + "                                                   sm.SYN_BCCS_EPR_DATA pc\n"
                + "                                               ON (pc.sta_cycle = to_date('" + starOfCycle + " 00:00:00','dd-MM-yyyy HH24:MI:SS') and pi.payment_id = pc.trans_id\n"
                + "                                                   AND pi.create_date = pc.trans_time AND pi.contract_id =  pc.contract_id)\n"
                + "                                           JOIN\n"
                + "                                               payment.contract@DBL_PAYMENT co\n"
                + "                                           ON (pc.contract_id = co.contract_id)\n"
                + "                                           left join \n"
                + "                                                payment.payment_type@DBL_PAYMENT pt \n"
                + "                                           ON (pc.trans_type = pt.code)\n"
                + "                                           left join \n"
                + "                                                sm.stock_owner_tmp tmp\n"
                + "                                           on (pc.shop_id = tmp.owner_id)\n"
                + "                                   WHERE       pi.status = '1'\n"
                + "\n"
                + "                                           AND pi.amount_tax > 0\n"
                + "                                           AND pc.status = '1'\n"
                //				+ "                                           AND pc.trans_type <> 14\n"
                //				+ "                                           AND pc.trans_type <> 10\n"
                + "                                           AND pi.create_date >=to_date('" + starOfCycle + " 00:00:00','dd-MM-yyyy HH24:MI:SS') and pi.create_date < to_date('" + endCycle + " 23:59:59','dd-MM-yyyy HH24:MI:SS')\n"
                + "                                           AND pc.revenue_type =" + revType + "\n"
                //				+ "                                           AND pi.serial_no != 'ATM'\n"
                //				+ "                                           AND pc.trans_type <> '06'"
                + ")\n"
                + "                      GROUP BY   collection_staff_id,\n"
                + "                                 servicename,\n"
                + "                                 servicetypes,\n"
                + "                                 sale_trans_type_name,\n"
                + "                                 tax,\n"
                + "                                 shop_id,\n"
                + "                                 shop_id_lv2,\n"
                + "                                shop_id_lv3,\n"
                + "                                shop_code_lv3,\n"
                + "                                shop_name_lv3,\n"
                + "                                shop_id_lv4,\n"
                + "                                shop_code_lv4,\n"
                + "                                shop_name_lv4,\n"
                + "                                shop_id_lv5,\n"
                + "                                shop_code_lv5,\n"
                + "                                shop_name_lv5\n"
                + "                                 ) rs,\n"
                + "                     payment.collection_group@DBL_PAYMENT cg\n"
                + "             WHERE   cg.collection_group_id(+) = rs.shop_id\n"
                + "             ) group by collectiongroupid,tax,servicename,servicetypes,sale_trans_type_name,shop_id_lv2,shop_id_lv3,shop_code_lv3,shop_name_lv3,shop_id_lv4,shop_code_lv4,shop_name_lv4,shop_id_lv5,shop_code_lv5,shop_name_lv5\n"
                + "             order by shop_code_lv3,shop_code_lv4,shop_code_lv5";

        PreparedStatement ps = null;
        try {
            connection = ConnectionPoolManager.getConnection("dbsm");
            ps = connection.prepareStatement(mainSql);
            if (QUERY_TIMEOUT > 0) {
                ps.setQueryTimeout(QUERY_TIMEOUT);
            }
            rs1 = ps.executeQuery();
            while (rs1.next()) {
                Revenue rev = new Revenue();
                rev.setAmount(retNull(rs1.getString("amount_after_tax")));
                rev.setAmountAfterTax(retNull(rs1.getString("amount_after_tax")));
                rev.setAmountBeforeTax(retNull(rs1.getString("amount_before_tax")));
                rev.setAmountTax(retNull(rs1.getString("amount_tax")));
                rev.setCycle(starOfCycle);
                rev.setDiscount("");
                rev.setPrice("");
                rev.setQuantity("");
                rev.setRevenueType(retNull(revType));
                rev.setSaleTransTypeName(retNull(rs1.getString("sale_trans_type_name")));
                rev.setStockTypeName("Payment service");
                rev.setStockModelCode("");
                rev.setStockModelName("");
                rev.setMoneyRate(MONEY_RATE);
                rev.setCurrency(retNull(CURRENTCY));
                rev.setVat(retNull(VAT));
                rev.setRvnServiceId("");
                rev.setRvnServiceName("");
                rev.setTelecomService(retNull(rs1.getString("servicetypes")));
                rev.setUnit(retNull(rs1.getString("unit")));
                rev.setUnitCodeLever1(retNull(rs1.getString("shop_code_lv3")));
                rev.setUnitCodeLever2(retNull(rs1.getString("shop_code_lv4")));
                rev.setUnitCodeLever3(retNull(rs1.getString("shop_code_lv5")));
                lstRevenue.add(rev);
            }
            return lstRevenue;
        } catch (Exception ex) {
            logger.error("ERROR getRevenue sale");
            logger.error(AppManager.logException(timeSt, ex));
            return null;
        } finally {
            closeResultSet(rs1);
            closeStatement(ps);
            closeConnection(connection);
        }
    }

    public List<Revenue> getBccsPaymentRevenueAdjusted(String starOfCycle, String endCycle, int revType) {
        long timeSt = System.currentTimeMillis();
        ArrayList<Revenue> lstRevenue = new ArrayList<Revenue>();
        Connection connection = null;
        ResultSet rs1 = null;
        String sql = ""
                + " SELECT  adjustTime, sum(totalinvoice) totalinvoice, "
				+ "decode(type_adjusted,1,sum(amountaftertax),-sum(amountaftertax)) amount_after_tax,"
				+ " decode(type_adjusted,1,sum(amountbeforetax ),-sum(amountbeforetax )) amount_before_tax"
				+ ",decode(type_adjusted,1,sum(amounttax ),-sum(amounttax )) amount_tax"
				+ ",servicename,servicetypes,sale_trans_type_name,\n"
                + " tax,shop_id_lv2,shop_id_lv3,shop_code_lv3,shop_name_lv3,shop_id_lv4,shop_code_lv4,shop_name_lv4,shop_id_lv5,shop_code_lv5,shop_name_lv5,type_adjusted,'Unit' as unit \n"
                + " FROM (\n"
                + " SELECT   rs.totalinvoice totalinvoice,\n"
                + "                     rs.amountaftertax amountaftertax,\n"
                + "                     rs.amountbeforetax amountbeforetax,\n"
                + "                     rs.amounttax amounttax,\n"
                + "                     rs.servicename,\n"
                + "                     decode(rs.servicetypes,'L','5','M','1','W','12','F','15') as servicetypes ,\n"
                + "                     sale_trans_type_name,\n"
                + "                     rs.tax tax,\n"
                + "                     shop_id_lv2,\n"
                + "                     shop_id_lv3,\n"
                + "                     shop_code_lv3,\n"
                + "                     shop_name_lv3,\n"
                + "                     shop_id_lv4,\n"
                + "                     shop_code_lv4,\n"
                + "                     shop_name_lv4,\n"
                + "                     shop_id_lv5,\n"
                + "                     shop_code_lv5,\n"
                + "                     shop_name_lv5,\n"
                + "                     type_adjusted,\n"
                + "			adjustTime\n"
                + "                     \n"
                + "              FROM   (  SELECT   SUM (amountaftertax) amountaftertax,\n"
                + "                                 SUM (amountbeforetax) amountbeforetax,\n"
                + "                                 SUM (amounttax) amounttax,\n"
                + "                                 tax,\n"
                + "                                 COUNT (1) totalinvoice,\n"
                + "                                 collection_staff_id,\n"
                + "                                 collection_group_id ,\n"
                + "                                 servicename,\n"
                + "                                 servicetypes,\n"
                + "                                 sale_trans_type_name,\n"
                + "                                shop_id_lv2,\n"
                + "                                shop_id_lv3,\n"
                + "                                shop_code_lv3,\n"
                + "                                shop_name_lv3,\n"
                + "                                shop_id_lv4,\n"
                + "                                shop_code_lv4,\n"
                + "                                shop_name_lv4,\n"
                + "                                shop_id_lv5,\n"
                + "                                shop_code_lv5,\n"
                + "                                shop_name_lv5,\n"
                + "                                type_adjusted,\n"
                + "			adjustTime\n"
                + "                                \n"
                + "                          FROM   (SELECT   pi.amount AS amountaftertax,\n"
                + "                                           ROUND ( pi.amount_tax * 100  / (100 + pi.tax), 2) + pi.amount_not_tax AS amountbeforetax,\n"
                + "                                           (pi.amount - pi.amount_not_tax - ROUND ( pi.amount_tax * 100 / (100 + pi.tax),  2)) AS amounttax,\n"
                + "                                           pi.tax tax,\n"
                + "                                           pi.payment_id totalinvoice,\n"
                + "                                           pc.collection_staff_id,\n"
                + "                                           pc.collection_group_id,\n"
                + "                                           (SELECT   service_name  FROM   payment.service_type@DBL_PAYMENT st WHERE   co.service_types = st.service_types) AS servicename,\n"
                + "                                           (SELECT   service_types  FROM   payment.service_type@DBL_PAYMENT st WHERE   co.service_types = st.service_types) AS servicetypes,\n"
                + "                                           pt.name as sale_trans_type_name,\n"
                + "                                             tmp.shop_id_lv2,\n"
                + "                                             tmp.shop_id_lv3,\n"
                + "                                             tmp.shop_code_lv3,\n"
                + "                                             tmp.shop_name_lv3,\n"
                + "                                             tmp.shop_id_lv4,\n"
                + "                                             tmp.shop_code_lv4,\n"
                + "                                             tmp.shop_name_lv4,\n"
                + "                                             tmp.shop_id_lv5,\n"
                + "                                             tmp.shop_code_lv5,\n"
                + "                                             tmp.shop_name_lv5,\n"
                + "                                             adj.type_adjusted,\n"
                + "			to_char(pc.last_update, 'MM-yyyy') adjustTime\n"
                + "                                    FROM     payment.payment_invoice@DBL_PAYMENT pi\n"
                + "                                          JOIN payment.payment_contract@DBL_PAYMENT pc ON (pc.create_date >=to_date('" + starOfCycle + " 00:00:00','dd-MM-yyyy HH24:MI:SS') and pc.create_date < to_date('" + endCycle + " 23:59:59','dd-MM-yyyy HH24:MI:SS') and pi.payment_id = pc.payment_id AND pi.create_date = pc.create_date AND pi.contract_id =  pc.contract_id )\n"
                + "                                          JOIN payment.contract@DBL_PAYMENT co  ON (pc.contract_id = co.contract_id)\n"
                + "                                          left join  payment.payment_type@DBL_PAYMENT pt  ON (pc.payment_type = pt.code)\n"
                + "                                          left join  sm.stock_owner_tmp tmp on (pc.collection_group_id = tmp.owner_id)\n"
                + "                                          JOIN  (select b.payment_id,'0' as type_adjusted from sm.syn_bccs_epr_data a,  payment.payment_contract@DBL_PAYMENT b where a.trans_id = b.payment_id  \n"
                + "                                                        and b.create_date >=to_date('" + starOfCycle + " 00:00:00','dd-MM-yyyy HH24:MI:SS') and b.create_date < to_date('" + endCycle + " 23:59:59','dd-MM-yyyy HH24:MI:SS') and a.status <> 0 and  b.status=0\n"
                + "                                                        union all\n"
                + "                                                        select b.payment_id,'1' as type_adjusted from sm.syn_bccs_epr_data a,  payment.payment_contract@DBL_PAYMENT b where a.trans_id(+) = b.payment_id  and b.create_date >=to_date('" + starOfCycle + " 00:00:00','dd-MM-yyyy HH24:MI:SS') and b.create_date < to_date('" + endCycle + " 23:59:59','dd-MM-yyyy HH24:MI:SS')\n"
                + "                                                        and b.status <> 0 and a.trans_id is null) adj \n"
                + "                                          ON (pi.payment_id = adj.payment_id)\n"
                + "                                   WHERE      \n"
                + "                                           pi.amount_tax > 0\n"
                + "                                           AND pi.create_date >=to_date('" + starOfCycle + " 00:00:00','dd-MM-yyyy HH24:MI:SS') and pi.create_date < to_date('" + endCycle + " 23:59:59','dd-MM-yyyy HH24:MI:SS') )\n"
                + "                      GROUP BY   "
                + "			adjustTime,\n"
                + "collection_staff_id,\n"
                + "                                 servicename,\n"
                + "                                 servicetypes,\n"
                + "                                 sale_trans_type_name,\n"
                + "                                 tax,\n"
                + "                                 collection_group_id,\n"
                + "                                 shop_id_lv2,\n"
                + "                                shop_id_lv3,\n"
                + "                                shop_code_lv3,\n"
                + "                                shop_name_lv3,\n"
                + "                                shop_id_lv4,\n"
                + "                                shop_code_lv4,\n"
                + "                                shop_name_lv4,\n"
                + "                                shop_id_lv5,\n"
                + "                                shop_code_lv5,\n"
                + "                                shop_name_lv5,\n"
                + "                                type_adjusted\n"
                + "                                 ) rs\n"
                + "             ) group by adjustTime, tax,servicename,servicetypes,sale_trans_type_name,shop_id_lv2,shop_id_lv3,shop_code_lv3,shop_name_lv3,shop_id_lv4,shop_code_lv4,shop_name_lv4,shop_id_lv5,shop_code_lv5,shop_name_lv5,type_adjusted\n"
                + "             order by shop_code_lv3,shop_code_lv4,shop_code_lv5"
                + ""
                + "";

        PreparedStatement ps = null;
        try {
            connection = ConnectionPoolManager.getConnection("dbsm");
            ps = connection.prepareStatement(sql);
            if (QUERY_TIMEOUT > 0) {
                ps.setQueryTimeout(QUERY_TIMEOUT);
            }
            rs1 = ps.executeQuery();
            while (rs1.next()) {
                Revenue rev = new Revenue();
                rev.setAmount(retNull(rs1.getString("amount_after_tax")));
                rev.setAmountAfterTax(retNull(rs1.getString("amount_after_tax")));
                rev.setAmountBeforeTax(retNull(rs1.getString("amount_before_tax")));
                rev.setAmountTax(retNull(rs1.getString("amount_tax")));
                rev.setCycle(starOfCycle);
                rev.setDiscount("");
                rev.setPrice("");
                rev.setQuantity("");
                rev.setRevenueType(retNull(revType));
                rev.setAdjustType(retNull(rs1.getString("type_adjusted")));
                rev.setSaleTransTypeName(retNull(rs1.getString("sale_trans_type_name")));
                rev.setStockTypeName("Payment service");
                rev.setStockModelCode("");
                rev.setStockModelName("");
                rev.setMoneyRate(MONEY_RATE);
                rev.setCurrency(retNull(CURRENTCY));
                rev.setVat(retNull(VAT));
                rev.setRvnServiceId("");
                rev.setRvnServiceName("");
                rev.setTelecomService(retNull(rs1.getString("servicetypes")));
                rev.setUnit(retNull(rs1.getString("unit")));
                rev.setUnitCodeLever1(retNull(rs1.getString("shop_code_lv3")));
                rev.setUnitCodeLever2(retNull(rs1.getString("shop_code_lv4")));
                rev.setUnitCodeLever3(retNull(rs1.getString("shop_code_lv5")));
                rev.setSyncTime(retNull(rs1.getString("adjustTime")));
                lstRevenue.add(rev);
            }
            return lstRevenue;
        } catch (Exception ex) {
            logger.error("ERROR getRevenue sale adjusted");
            logger.error(AppManager.logException(timeSt, ex));
            return null;
        } finally {
            closeResultSet(rs1);
            closeStatement(ps);
            closeConnection(connection);
        }
    }
	
	
	public List<Revenue> getBccsRevenueAdjusted(String starOfCycle, int revType) {
        long timeSt = System.currentTimeMillis();
        List<Revenue> lstRevenue = new ArrayList<Revenue>();
        Connection connection = null;
        ResultSet rs1 = null;
        String mainSql = "select "
				+ "revenue_type,cycle_date,shop_id_lv3,shop_code_lv3,shop_name_lv3,shop_id_lv4,shop_code_lv4,shop_name_lv4\n" +
					"shop_id_lv5,shop_code_lv5,shop_name_lv5,trans_type,sale_trans_type_name,stock_type_id,stock_type_name,stock_model_id,\n" +
					"stock_model_code,stock_model_name,accounting_model_code,accounting_model_name,price,price_vat,\n" +
					"quantity,amount,discount_amount,amount_before_tax,amount_tax,amount_after_tax,currency,rnv_service_id,rnv_service_name,\n" +
					"rate,vat,decode(telecom_service_id,1,53,5,89,12,94,15,84,telecom_service_id ) telecom_service_id ,type_adjusted,adjusttime,current_cycle,process_date"
				+ " from sm.sync_rvn_erp_adjusted where current_cycle=to_date(?,'dd-MM-yyyy') and revenue_type=?";
        PreparedStatement ps = null;
        try {
            connection = ConnectionPoolManager.getConnection("dbsm");
            ps = connection.prepareStatement(mainSql);
			ps.setString(1, starOfCycle);
			ps.setInt(2, revType);
            if (QUERY_TIMEOUT > 0) {
                ps.setQueryTimeout(QUERY_TIMEOUT);
            }
            rs1 = ps.executeQuery();
            while (rs1.next()) {
                Revenue rev = new Revenue();
                rev.setAmount(retNull(rs1.getString("amount")));
                rev.setAmountAfterTax(retNull(rs1.getString("amount_after_tax")));
                rev.setAmountBeforeTax(retNull(rs1.getString("amount_before_tax")));
                rev.setAmountTax(retNull(rs1.getString("amount_tax")));
                rev.setCycle(starOfCycle);
                rev.setDiscount(retNull(rs1.getString("discount_amount")));
                rev.setPrice(retNull(rs1.getString("price")));
                rev.setQuantity(retNull(rs1.getString("quantity")));
                rev.setRevenueType(retNull(revType));
                rev.setSaleTransTypeName(retNull(rs1.getString("sale_trans_type_name")));
                rev.setStockTypeName(retNull(rs1.getString("stock_type_name")));
                rev.setStockModelCode(retNull(rs1.getString("stock_model_code")));
                rev.setStockModelName(retNull(rs1.getString("stock_model_name")));
                rev.setAdjustType(retNull(rs1.getString("type_adjusted")));
                rev.setMoneyRate(MONEY_RATE);
                rev.setCurrency(retNull(CURRENTCY));
                rev.setVat(retNull(VAT));
                rev.setRvnServiceId(retNull(rs1.getString("rnv_service_id")));
                rev.setRvnServiceName(retNull(rs1.getString("rnv_service_name")));
                rev.setUnit("Unit");
                rev.setTelecomService(retNull(rs1.getString("telecom_service_id")));
                rev.setUnitCodeLever1(retNull(rs1.getString("shop_code_lv3")));
                rev.setUnitCodeLever2(retNull(rs1.getString("shop_code_lv4")));
                rev.setUnitCodeLever3(retNull(rs1.getString("shop_code_lv5")));
//                rev.setSyncTime(new SimpleDateFormat("dd-MM-yyyy HH:mm:ss").format(new Date()));
                rev.setSyncTime(retNull(rs1.getString("adjustTime")));
                lstRevenue.add(rev);
            }
            return lstRevenue;
        } catch (Exception ex) {
            logger.error("ERROR getRevenue sale adjusted");
            logger.error(AppManager.logException(timeSt, ex));
            return null;
        } finally {
            closeResultSet(rs1);
            closeStatement(ps);
            closeConnection(connection);
        }
    }
	

    public List<Shop> getBccsShop() {
        long timeSt = System.currentTimeMillis();
        List<Shop> listShop = new ArrayList<Shop>();
        Connection connection = null;
        ResultSet rs1 = null;
        String sql = "select shop_id,name,parent_shop_id,address,upper(shop_code) shop_code ,province,to_char(create_date,'dd-MM-yyyy HH24:MI:SS') create_date,shop_path from sm.shop where  status =1 and (SUBSTR (shop_code, 4, 2) != 'SH' or length(shop_code) < 4 ) ";

        PreparedStatement ps = null;
        try {
            connection = ConnectionPoolManager.getConnection("dbsm");
            ps = connection.prepareStatement(sql);
            if (QUERY_TIMEOUT > 0) {
                ps.setQueryTimeout(QUERY_TIMEOUT);
            }
            rs1 = ps.executeQuery();
            while (rs1.next()) {
                Shop sh = new Shop();
                sh.setShopId(retNull(rs1.getString("shop_id")));
                sh.setAddress(retNull(rs1.getString("address")));
                sh.setCreateDate(retNull(rs1.getString("create_date")));
                sh.setName(retNull(rs1.getString("name")));
                sh.setParentShopId(retNull(rs1.getString("parent_shop_id")));
                sh.setShopPath(retNull(rs1.getString("shop_path")));
                sh.setShopCode(retNull(rs1.getString("shop_code")));
                sh.setProvince(retNull(rs1.getString("province")));
                listShop.add(sh);
            }
            return listShop;
        } catch (Exception ex) {
            logger.error("ERROR getBccsShop");
            logger.error(AppManager.logException(timeSt, ex));
            return null;
        } finally {
            closeResultSet(rs1);
            closeStatement(ps);
            closeConnection(connection);
        }
    }

    public List<StockModel> getBccsStockModel() {
        long timeSt = System.currentTimeMillis();
        List<StockModel> listStockModel = new ArrayList<StockModel>();
        Connection connection = null;
        ResultSet rs1 = null;
        String sql = "select stock_model_id,stock_model_code,name,stock_type_id,'Unit' as unit,telecom_service_id from sm.stock_model where  status =1 ";

        PreparedStatement ps = null;
        try {
            connection = ConnectionPoolManager.getConnection("dbsm");
            ps = connection.prepareStatement(sql);
            if (QUERY_TIMEOUT > 0) {
                ps.setQueryTimeout(QUERY_TIMEOUT);
            }
            rs1 = ps.executeQuery();
            while (rs1.next()) {
                StockModel sm = new StockModel();
                sm.setStockModelCode(retNull(rs1.getString("stock_model_code")));
                sm.setStockModelId(retNull(rs1.getString("stock_model_id")));
                sm.setStockTypeId(retNull(rs1.getString("stock_type_id")));
                sm.setName(retNull(rs1.getString("name")));
                sm.setUnit(retNull(rs1.getString("unit")));
                sm.setTelecomServiceId(retNull(rs1.getString("telecom_service_id")));
                listStockModel.add(sm);
            }
            return listStockModel;
        } catch (Exception ex) {
            logger.error("ERROR getBccsStockModel");
            logger.error(AppManager.logException(timeSt, ex));
            return null;
        } finally {
            closeResultSet(rs1);
            closeStatement(ps);
            closeConnection(connection);
        }

    }

    public List<StockType> getBccsStockType() {
        long timeSt = System.currentTimeMillis();
        List<StockType> listStockModel = new ArrayList<StockType>();
        Connection connection = null;
        ResultSet rs1 = null;
        String sql = "select * from stock_type where status =1 ";

        PreparedStatement ps = null;
        try {
            connection = ConnectionPoolManager.getConnection("dbsm");
            ps = connection.prepareStatement(sql);
            if (QUERY_TIMEOUT > 0) {
                ps.setQueryTimeout(QUERY_TIMEOUT);
            }
            rs1 = ps.executeQuery();
            while (rs1.next()) {
                StockType st = new StockType();
                st.setStockTypeName(retNull(rs1.getString("name")));
                st.setStockTypeId(retNull(rs1.getString("stock_type_id")));
                st.setStatus(rs1.getInt("status"));
                st.setNote(retNull(rs1.getString("notes")));
                listStockModel.add(st);
            }
            return listStockModel;
        } catch (Exception ex) {
            logger.error("ERROR getBccsStockModel");
            logger.error(AppManager.logException(timeSt, ex));
            return null;
        } finally {
            closeResultSet(rs1);
            closeStatement(ps);
            closeConnection(connection);
        }

    }

    public List<RvnService> getBccsRvnService() {
        long timeSt = System.currentTimeMillis();
        List<RvnService> listRvnService = new ArrayList<RvnService>();
        Connection connection = null;
        ResultSet rs1 = null;
        String sql = "select * from vtict_report.rvn_service where status =1 ";

        PreparedStatement ps = null;
        try {
            connection = ConnectionPoolManager.getConnection("dbsm");
            ps = connection.prepareStatement(sql);
            if (QUERY_TIMEOUT > 0) {
                ps.setQueryTimeout(QUERY_TIMEOUT);
            }
            rs1 = ps.executeQuery();
            while (rs1.next()) {
                RvnService rvns = new RvnService();
                rvns.setRvnId(retNull(rs1.getInt("ID")));
                rvns.setRvnCode(retNull(rs1.getString("CODE")));
                rvns.setRvnName(retNull(rs1.getString("NAME")));
                rvns.setStatus(rs1.getInt("STATUS"));
                rvns.setUnitId(rs1.getInt("UNIT_ID"));
                rvns.setSource(retNull(rs1.getString("SOURCE")));
                rvns.setCurencyCode(retNull(rs1.getString("CURRENCY_CODE")));
                listRvnService.add(rvns);
            }
            return listRvnService;
        } catch (Exception ex) {
            logger.error("ERROR getBccsStockModel");
            logger.error(AppManager.logException(timeSt, ex));
            return null;
        } finally {
            closeResultSet(rs1);
            closeStatement(ps);
            closeConnection(connection);
        }

    }

    public static String retNull(Object str) {
        if (str == null) {
            return "";
        } else {
            return str.toString().trim();
        }
    }
}
