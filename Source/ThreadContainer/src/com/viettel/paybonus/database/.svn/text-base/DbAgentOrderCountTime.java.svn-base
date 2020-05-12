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
import com.viettel.vas.util.PoolStore;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;
import org.apache.log4j.Logger;
import java.util.Date;

/**
 *
 * Thong tin phien ban
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class DbAgentOrderCountTime extends DbProcessorAbstract {

//    private Logger logger;
    private String loggerLabel = DbAgentOrderCountTime.class.getSimpleName() + ": ";
//    private StringBuffer br = new StringBuffer();
    private PoolStore poolStore;
    private String dbNameCofig;

    public DbAgentOrderCountTime() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
        dbNameCofig = ResourceBundle.getBundle("configPayBonus").getString("dbNameConfig");
        poolStore = new PoolStore(dbNameCofig, logger);
    }

    public DbAgentOrderCountTime(String sessionName, Logger logger) throws SQLException, Exception {
        this.logger = logger;
        dbNameCofig = sessionName;
        poolStore = new PoolStore(dbNameCofig, logger);
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
        SaleTransOrder record = new SaleTransOrder();
        long timeSt = System.currentTimeMillis();
        try {
            record.setReceiverId(rs.getLong("receiver_id"));
            record.setSaleTransDate(rs.getString("sale_trans_date"));
            record.setSysdate(rs.getString("sys_date"));
            record.setShopId(rs.getLong("shop_id"));
            record.setStaffId(rs.getLong("staff_id"));
            record.setSaleTransOrderId(rs.getLong("sale_trans_order_id"));
            record.setResultCode("0");
            record.setDescription("Processing");
        } catch (Exception ex) {
            logger.error("ERROR parse MoRecord");
            logger.error(AppManager.logException(timeSt, ex));
        }
        return record;
    }

    @Override
    public int[] deleteQueue(List<Record> listRecords) {
        int[] res = new int[0];
        return res;
    }

    public String getListIsdnReceiveWarningOrder(String shopId) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        String channelCode = "";
        StringBuilder br = new StringBuilder();
        String sql = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "select * from agent_receive_sms where shop_id = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, shopId);
            rs = ps.executeQuery();
            while (rs.next()) {
                channelCode = rs.getString("list_isdn");
            }
            logger.info("End getListIsdnReceiveWarningOrder shopId " + shopId + "result: " + channelCode + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR getListIsdnReceiveWarningOrder: ").
                    append(sql).append("\n")
                    .append(" shopId ")
                    .append(shopId);
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return channelCode;
        }
    }

    public int getKPIHours(Long receiverId) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        int kpiHours = 0;
        String sqlMo = "select kpi_hours from sm.agent_order_info where staff_id = ? and rownum < 2";
        PreparedStatement psMo = null;
        try {
            connection = getConnection("dbsm");
            psMo = connection.prepareStatement(sqlMo);
            psMo.setLong(1, receiverId);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                kpiHours = rs1.getInt("kpi_hours");
                break;
            }
            logTimeDb("Time to getKPIHours: receiverId: " + receiverId + ", kpiHours: " + kpiHours, timeSt);
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error("ERROR getKPIHours,receiverId " + receiverId);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return kpiHours;
    }

    public List<AgentReportInfo> getListReportInfo() {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        List<AgentReportInfo> lstReport = new ArrayList<AgentReportInfo>();
        StringBuilder br = new StringBuilder();
        String sql = "";
        try {
            connection = getConnection("dbsm");
            sql = "select a.province_refefence, nvl(a1.approve_not_confirm,0) as agent_not_confirm, nvl(a2.export_not_confirm,0) as export_not_confirm,\n"
                    + "                    nvl(reject_in_day,0) as reject_in_day, nvl(reject_in_month,0) as reject_in_month, nvl(less_than_3h,0) as less_than_3h,\n"
                    + "                    nvl(between_3h_5h,0) as between_3h_5h, nvl(between_5h_8h,0) as between_5h_8h, nvl(over_8h,0) as over_8h from sm.area a\n"
                    + "                    left join\n"
                    + "                    (select province, nvl(count(1),0) as approve_not_confirm from (select (case when b.staff_code like '%CAB%' then 'CAB' when b.staff_code like '%GAZ%' then 'GAZ'\n"
                    + "                            when b.staff_code like '%INH%' then 'INH' when b.staff_code like '%MAC%' then 'MAC'\n"
                    + "                            when b.staff_code like '%MAN%' then 'MAN' when b.staff_code like '%MAT%' then 'MAT'\n"
                    + "                            when b.staff_code like '%MOC%' then 'MOC' when b.staff_code like '%NAC%' then 'NAC'\n"
                    + "                            when b.staff_code like '%NAM%' then 'NAM' when b.staff_code like '%NIA%' then 'NIA'\n"
                    + "                            when b.staff_code like '%SOF%' then 'SOF' when b.staff_code like '%TET%' then 'TET'\n"
                    + "                            when b.staff_code like '%ZAM%' then 'ZAM' else 'UNKNOWN' end) province from sm.sale_trans_order a,\n"
                    + "                            sm.staff b where a.receiver_id = b.staff_id and sale_trans_date > '01-jul-2019' and\n"
                    + "                    a.order_from = 'mBCCS' and a.status <> 4 and a.over_quantity = 0 \n"
                    + "                    and not exists (select 1 from sm.agent_order_pincode where status = 0 and a.sale_trans_order_id = sale_trans_order_id)) group by province) a1\n"
                    + "                    on a.province_refefence = a1.province\n"
                    + "                    left join\n"
                    + "                    (select province, nvl(count(1),0) as export_not_confirm from (select (case when b.staff_code like '%CAB%' then 'CAB' when b.staff_code like '%GAZ%' then 'GAZ'\n"
                    + "                            when b.staff_code like '%INH%' then 'INH' when b.staff_code like '%MAC%' then 'MAC'\n"
                    + "                            when b.staff_code like '%MAN%' then 'MAN' when b.staff_code like '%MAT%' then 'MAT'\n"
                    + "                            when b.staff_code like '%MOC%' then 'MOC' when b.staff_code like '%NAC%' then 'NAC'\n"
                    + "                            when b.staff_code like '%NAM%' then 'NAM' when b.staff_code like '%NIA%' then 'NIA'\n"
                    + "                            when b.staff_code like '%SOF%' then 'SOF' when b.staff_code like '%TET%' then 'TET'\n"
                    + "                            when b.staff_code like '%ZAM%' then 'ZAM' else 'UNKNOWN' end) province from sm.sale_trans_order a,\n"
                    + "                            sm.staff b where a.receiver_id = b.staff_id and sale_trans_date > '01-jul-2019' and\n"
                    + "                    a.order_from = 'mBCCS' and a.status = 6 and a.over_quantity = 0\n"
                    + "                    and not exists (select 1 from sm.agent_order_pincode where status = 0 and a.sale_trans_order_id = sale_trans_order_id)) group by province) a2\n"
                    + "                    on a.province_refefence = a2.province\n"
                    + "                    left join\n"
                    + "                    (select province, nvl(count(1),0) as reject_in_day from (select (case when b.staff_code like '%CAB%' then 'CAB' when b.staff_code like '%GAZ%' then 'GAZ'\n"
                    + "                            when b.staff_code like '%INH%' then 'INH' when b.staff_code like '%MAC%' then 'MAC'\n"
                    + "                            when b.staff_code like '%MAN%' then 'MAN' when b.staff_code like '%MAT%' then 'MAT'\n"
                    + "                            when b.staff_code like '%MOC%' then 'MOC' when b.staff_code like '%NAC%' then 'NAC'\n"
                    + "                            when b.staff_code like '%NAM%' then 'NAM' when b.staff_code like '%NIA%' then 'NIA'\n"
                    + "                            when b.staff_code like '%SOF%' then 'SOF' when b.staff_code like '%TET%' then 'TET'\n"
                    + "                            when b.staff_code like '%ZAM%' then 'ZAM' else 'UNKNOWN' end) province from sm.sale_trans_order a,\n"
                    + "                            sm.staff b where a.receiver_id = b.staff_id and sale_trans_date > trunc(sysdate) and a.order_from = 'mBCCS' and a.status = 4\n"
                    + "                            and not exists (select 1 from sm.agent_order_pincode where status = 0 and a.sale_trans_order_id = sale_trans_order_id)\n"
                    + "                    ) group by province) a5\n"
                    + "                    on a.province_refefence = a5.province\n"
                    + "                    left join\n"
                    + "                    (select province, nvl(count(1),0) as reject_in_month from (select (case when b.staff_code like '%CAB%' then 'CAB' when b.staff_code like '%GAZ%' then 'GAZ'\n"
                    + "                            when b.staff_code like '%INH%' then 'INH' when b.staff_code like '%MAC%' then 'MAC'\n"
                    + "                            when b.staff_code like '%MAN%' then 'MAN' when b.staff_code like '%MAT%' then 'MAT'\n"
                    + "                            when b.staff_code like '%MOC%' then 'MOC' when b.staff_code like '%NAC%' then 'NAC'\n"
                    + "                            when b.staff_code like '%NAM%' then 'NAM' when b.staff_code like '%NIA%' then 'NIA'\n"
                    + "                            when b.staff_code like '%SOF%' then 'SOF' when b.staff_code like '%TET%' then 'TET'\n"
                    + "                            when b.staff_code like '%ZAM%' then 'ZAM' else 'UNKNOWN' end) province from sm.sale_trans_order a,\n"
                    + "                            sm.staff b where a.receiver_id = b.staff_id and sale_trans_date > trunc(sysdate,'mm') and a.order_from = 'mBCCS' and a.status = 4\n"
                    + "                            and not exists (select 1 from sm.agent_order_pincode where status = 0 and a.sale_trans_order_id = sale_trans_order_id)\n"
                    + "                    ) group by province) a6\n"
                    + "                    on a.province_refefence = a6.province\n"
                    + "                    left join\n"
                    + "                    (select province, nvl(count(1),0) as less_than_3h from (select (case when b.staff_code like '%CAB%' then 'CAB' when b.staff_code like '%GAZ%' then 'GAZ'\n"
                    + "                            when b.staff_code like '%INH%' then 'INH' when b.staff_code like '%MAC%' then 'MAC'\n"
                    + "                            when b.staff_code like '%MAN%' then 'MAN' when b.staff_code like '%MAT%' then 'MAT'\n"
                    + "                            when b.staff_code like '%MOC%' then 'MOC' when b.staff_code like '%NAC%' then 'NAC'\n"
                    + "                            when b.staff_code like '%NAM%' then 'NAM' when b.staff_code like '%NIA%' then 'NIA'\n"
                    + "                            when b.staff_code like '%SOF%' then 'SOF' when b.staff_code like '%TET%' then 'TET'\n"
                    + "                            when b.staff_code like '%ZAM%' then 'ZAM' else 'UNKNOWN' end) province from sm.agent_order_active_duration a,\n"
                    + "                            sm.staff b where a.receiver_id = b.staff_id and a.duration < 3 and a.status = 0\n"
                    + "                            and not exists (select 1 from sm.agent_order_pincode where status = 0 and a.sale_trans_order_id = sale_trans_order_id)\n"
                    + "                    ) group by province) a7\n"
                    + "                    on a.province_refefence = a7.province\n"
                    + "                    left join\n"
                    + "                    (select province, nvl(count(1),0) as between_3h_5h from (select (case when b.staff_code like '%CAB%' then 'CAB' when b.staff_code like '%GAZ%' then 'GAZ'\n"
                    + "                            when b.staff_code like '%INH%' then 'INH' when b.staff_code like '%MAC%' then 'MAC'\n"
                    + "                            when b.staff_code like '%MAN%' then 'MAN' when b.staff_code like '%MAT%' then 'MAT'\n"
                    + "                            when b.staff_code like '%MOC%' then 'MOC' when b.staff_code like '%NAC%' then 'NAC'\n"
                    + "                            when b.staff_code like '%NAM%' then 'NAM' when b.staff_code like '%NIA%' then 'NIA'\n"
                    + "                            when b.staff_code like '%SOF%' then 'SOF' when b.staff_code like '%TET%' then 'TET'\n"
                    + "                            when b.staff_code like '%ZAM%' then 'ZAM' else 'UNKNOWN' end) province from sm.agent_order_active_duration a,\n"
                    + "                            sm.staff b where a.receiver_id = b.staff_id and a.duration >= 3 and a.duration < 5 and a.status = 0\n"
                    + "                            and not exists (select 1 from sm.agent_order_pincode where status = 0 and a.sale_trans_order_id = sale_trans_order_id)\n"
                    + "                    ) group by province) a8\n"
                    + "                    on a.province_refefence = a8.province\n"
                    + "                    left join\n"
                    + "                    (select province, nvl(count(1),0) as between_5h_8h from (select (case when b.staff_code like '%CAB%' then 'CAB' when b.staff_code like '%GAZ%' then 'GAZ'\n"
                    + "                            when b.staff_code like '%INH%' then 'INH' when b.staff_code like '%MAC%' then 'MAC'\n"
                    + "                            when b.staff_code like '%MAN%' then 'MAN' when b.staff_code like '%MAT%' then 'MAT'\n"
                    + "                            when b.staff_code like '%MOC%' then 'MOC' when b.staff_code like '%NAC%' then 'NAC'\n"
                    + "                            when b.staff_code like '%NAM%' then 'NAM' when b.staff_code like '%NIA%' then 'NIA'\n"
                    + "                            when b.staff_code like '%SOF%' then 'SOF' when b.staff_code like '%TET%' then 'TET'\n"
                    + "                            when b.staff_code like '%ZAM%' then 'ZAM' else 'UNKNOWN' end) province from sm.agent_order_active_duration a,\n"
                    + "                            sm.staff b where a.receiver_id = b.staff_id and a.duration >= 5 and a.duration < 8 and a.status = 0\n"
                    + "                            and not exists (select 1 from sm.agent_order_pincode where status = 0 and a.sale_trans_order_id = sale_trans_order_id)\n"
                    + "                    ) group by province) a9\n"
                    + "                    on a.province_refefence = a9.province\n"
                    + "                    left join\n"
                    + "                    (select province, nvl(count(1),0) as over_8h from (select (case when b.staff_code like '%CAB%' then 'CAB' when b.staff_code like '%GAZ%' then 'GAZ'\n"
                    + "                            when b.staff_code like '%INH%' then 'INH' when b.staff_code like '%MAC%' then 'MAC'\n"
                    + "                            when b.staff_code like '%MAN%' then 'MAN' when b.staff_code like '%MAT%' then 'MAT'\n"
                    + "                            when b.staff_code like '%MOC%' then 'MOC' when b.staff_code like '%NAC%' then 'NAC'\n"
                    + "                            when b.staff_code like '%NAM%' then 'NAM' when b.staff_code like '%NIA%' then 'NIA'\n"
                    + "                            when b.staff_code like '%SOF%' then 'SOF' when b.staff_code like '%TET%' then 'TET'\n"
                    + "                            when b.staff_code like '%ZAM%' then 'ZAM' else 'UNKNOWN' end) province from sm.agent_order_active_duration a,\n"
                    + "                            sm.staff b where a.receiver_id = b.staff_id and a.duration > 8 and a.status = 0\n"
                    + "                            and not exists (select 1 from sm.agent_order_pincode where status = 0 and a.sale_trans_order_id = sale_trans_order_id)\n"
                    + "                    ) group by province) a10\n"
                    + "                    on a.province_refefence = a10.province\n"
                    + "                    where a.district is null and a.precinct is null\n"
                    + "                    --order by a.province\n"
                    + "union \n"
                    + "(select 'PINCODE' as province_refefence, a1.approve_not_confirm, a2.export_not_confirm, a3.reject_in_day, \n"
                    + "a4.reject_in_month, a5.less_than_3h, a6.between_3h_5h, a7.between_5h_8h, a8.over_8h from \n"
                    + "(select nvl(count(1),0) approve_not_confirm from sm.sale_trans_order a,\n"
                    + "                            sm.staff b where a.receiver_id = b.staff_id and sale_trans_date > '01-jul-2019' and\n"
                    + "                    a.order_from = 'mBCCS' and a.status <> 4 and a.over_quantity = 0 \n"
                    + "                    and exists (select 1 from sm.agent_order_pincode where status = 0 and a.sale_trans_order_id = sale_trans_order_id)) a1\n"
                    + ",\n"
                    + "(select nvl(count(1),0) export_not_confirm from sm.sale_trans_order a,\n"
                    + "                            sm.staff b where a.receiver_id = b.staff_id and sale_trans_date > '01-jul-2019' and\n"
                    + "                    a.order_from = 'mBCCS' and a.status = 6 and a.over_quantity = 0\n"
                    + "                    and exists (select 1 from sm.agent_order_pincode where status = 0 and a.sale_trans_order_id = sale_trans_order_id)) a2\n"
                    + ",\n"
                    + "(select nvl(count(1),0) reject_in_day from sm.sale_trans_order a,\n"
                    + "                            sm.staff b where a.receiver_id = b.staff_id and sale_trans_date > trunc(sysdate) and a.order_from = 'mBCCS' and a.status = 4\n"
                    + "                            and exists (select 1 from sm.agent_order_pincode where status = 0 and a.sale_trans_order_id = sale_trans_order_id)) a3\n"
                    + ",\n"
                    + "(select nvl(count(1),0) reject_in_month from sm.sale_trans_order a,\n"
                    + "                            sm.staff b where a.receiver_id = b.staff_id and sale_trans_date > trunc(sysdate,'mm') and a.order_from = 'mBCCS' and a.status = 4\n"
                    + "                            and exists (select 1 from sm.agent_order_pincode where status = 0 and a.sale_trans_order_id = sale_trans_order_id)) a4\n"
                    + ",    \n"
                    + "(select nvl(count(1),0) less_than_3h from sm.agent_order_active_duration a,\n"
                    + "                            sm.staff b where a.receiver_id = b.staff_id and a.duration < 3 and a.status = 0\n"
                    + "                            and exists (select 1 from sm.agent_order_pincode where status = 0 and a.sale_trans_order_id = sale_trans_order_id))  a5\n"
                    + ",\n"
                    + "(select nvl(count(1),0) between_3h_5h from sm.agent_order_active_duration a,\n"
                    + "                            sm.staff b where a.receiver_id = b.staff_id and a.duration >= 3 and a.duration < 5 and a.status = 0\n"
                    + "                            and exists (select 1 from sm.agent_order_pincode where status = 0 and a.sale_trans_order_id = sale_trans_order_id)) a6           \n"
                    + ",\n"
                    + "(select nvl(count(1),0) between_5h_8h from sm.agent_order_active_duration a,\n"
                    + "                            sm.staff b where a.receiver_id = b.staff_id and a.duration >= 5 and a.duration < 8 and a.status = 0\n"
                    + "                            and exists (select 1 from sm.agent_order_pincode where status = 0 and a.sale_trans_order_id = sale_trans_order_id)) a7\n"
                    + ",\n"
                    + "(select nvl(count(1),0) over_8h from sm.agent_order_active_duration a,\n"
                    + "                            sm.staff b where a.receiver_id = b.staff_id and a.duration > 8 and a.status = 0\n"
                    + "                            and exists (select 1 from sm.agent_order_pincode where status = 0 and a.sale_trans_order_id = sale_trans_order_id)) a8)";
            ps = connection.prepareStatement(sql);
            rs = ps.executeQuery();
            while (rs.next()) {
                String province = rs.getString("province_refefence");
                int agentNotConfirm = rs.getInt("agent_not_confirm");
                int exportNotConfirm = rs.getInt("export_not_confirm");
                int rejectInDay = rs.getInt("reject_in_day");
                int rejectInMonth = rs.getInt("reject_in_month");
                int lessThan3h = rs.getInt("less_than_3h");
                int lessThan5h = rs.getInt("between_3h_5h");
                int lessThan8h = rs.getInt("between_5h_8h");
                int over8h = rs.getInt("over_8h");

                AgentReportInfo agentReportInfo = new AgentReportInfo();
                agentReportInfo.setProvince(province);
                agentReportInfo.setAgentNotConfirm(agentNotConfirm);
                agentReportInfo.setExportNotConfirm(exportNotConfirm);
                agentReportInfo.setRejectInDay(rejectInDay);
                agentReportInfo.setRejectInMonth(rejectInMonth);
                agentReportInfo.setNotActiveLessThan3h(lessThan3h);
                agentReportInfo.setNotActiveLessThan5h(lessThan5h);
                agentReportInfo.setNotActiveLessThan8h(lessThan8h);
                agentReportInfo.setNotActiveOver8h(over8h);

                lstReport.add(agentReportInfo);

            }
        } catch (Exception ex) {
            ex.printStackTrace();
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR getAgentOwnerCode: ");
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return lstReport;
        }
    }

    public int resetDataReport() {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            String sql = "update agent_report_daily set agent_not_confirm = 0, approve_over_2h = 0, approve_over_24h= 0, approve_over_72h = 0, export_not_confirm = 0,\n"
                    + "reject_in_day = 0, reject_in_month = 0, less_than_3h = 0, between_3h_5h = 0, between_5h_8h = 0, over_8h= 0";
            ps = connection.prepareStatement(sql);

            result = ps.executeUpdate();
        } catch (Exception ex) {
            ex.printStackTrace();
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR resetDataReport.");
            logger.error(br + ex.toString());
            logger.error(AppManager.logException(startTime, ex));
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int updateBasicInfo(AgentReportInfo obj) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            String sql = "update agent_report_daily set agent_not_confirm = ?, export_not_confirm = ?,\n"
                    + "reject_in_day = ?, reject_in_month = ?, less_than_3h = ?, between_3h_5h = ?, between_5h_8h = ?, over_8h= ?\n"
                    + "where province_refefence = ?";
            ps = connection.prepareStatement(sql);
            ps.setInt(1, obj.getAgentNotConfirm());
            ps.setInt(2, obj.getExportNotConfirm());
            ps.setInt(3, obj.getRejectInDay());
            ps.setInt(4, obj.getRejectInMonth());
            ps.setInt(5, obj.getNotActiveLessThan3h());
            ps.setInt(6, obj.getNotActiveLessThan5h());
            ps.setInt(7, obj.getNotActiveLessThan8h());
            ps.setInt(8, obj.getNotActiveOver8h());
            ps.setString(9, obj.getProvince());

            result = ps.executeUpdate();

            logger.info("End updateBasicInfo branch " + obj.getProvince() + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));

        } catch (Exception ex) {
            ex.printStackTrace();
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR updateBasicInfo.");
            logger.error(br + ex.toString());
            logger.error(AppManager.logException(startTime, ex));
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public String getReceiverCode(Long receiverId) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        String staffCode = "";
        StringBuilder br = new StringBuilder();
        String sql = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "select * from sm.staff where staff_id = ?";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, receiverId);
            rs = ps.executeQuery();
            while (rs.next()) {
                staffCode = rs.getString("staff_code");
            }
            logger.info("End getAgentMakeOrder staffCode " + staffCode + " receiverId " + receiverId + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            ex.printStackTrace();
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR getAgentMakeOrder: ").
                    append(sql).append("\n")
                    .append(" receiverId ")
                    .append(receiverId);
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return staffCode;
        }
    }

    public int updateAgentOrderOver2h(String province) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            String sql = "update agent_report_daily set approve_over_2h = (approve_over_2h + 1)\n"
                    + "where province_refefence = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, province);

            result = ps.executeUpdate();

            logger.info("End updateAgentOrderOver2h branch " + province + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));

        } catch (Exception ex) {
            ex.printStackTrace();
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR updateAgentOrderOver2h.");
            logger.error(br + ex.toString());
            logger.error(AppManager.logException(startTime, ex));
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int updateAgentOrderOver24h(String province) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            String sql = "update agent_report_daily set approve_over_24h = (approve_over_24h + 1)\n"
                    + "where province_refefence = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, province);

            result = ps.executeUpdate();

            logger.info("End updateAgentOrderOver24h branch " + province + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));

        } catch (Exception ex) {
            ex.printStackTrace();
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR updateAgentOrderOver24h.");
            logger.error(br + ex.toString());
            logger.error(AppManager.logException(startTime, ex));
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int updateAgentOrderOver72h(String province) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            String sql = "update agent_report_daily set approve_over_72h = (approve_over_72h + 1)\n"
                    + "where province_refefence = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, province);

            result = ps.executeUpdate();

            logger.info("End updateAgentOrderOver72h branch " + province + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));

        } catch (Exception ex) {
            ex.printStackTrace();
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR updateAgentOrderOver72h.");
            logger.error(br + ex.toString());
            logger.error(AppManager.logException(startTime, ex));
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public String getNameOfAgentCode(String agentCode) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        String name = "";
        StringBuilder br = new StringBuilder();
        String sql = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "select * from agent_order_info where upper(agent_code) = upper(?)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, agentCode);
            rs = ps.executeQuery();
            while (rs.next()) {
                name = rs.getString("name");
            }
            logger.info("End getNameOfAgentCode agentCode " + agentCode + " name " + name + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR getAgentOwnerCode: ").
                    append(sql).append("\n")
                    .append(" agentCode ")
                    .append(agentCode);
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return name;
        }
    }

    public String getPriceOfScratchCard(String serial) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        String price = "0";
        StringBuilder br = new StringBuilder();
        String sql = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "select TRIM(TO_CHAR(price, '999,999,999,999,999')) price from sm.price "
                    + "where stock_model_id = (select stock_model_id from sm.stock_card where serial = to_number(?)) \n"
                    + "and status = 1 and trunc(sysdate) > sta_date \n"
                    + "and (end_date is null or trunc(sysdate) < end_date) and type = 9 and price_policy = 1";
            ps = connection.prepareStatement(sql);
            ps.setString(1, serial);
            rs = ps.executeQuery();
            while (rs.next()) {
                price = rs.getString("price");
            }
            logger.info("End getPriceOfScratchCard serial " + serial + " price " + price + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR getPriceOfScratchCard: ").
                    append(sql).append("\n")
                    .append(" serial ")
                    .append(serial);
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return price;
        }
    }

    public boolean checkSaleScratchCard(String serial) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        boolean result = false;
        StringBuilder br = new StringBuilder();
        String sql = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "select * from sm.stock_card where serial = to_number(?) and status = 0";
            ps = connection.prepareStatement(sql);
            ps.setString(1, serial);
            rs = ps.executeQuery();
            while (rs.next()) {
                result = true;
                break;
            }
            logger.info("End checkSaleScratchCard serial " + serial + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR checkSaleScratchCard: ").
                    append(sql).append("\n")
                    .append(" serial ")
                    .append(serial);
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public boolean checkExistAgentOrderActiveDuration(String fromSerial, String toSerial, long saleTransOrderId) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        boolean result = false;
        StringBuilder br = new StringBuilder();
        String sql = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "select * from agent_order_active_duration where from_serial = ? and to_serial = ? and sale_trans_order_id = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, fromSerial);
            ps.setString(2, toSerial);
            ps.setLong(3, saleTransOrderId);
            rs = ps.executeQuery();
            while (rs.next()) {
                result = true;
                break;
            }
            logger.info("End checkExistAgentOrderActiveDuration fromSerial: " + fromSerial + ", saleTransOrderId " + saleTransOrderId + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR checkExistAgentOrderActiveDuration: ").
                    append(sql).append("\n")
                    .append(" saleTransOrderId ")
                    .append(saleTransOrderId);
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public boolean checkOrderPincode(long saleTransOrderId) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        boolean result = false;
        StringBuilder br = new StringBuilder();
        String sql = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "select * from sm.agent_order_pincode where sale_trans_order_id = ?";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, saleTransOrderId);
            rs = ps.executeQuery();
            while (rs.next()) {
                result = true;
                break;
            }
            logger.info("End checkOrderPincode saleTransOrderId " + saleTransOrderId + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR checkOrderPincode: ").
                    append(sql).append("\n")
                    .append(" saleTransOrderId ")
                    .append(saleTransOrderId);
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int insertAgentOrderDuration(int status, String fromSerial, String toSerial, long saleTransOrderId) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "INSERT INTO agent_order_active_duration (SALE_TRANS_ORDER_ID,FROM_SERIAL,TO_SERIAL,DURATION,RECEIVER_ID,STATUS,CREATE_TIME) \n"
                    + "VALUES(?,?,?,null,(select receiver_id from sm.agent_trans_order_his where sale_trans_order_id = ? and rownum < 2),?,sysdate)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, saleTransOrderId);
            ps.setString(2, fromSerial);
            ps.setString(3, toSerial);
            ps.setLong(4, saleTransOrderId);
            ps.setInt(5, status);
            result = ps.executeUpdate();
            logger.info("End insertAgentOrderDuration fromSerial " + fromSerial
                    + " toSerial " + toSerial
                    + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertActionLog: ").
                    append(sql).append("\n")
                    .append(" fromSerial ")
                    .append(fromSerial)
                    .append(" toSerial ")
                    .append(toSerial);
            logger.error(br + ex.toString());
            logger.error(AppManager.logException(startTime, ex));
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public long countTotalRangeSerial(long saleTransOrderId) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        long total = 0;
        StringBuilder br = new StringBuilder();
        String sql = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "select count(1) as total from sm.agent_order_vc_request where sale_trans_order_id = ?";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, saleTransOrderId);
            rs = ps.executeQuery();
            while (rs.next()) {
                total = rs.getLong("total");
            }
            logger.info("End countTotalRangeSerial saleTransOrderId " + saleTransOrderId + " total " + total + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR countTotalRangeSerial: ").
                    append(sql).append("\n")
                    .append(" saleTransOrderId ")
                    .append(saleTransOrderId);
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return total;
        }
    }

    public long countCurrentRangeSerialActive(long saleTransOrderId) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        long total = 0;
        StringBuilder br = new StringBuilder();
        String sql = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "select count(1) as total from agent_order_active_duration where sale_trans_order_id = ? and status = 1";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, saleTransOrderId);
            rs = ps.executeQuery();
            while (rs.next()) {
                total = rs.getLong("total");
            }
            logger.info("End countCurrentRangeSerialActive saleTransOrderId " + saleTransOrderId + " total " + total + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR countCurrentRangeSerialActive: ").
                    append(sql).append("\n")
                    .append(" saleTransOrderId ")
                    .append(saleTransOrderId);
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return total;
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

    public String getTelByStaffCode(String staffCode) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        String tel = null;
        String sqlMo = " select cellphone from vsa_v3.users where user_name = ? and status = 1";
        PreparedStatement psMo = null;
        try {
            connection = getConnection(dbNameCofig);
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

    @Override
    public int[] insertQueueHis(List<Record> listRecords) {
        int[] res = new int[0];
        return res;
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
                sb.append(":" + sd);
            }
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
        int[] res = new int[0];
        return res;
    }

//    public int sendSms(String msisdn, String message, String channel) {
//        Connection connection = null;
//        PreparedStatement ps = null;
//        StringBuilder br = new StringBuilder();
//        String sql = "";
//        int result = 0;
//        long startTime = System.currentTimeMillis();
//        try {
//            connection = getConnection("dbsm");
//            sql = "INSERT INTO mt (mt_id,msisdn,message,mo_his_id,retry_num,receive_time,channel) "
//                    + "VALUES(mt_SEQ.nextval,?,?,null,0,sysdate,?)";
//            ps = connection.prepareStatement(sql);
//            if (!msisdn.startsWith("258")) {
//                msisdn = "258" + msisdn;
//            }
//            ps.setString(1, msisdn.trim());
//            ps.setString(2, message.trim());
//            ps.setString(3, channel.trim());
//            result = ps.executeUpdate();
//            logger.info("End sendSms isdn " + msisdn + " message " + message + " result " + result + " time "
//                    + (System.currentTimeMillis() - startTime));
//        } catch (Exception ex) {
//            br.setLength(0);
//            br.append(loggerLabel).append(new Date()).
//                    append("\nERROR sendSms: ").
//                    append(sql).append("\n")
//                    .append(" isdn ")
//                    .append(msisdn)
//                    .append(" message ")
//                    .append(message)
//                    .append(" result ")
//                    .append(result);
//            logger.error(br + ex.toString());
//            logger.error(AppManager.logException(startTime, ex));
//        } finally {
//            closeStatement(ps);
//            closeConnection(connection);
//            return result;
//        }
//    }
//    public int sendSms(String msisdn, String message, String channel) {
//        Connection connection = null;
//        PreparedStatement ps = null;
//        StringBuilder br = new StringBuilder();
//        String sql = "";
//        int result = 0;
//        long startTime = System.currentTimeMillis();
//        try {
//            connection = getConnection("dbapp2");
//            sql = "INSERT INTO mt (mt_id,msisdn,message,mo_his_id,retry_num,receive_time,channel) "
//                    + "VALUES(mt_SEQ.nextval,?,?,null,0,sysdate,?)";
//            ps = connection.prepareStatement(sql);
//            if (!msisdn.startsWith("258")) {
//                msisdn = "258" + msisdn;
//            }
//            ps.setString(1, msisdn);
//            ps.setString(2, message);
//            ps.setString(3, channel);
//            result = ps.executeUpdate();
//            logger.info("End sendSms isdn " + msisdn + " message " + message + " result " + result + " time "
//                    + (System.currentTimeMillis() - startTime));
//        } catch (Exception ex) {
//            br.setLength(0);
//            br.append(loggerLabel).append(new Date()).
//                    append("\nERROR sendSms: ").
//                    append(sql).append("\n")
//                    .append(" isdn ")
//                    .append(msisdn)
//                    .append(" message ")
//                    .append(message)
//                    .append(" result ")
//                    .append(result);
//            logger.error(br + ex.toString());
//        } finally {
//            closeStatement(ps);
//            closeConnection(connection);
//            return result;
//        }
//    }
    public int sendSmsV2(String msisdn, String message, String channel) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "insert into mt values(mt_seq.nextval,1,?,?,sysdate,null,?,'PROCESS')";
            ps = connection.prepareStatement(sql);
            ps.setString(1, msisdn);
            ps.setString(2, message);
            ps.setString(3, channel);
            result = ps.executeUpdate();
            logger.info("End sendSmsV2 isdn " + msisdn + " message " + message + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR sendSmsV2: ").
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

    public int sendSmsReport(String msisdn) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "insert into mt values(mt_seq.nextval,1,?,(select 'Report AGENT ORDER: ' ||TRUNC(sysdate)|| ' from '|| TO_CHAR(TRUNC(SYSDATE - 1/24, 'HH'), 'HH24:MI:SS') || ' - '\n"
                    + "        || TO_CHAR(TRUNC(SYSDATE - 1/24, 'HH') + INTERVAL '59:59' MINUTE TO SECOND, 'HH24:MI:SS')\n"
                    + "        ||chr(10)||'1.Ordered & Approved, not confirm: >3h/>=24h/>=72h\n"
                    + "2.Exported, not confirm\n"
                    + "3.Canceled in day/in month\n"
                    + "4.Confirmed, not active: <3h/>=3h-<5h/>=5h-<8h/>=8h\n"
                    + "'||chr(10)||listagg(content,chr(10)) within group (order by content) as content from (\n"
                    + "select listagg(content,chr(10)) within group (order by id asc) as content from (\n"
                    + "select content, id from (\n"
                    + "select  province_refefence||':'||chr(10)||'1.'||approve_over_2h||'/'||approve_over_24h||'/'||approve_over_72h\n"
                    + "||chr(10)||'2.'||agent_not_confirm||'/'||export_not_confirm||chr(10)||'3.'||reject_in_day||'/'||reject_in_month\n"
                    + "||chr(10)||'4.'||less_than_3h||'/'||between_3h_5h||'/'||between_5h_8h||'/'||over_8h as content, 2 as id  from (\n"
                    + "select * from sm.agent_report_daily\n"
                    + ")\n"
                    + "union\n"
                    + "select  'MOV:'||chr(10)||'1.'||sum(approve_over_2h)||'/'||sum(approve_over_24h)||'/'||sum(approve_over_72h)\n"
                    + "||chr(10)||'2.'||sum(agent_not_confirm)||'/'||sum(export_not_confirm)\n"
                    + "||chr(10)||'3.'||sum(reject_in_day)||'/'||sum(reject_in_month)\n"
                    + "||chr(10)||'4.'||sum(less_than_3h)||'/'||sum(between_3h_5h)||'/'||sum(between_5h_8h)||'/'||sum(over_8h) as content, 1 as id  from (\n"
                    + "select * from sm.agent_report_daily\n"
                    + ") order by id asc\n"
                    + ")\n"
                    + ")\n"
                    + ")),sysdate,null,'86952','PROCESS')";
            ps = connection.prepareStatement(sql);
            ps.setString(1, msisdn);
            result = ps.executeUpdate();
            logger.info("End sendSmsReport isdn " + msisdn + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR sendSmsReport: ").
                    append(sql).append("\n")
                    .append(" isdn ")
                    .append(msisdn)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }
}
