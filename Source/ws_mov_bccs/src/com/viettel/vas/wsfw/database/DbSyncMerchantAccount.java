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
import com.viettel.vas.wsfw.object.MerchantAccount;
import java.math.BigDecimal;
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
public class DbSyncMerchantAccount extends DbProcessorAbstract {

	private String loggerLabel = DbSyncMerchantAccount.class.getSimpleName() + ": ";
	private PoolStore poolStore;
	private String dbNameCofig;

	public DbSyncMerchantAccount() throws SQLException, Exception {
		this.logger = Logger.getLogger(loggerLabel);
		dbNameCofig = ResourceBundle.getBundle("vas").getString("dbsm");
		poolStore = new PoolStore(dbNameCofig, logger);
	}

	public DbSyncMerchantAccount(String sessionName, Logger logger) throws SQLException, Exception {
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

	public List<MerchantAccount> getListMerchantAccount(String fromDate, String toDate) {
		long timeSt = System.currentTimeMillis();
		Connection connection = null;
		ResultSet rs1 = null;
//		String sql = "select p2.mvt_trans_code,p1.emola_code,p1.created_date,p1.total_amount,p1.staff_code,p1.shop_code,p1.purpose,shop_code_lv3,shop_code_lv4,shop_code_lv5 from (\n"
//				+ "select distinct tb1.*  from (\n"
//				+ "select   a.mvt_trans_code,a.emola_code,to_char(a.created_date,'dd-MM-yyyy') created_date, a.total_amount,c.staff_code,d.shop_code,4 as purpose ,e.shop_code_lv3,e.shop_code_lv4,e.shop_code_lv5\n"
//				+ "from  report.sync_debit_emola_bccs@DBL_REPORT a,sm.deposit b, sm.staff c,sm.shop d, sm.stock_owner_tmp e where \n"
//				+ "a.created_date > to_date('" + fromDate + " 000000','dd-MM-yyyy HH24MISS') and a.created_date < to_date('" + toDate + " 235959','dd-MM-yyyy HH24MISS')\n"
//				+ "and b.create_date > to_date('" + fromDate + " 000000','dd-MM-yyyy HH24MISS') and b.create_date < to_date('" + toDate + " 235959','dd-MM-yyyy HH24MISS')\n"
//				+ "and b.staff_id = c.staff_id and b.shop_id = d.shop_id\n"
//				+ "and a.mvt_trans_code = b.clear_debit_request_id\n"
//				+ "and d.shop_id = e.owner_id(+)\n"
//				+ "\n"
//				+ "union all\n"
//				+ "select  a.mvt_trans_code,a.emola_code,to_char(a.created_date,'dd-MM-yyyy') created_date, a.total_amount,c.staff_code,d.shop_code,1  as purpose,e.shop_code_lv3,e.shop_code_lv4,e.shop_code_lv5 from \n"
//				+ " report.sync_debit_emola_bccs@DBL_REPORT a ,  itbusiness.sale_debit_cleardate_new  b,sm.staff c,sm.shop d, sm.stock_owner_tmp e  where \n"
//				+ "a.created_date > to_date('" + fromDate + " 000000','dd-MM-yyyy HH24MISS') and a.created_date < to_date('" + toDate + " 235959','dd-MM-yyyy HH24MISS')\n"
//				+ "and b.clear_debit_time > to_date('" + fromDate + " 000000','dd-MM-yyyy HH24MISS') and b.clear_debit_time < to_date('" + toDate + " 235959','dd-MM-yyyy HH24MISS')\n"
//				+ "and a.mvt_trans_code = b.clear_trans_code\n"
//				+ "and lower(b.staff) = lower(c.staff_code)\n"
//				+ "and c.shop_id = d.shop_id \n"
//				+ "and b.clear_user <> 'SYSTEM_AUTO_CN_KIT'\n"
//				+ "and d.shop_id = e.owner_id(+)\n"
//				+ "\n"
//				+ "union all\n"
//				+ "\n"
//				+ "select  a.mvt_trans_code,a.emola_code,to_char(a.created_date,'dd-MM-yyyy') created_date, a.total_amount,c.staff_code,d.shop_code,1  as purpose,e.shop_code_lv3,e.shop_code_lv4,e.shop_code_lv5 from \n"
//				+ " report.sync_debit_emola_bccs@DBL_REPORT a ,  itbusiness.sale_debit_cleardate_new  b,sm.staff c,sm.shop d, sm.stock_owner_tmp e where \n"
//				+ "a.created_date > to_date('" + fromDate + " 000000','dd-MM-yyyy HH24MISS') and a.created_date < to_date('" + toDate + " 235959','dd-MM-yyyy HH24MISS')\n"
//				+ "and b.clear_debit_time > to_date('" + fromDate + " 000000','dd-MM-yyyy HH24MISS') and b.clear_debit_time < to_date('" + toDate + " 235959','dd-MM-yyyy HH24MISS')\n"
//				+ "and a.emola_code = b.clear_trans_code\n"
//				+ "and lower(b.staff) = lower(c.staff_code)\n"
//				+ "and c.shop_id = d.shop_id \n"
//				+ "and b.clear_user = 'SYSTEM_AUTO_CN_KIT'\n"
//				+ "and d.shop_id = e.owner_id(+)\n"
//				+ "\n"
//				+ "union all\n"
//				+ "select  a.mvt_trans_code,a.emola_code,to_char(a.created_date,'dd-MM-yyyy') created_date, a.total_amount,c.staff_code,d.shop_code,3  as purpose,e.shop_code_lv3,e.shop_code_lv4,e.shop_code_lv5 from \n"
//				+ " report.sync_debit_emola_bccs@DBL_REPORT a ,  itbusiness.sale_float_cleardate  b,sm.staff c,sm.shop d, sm.stock_owner_tmp e where \n"
//				+ "a.created_date > to_date('" + fromDate + " 000000','dd-MM-yyyy HH24MISS') and a.created_date < to_date('" + toDate + " 235959','dd-MM-yyyy HH24MISS')\n"
//				+ "and b.clear_debit_time > to_date('" + fromDate + " 000000','dd-MM-yyyy HH24MISS') and b.clear_debit_time < to_date('" + toDate + " 235959','dd-MM-yyyy HH24MISS')\n"
//				+ "and a.mvt_trans_code = b.clear_trans_code\n"
//				+ "and lower(b.staff) = lower(c.staff_code)\n"
//				+ "and c.shop_id = d.shop_id \n"
//				+ "and b.clear_user <> 'SYSTEM_AUTO_CN_KIT'\n"
//				+ "and d.shop_id = e.owner_id(+)\n"
//				+ "\n"
//				+ "union all\n"
//				+ "select  a.mvt_trans_code,a.emola_code,to_char(a.created_date,'dd-MM-yyyy') created_date, a.total_amount,c.staff_code,d.shop_code,3  as purpose,e.shop_code_lv3,e.shop_code_lv4,e.shop_code_lv5 from \n"
//				+ " report.sync_debit_emola_bccs@DBL_REPORT a ,  itbusiness.sale_float_cleardate  b,sm.staff c,sm.shop d, sm.stock_owner_tmp e where \n"
//				+ "a.created_date > to_date('" + fromDate + " 000000','dd-MM-yyyy HH24MISS') and a.created_date < to_date('" + toDate + " 235959','dd-MM-yyyy HH24MISS')\n"
//				+ "and b.clear_debit_time > to_date('" + fromDate + " 000000','dd-MM-yyyy HH24MISS') and b.clear_debit_time < to_date('" + toDate + " 235959','dd-MM-yyyy HH24MISS')\n"
//				+ "and a.emola_code = b.clear_trans_code\n"
//				+ "and lower(b.staff) = lower(c.staff_code)\n"
//				+ "and c.shop_id = d.shop_id \n"
//				+ "and b.clear_user = 'SYSTEM_AUTO_CN_KIT'\n"
//				+ "and d.shop_id = e.owner_id(+)\n"
//				+ "\n"
//				+ "union all\n"
//				+ "select  a.mvt_trans_code,a.emola_code,to_char(a.created_date,'dd-MM-yyyy') created_date, a.total_amount,c.staff_code,d.shop_code,2  as purpose,e.shop_code_lv3,e.shop_code_lv4,e.shop_code_lv5\n"
//				+ " from  report.sync_debit_emola_bccs@DBL_REPORT a , payment.payment_contract@DBL_PAYMENT  b,sm.staff c,sm.shop d, sm.stock_owner_tmp e where \n"
//				+ "a.created_date > to_date('" + fromDate + " 000000','dd-MM-yyyy HH24MISS') and a.created_date < to_date('" + toDate + " 235959','dd-MM-yyyy HH24MISS')\n"
//				+ "and b.create_date > to_date('" + fromDate + " 000000','dd-MM-yyyy HH24MISS')\n"
//				+ "and a.mvt_trans_code = b.clear_debit_request_id\n"
//				+ "and b.collection_staff_id = c.staff_id\n"
//				+ "and b.collection_group_id = d.shop_id\n"
//				+ "and b.status <> 0\n"
//				+ "and d.shop_id = e.owner_id(+)\n"
//				+ "\n"
//				+ "union all\n"
//				+ "select  a.mvt_trans_code,a.emola_code,to_char(a.created_date,'dd-MM-yyyy') created_date, a.total_amount,upper(b.staff_code) staff, d.shop_code,1  as purpose ,e.shop_code_lv3,e.shop_code_lv4,e.shop_code_lv5\n"
//				+ " from  report.sync_debit_emola_bccs@DBL_REPORT a , cm_pre.ewallet_connect_kit_log b,sm.staff c,sm.shop d, sm.stock_owner_tmp e where \n"
//				+ "a.created_date > to_date('" + fromDate + " 000000','dd-MM-yyyy HH24MISS') and a.created_date < to_date('" + toDate + " 235959','dd-MM-yyyy HH24MISS')\n"
//				+ "and a.mvt_trans_code = b.request_id\n"
//				+ "and lower(b.staff_code) = lower(c.staff_code)\n"
//				+ "and c.shop_id = d.shop_id\n"
//				+ "and d.shop_id = e.owner_id(+)\n"
//				+ "\n"
//				+ "union all\n"
//				+ "select  a.mvt_trans_code,a.emola_code,to_char(a.created_date,'dd-MM-yyyy') created_date, a.total_amount total_amount,b.staff_code,'MV' shop, 1  as purpose,'MV' shop_code_lv3,'MV'shop_code_lv4,'MV'shop_code_lv5\n"
//				+ " from  report.sync_debit_emola_bccs@DBL_REPORT a ,cm_pre.EWALLET_ELITE_GROUP_LOG b where \n"
//				+ "a.created_date > to_date('" + fromDate + " 000000','dd-MM-yyyy HH24MISS') and a.created_date < to_date('" + toDate + " 235959','dd-MM-yyyy HH24MISS')\n"
//				+ "and a.mvt_trans_code = b.request_id\n"
//				+ ") tb1\n"
//				+ ") p1 ,  report.sync_debit_emola_bccs@DBL_REPORT p2  where p2.created_date > to_date('" + fromDate + " 000000','dd-MM-yyyy HH24MISS') and p2.created_date < to_date('" + toDate + " 235959','dd-MM-yyyy HH24MISS')\n"
//				+ " and p1.mvt_trans_code(+) = p2.mvt_trans_code ";
		String sql = " select * from report.sync_merchant_account_bccs a  where a.rp_date  >= to_date(?,'dd-MM-yyyy HH24MISS') and a.rp_date < to_date(?,'dd-MM-yyyy HH24MISS') ";
		PreparedStatement ps = null;
		List<MerchantAccount> listMerchantAccount = null;
		try {
			listMerchantAccount = new ArrayList<MerchantAccount>();
			connection = ConnectionPoolManager.getConnection("dbCdr");
			ps = connection.prepareStatement(sql);
			if (QUERY_TIMEOUT > 0) {
				ps.setQueryTimeout(QUERY_TIMEOUT);
			}
			ps.setString(1, fromDate + " 000000");
			ps.setString(2, toDate + " 235959");
			rs1 = ps.executeQuery();
			while (rs1.next()) {
				MerchantAccount acc = new MerchantAccount();
				acc.setTransferCode(retNull(rs1.getString("emola_code")));
				acc.setAmount(new BigDecimal(rs1.getString("total_amount") != null ? rs1.getString("total_amount") : "0"));
				acc.setPurpose(rs1.getInt("perpose"));
				acc.setCreatedDate(retNull(rs1.getString("created_date")));
				acc.setCreatedUser(retNull(rs1.getString("staff_code")));
				acc.setShopCodeLv1(retNull(rs1.getString("shop_code_lv3")));
				acc.setShopCodeLv2(retNull(rs1.getString("shop_code_lv4")));
				acc.setShopCodeLv3(retNull(rs1.getString("shop_code_lv5")));
				acc.setDescription(retNull(rs1.getString("mvt_trans_code")));
				listMerchantAccount.add(acc);
			}
			return listMerchantAccount;
		} catch (Exception ex) {
			logger.error("ERROR DbSyncMerchantAccount");
			logger.error(AppManager.logException(timeSt, ex));
			return null;
		} finally {
			closeResultSet(rs1);
			closeStatement(ps);
			closeConnection(connection);
		}
	}

	public String retNull(String str) {
		if (str == null || str.trim().length() == 0) {
			return "";
		} else {
			return str.trim();
		}
	}
}
