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
import com.viettel.vas.wsfw.object.BankTransfer;
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
public class DbSyncBankTransfer extends DbProcessorAbstract {

	private String loggerLabel = DbSyncBankTransfer.class.getSimpleName() + ": ";
	private PoolStore poolStore;
	private String dbNameCofig;

	public DbSyncBankTransfer() throws SQLException, Exception {
		this.logger = Logger.getLogger(loggerLabel);
		dbNameCofig = ResourceBundle.getBundle("vas").getString("dbsm");
		poolStore = new PoolStore(dbNameCofig, logger);
	}

	public DbSyncBankTransfer(String sessionName, Logger logger) throws SQLException, Exception {
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

	public List<BankTransfer> getListBankTransfer(String fromDate, String toDate) {
		long timeSt = System.currentTimeMillis();
		Connection connection = null;
		ResultSet rs1 = null;
//		String sql = "select tranfer_code,amount,bank_name,to_char(bank_create,'dd-MM-yyyy') bank_create,create_user,update_time,purpose,status,staff_request,\n"
//				+ "account_bank ,create_time,description,nvl(shop_code_lv3,'MV')shop_code_lv3,nvl(shop_code_lv4,'MV')shop_code_lv4,\n"
//				+ "nvl(shop_code_lv5,'MV')shop_code_lv5\n"
//				+ "from (\n"
//				+ "select a.tranfer_code,a.amount,a.bank_name,a.bank_create,a.create_user,a.update_time,a.purpose,a.status,a.staff_request\n"
//				+ ",a.account_bank ,a.create_time,a.description,\n"
//				+ "(select a1.shop_code_lv3 from  sm.stock_owner_tmp a1 where a1.owner_id = b.shop_id) shop_code_lv3,\n"
//				+ "(select a1.shop_code_lv4 from  sm.stock_owner_tmp a1 where a1.owner_id = b.shop_id) shop_code_lv4,\n"
//				+ "(select a1.shop_code_lv5 from  sm.stock_owner_tmp a1 where a1.owner_id = b.shop_id) shop_code_lv5\n"
//				+ "from sm.bank_tranfer_info a, sm.staff b \n"
//				+ "where a.bank_create  >= to_date(?,'dd-MM-yyyy HH24MISS') and a.bank_create < to_date(?,'dd-MM-yyyy HH24MISS')  and lower(b.staff_code(+)) = lower(a.staff_request)\n"
//				+ ") ";
		String sql = "select * from report.sync_bank_transfer_bccs a where a.rp_date  >= to_date(?,'dd-MM-yyyy HH24MISS') and a.rp_date < to_date(?,'dd-MM-yyyy HH24MISS') ";
		PreparedStatement ps = null;
		List<BankTransfer> listBankTransfer = null;
		try {
			listBankTransfer = new ArrayList<BankTransfer>();
			connection = ConnectionPoolManager.getConnection("dbCdr");
			ps = connection.prepareStatement(sql);
			ps.setString(1, fromDate + " 000000");
			ps.setString(2, toDate + " 235959");
			if (QUERY_TIMEOUT > 0) {
				ps.setQueryTimeout(QUERY_TIMEOUT);
			}
			rs1 = ps.executeQuery();
			while (rs1.next()) {
				BankTransfer doc = new BankTransfer();
				doc.setBankName(retNull(rs1.getString("bank_name")));
				doc.setTransferCode(retNull(rs1.getString("tranfer_code")));
				doc.setAmount(new BigDecimal(rs1.getString("amount")));
				doc.setPurpose(rs1.getInt("purpose"));
				doc.setDescription(retNull(rs1.getString("description")));
				doc.setBankAccount(retNull(rs1.getString("account_bank")));
				doc.setBankDate(retNull(rs1.getString("bank_create")));
				doc.setShopCodeLv1(retNull(rs1.getString("shop_code_lv3")));
				doc.setShopCodeLv2(retNull(rs1.getString("shop_code_lv4")));
				doc.setShopCodeLv3(retNull(rs1.getString("shop_code_lv5")));
				listBankTransfer.add(doc);
			}
			return listBankTransfer;
		} catch (Exception ex) {
			logger.error("ERROR getListBankTransfer");
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
