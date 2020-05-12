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
import com.viettel.vas.wsfw.object.EmolaFloat;
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
public class DbSyncEmolaFloat extends DbProcessorAbstract {

	private String loggerLabel = DbSyncEmolaFloat.class.getSimpleName() + ": ";
	private PoolStore poolStore;
	private String dbNameCofig;

	public DbSyncEmolaFloat() throws SQLException, Exception {
		this.logger = Logger.getLogger(loggerLabel);
		dbNameCofig = ResourceBundle.getBundle("vas").getString("dbsm");
		poolStore = new PoolStore(dbNameCofig, logger);
	}

	public DbSyncEmolaFloat(String sessionName, Logger logger) throws SQLException, Exception {
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

	public List<EmolaFloat> getListEmolaFloat(String fromDate, String toDate) {
		long timeSt = System.currentTimeMillis();
		Connection connection = null;
		ResultSet rs1 = null;
		String sql = " select a.emola_code,a.mvt_trans_code,to_char(a.created_date,'dd-MM-yyyy') created_date,a.total_amount,b.staff_code,b.shop_code_lv3,b.shop_code_lv4,b.shop_code_lv5 from report.sync_float_emola_bccs a, report.bccs_sale_float_rp b\n"
				+ "where a.mvt_trans_code = b.request_id(+) and a.created_date >= to_date(?,'dd-MM-yyyy HH24MISS') and a.created_date < to_date(?,'dd-MM-yyyy HH24MISS')";
		PreparedStatement ps = null;
		List<EmolaFloat> listEmolaFloat = null;
		try {
			listEmolaFloat = new ArrayList<EmolaFloat>();
			connection = ConnectionPoolManager.getConnection("dbCdr");
			ps = connection.prepareStatement(sql);
			if (QUERY_TIMEOUT > 0) {
				ps.setQueryTimeout(QUERY_TIMEOUT);
			}
			ps.setString(1, fromDate + " 000000");
			ps.setString(2, toDate + " 235959");
			rs1 = ps.executeQuery();
			while (rs1.next()) {
				EmolaFloat emola = new EmolaFloat();
				emola.setTransferCode(retNull(rs1.getString("emola_code")));
				emola.setAmount(new BigDecimal(rs1.getString("total_amount") != null ? rs1.getString("total_amount") : "0"));
				emola.setPurpose(0);
				emola.setCreatedDate(retNull(rs1.getString("created_date")));
				emola.setCreatedUser(retNull(rs1.getString("staff_code")));
				emola.setShopCodeLv1(retNull(rs1.getString("shop_code_lv3")));
				emola.setShopCodeLv2(retNull(rs1.getString("shop_code_lv4")));
				emola.setShopCodeLv3(retNull(rs1.getString("shop_code_lv5")));
				emola.setDescription(retNull(rs1.getString("mvt_trans_code")));
				listEmolaFloat.add(emola);
			}
			return listEmolaFloat;
		} catch (Exception ex) {
			logger.error("ERROR getListEmolaFloat");
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
