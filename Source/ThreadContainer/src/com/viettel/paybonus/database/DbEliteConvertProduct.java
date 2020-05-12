/*
 * Copyright 2012 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.paybonus.database;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.threadfw.manager.AppManager;
import com.viettel.paybonus.obj.*;
import com.viettel.threadfw.database.DbProcessorAbstract;
import com.viettel.vas.util.PoolStore;
import com.viettel.vas.util.obj.Param;
import com.viettel.vas.util.obj.ParamList;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.apache.log4j.Logger;
import java.util.ArrayList;
import java.util.Date;

/**
 *
 * Thong tin phien ban
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class DbEliteConvertProduct extends DbProcessorAbstract {

	private String loggerLabel = DbEliteConvertProduct.class.getSimpleName() + ": ";
	private PoolStore poolStore;
	private String dbNameCofig;

	public DbEliteConvertProduct() throws SQLException, Exception {
		this.logger = Logger.getLogger(loggerLabel);
		dbNameCofig = "dbElite";
		poolStore = new PoolStore(dbNameCofig, logger);
	}

	public DbEliteConvertProduct(String sessionName, Logger logger) throws SQLException, Exception {
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
		EliteConvertProduct record = new EliteConvertProduct();
		long timeSt = System.currentTimeMillis();
		try {
			record.setId(rs.getLong("ID"));
			record.setIsdn(rs.getString("ISDN"));
			record.setProductA(rs.getString("PRODUCT_A"));
			record.setProductA1(rs.getString("PRODUCT_A1"));
			record.setProductB(rs.getString("PRODUCT_B"));
		} catch (Exception ex) {
			logger.error("ERROR parse EliteConverProduct");
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
		List<ParamList> listParam = new ArrayList<ParamList>();
		String batchId = "";
		int[] res = new int[0];
		long timeSt = System.currentTimeMillis();
		try {
			for (Record rc : listRecords) {
				EliteConvertProduct sd = (EliteConvertProduct) rc;
				if(checkAlreadyProcessed(sd.getIsdn())){
					continue;
				}
				batchId = sd.getBatchId();
				ParamList paramList = new ParamList();
				paramList.add(new Param("STATUS", "1", Param.DataType.CONST, Param.IN));
				paramList.add(new Param("PROCESS_DATE", "sysdate", Param.DataType.CONST, Param.IN));
				paramList.add(new Param("RESULT_CODE", sd.getResultCode(), Param.DataType.STRING, Param.IN));
				paramList.add(new Param("DESCRIPTION", sd.getDescription(), Param.DataType.STRING, Param.IN));
				paramList.add(new Param("EFFECTIVE_DATE_A", sd.getEffectiveDateA(), Param.DataType.STRING, Param.IN));
				paramList.add(new Param("EXPIRE_DATE_B", sd.getExpireDateB(), Param.DataType.STRING, Param.IN));
				paramList.add(new Param("PRODUCT_B", sd.getProductB(), Param.DataType.STRING, Param.IN));
				paramList.add(new Param("ID", sd.getID(), Param.DataType.STRING, Param.OUT));

				listParam.add(paramList);
			}
			if (listParam.size() > 0) {
				if (poolStore == null) {
					poolStore = new PoolStore("dbElite", logger);
				}
				res = poolStore.updateTable(listParam.toArray(new ParamList[listParam.size()]), "ELITE_CONVERT_PRODUCT_TEMP");
				logTimeDb("Time to updfate elite_convert_product_temp, batchid " + batchId + " total result: " + res.length, timeSt);
			} else {
				logTimeDb("List Record to update Queue Output is empty, batchid " + batchId, timeSt);
			}
			return res;
		} catch (Exception ex) {
			logger.error("ERROR update elite_convert_product_temp batchid " + batchId, ex);
			try {
				
				res = poolStore.updateTable(listParam.toArray(new ParamList[listParam.size()]), "MT");
				logTimeDb("Time to retry update elite_convert_product_temp, batchid " + batchId + " total result: " + res.length, timeSt);
				return res;
			} catch (Exception ex1) {
				logger.error("ERROR retry update elite_convert_product_temp MT, batchid " + batchId, ex1);
				logger.error(AppManager.logException(timeSt, ex));
				return null;
			}
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

	public boolean checkAlreadyProcessed(String  isdn) {
		ResultSet rs = null;
		Connection connection = null;
		PreparedStatement ps = null;
		StringBuilder br = new StringBuilder();
		try {
			connection = getConnection("dbElite");
			String sql = "select * from ELITE_CONVERT_PRODUCT_TEMP where status = 1 and isdn =?";
			ps = connection.prepareStatement(sql);
			ps.setString(1, isdn);
			rs = ps.executeQuery();
			while (rs.next()) {
				return Boolean.TRUE;
			}
			logger.info("End checkAlreadyProcessed: isdn " + isdn);
			return  Boolean.FALSE;
		} catch (Exception ex) {
			br.setLength(0);
			br.append(loggerLabel).append(new Date()).append("\nERROR isdn ").append(isdn).append(" Message: ").
					append(ex.getMessage());
			logger.error(br + ex.toString());
			return Boolean.FALSE;
		} finally {
			closeResultSet(rs);
			closeStatement(ps);
			closeConnection(connection);
		}
	}

}
