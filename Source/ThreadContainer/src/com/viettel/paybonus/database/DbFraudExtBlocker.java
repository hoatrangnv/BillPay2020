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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.apache.log4j.Logger;
import java.sql.Connection;
import com.viettel.vas.util.PoolStore;
import com.viettel.vas.util.obj.Param;
import com.viettel.vas.util.obj.ParamList;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.ResourceBundle;

/**
 *
 * Thong tin phien ban
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class DbFraudExtBlocker extends DbProcessorAbstract {

//    private Logger logger;
	private String loggerLabel = DbFraudExtBlocker.class.getSimpleName() + ": ";
	private String dbNameCofig;
	private PoolStore poolStore;
	private String sqlDeleteMo = "delete fraud_suboffne_input where fraud_id = ?";
	private String sqlUpdateQueueInput = "update fraud_suboffnet_rollback set process_date = sysdate, result =?,fraud_id =?,status =1 where id = ?";

	public DbFraudExtBlocker() throws SQLException, Exception {
		this.logger = Logger.getLogger(loggerLabel);
		dbNameCofig = "db_payment";
		poolStore = new PoolStore(dbNameCofig, logger);
	}

	public DbFraudExtBlocker(String sessionName, Logger logger) throws SQLException, Exception {
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
		FraudSubInput record = new FraudSubInput();
		long timeSt = System.currentTimeMillis();
		try {
			record.setIsdn(rs.getString("ISDN"));
			record.setFraud_sub_input_id(rs.getLong("FRAUD_ID"));
			record.setActionType(rs.getString("ACT_TYPE"));
			record.setRollBackId(rs.getLong("ROLLBACK_ID"));
		} catch (Exception ex) {
			logger.error("ERROR parse MoRecord");
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
				FraudSubInput sd = (FraudSubInput) rc;
				if ("BLOCK".equals(sd.getActionType())) {
					batchId = sd.getBatchId();
					ps.setLong(1, sd.getFraud_sub_input_id());
					ps.addBatch();
				}
			}
			return ps.executeBatch();
		} catch (Exception ex) {
			logger.error("ERROR deleteQueue rp_laucuoc_call_ext_4 batchid " + batchId, ex);
			logger.error(AppManager.logException(timeStart, ex));
			return null;
		} finally {
			closeStatement(ps);
			closeConnection(connection);
			logTimeDb("Time to deleteQueue fraud_sub_input, batchid " + batchId, timeStart);
		}
	}

	@Override
	public int[] insertQueueHis(List<Record> listRecords) {
		List<ParamList> listParam = new ArrayList();
		String batchId = "";
		long timeSt = System.currentTimeMillis();
		try {
			for (Record rc : listRecords) {
				FraudSubInput sd = (FraudSubInput) rc;
				batchId = sd.getBatchId();
				ParamList paramList = new ParamList();
				paramList.add(new Param("ID", getSequence("fraud_suboffnet_his_seq", dbNameCofig), Param.DataType.LONG, 0));
				paramList.add(new Param("FRAUD_ID", sd.getFraud_sub_input_id(), Param.DataType.LONG, 0));
				paramList.add(new Param("COMMAND", sd.getCommand(), Param.DataType.STRING, 0));
				paramList.add(new Param("ISDN", sd.getIsdn(), Param.DataType.STRING, 0));
				paramList.add(new Param("PROCESS_TIME", "sysdate", Param.DataType.CONST, 0));
				paramList.add(new Param("ERR_CODE", sd.getResultCode(), Param.DataType.STRING, 0));
				paramList.add(new Param("NODE_NAME", sd.getNodeName(), Param.DataType.STRING, 0));
				paramList.add(new Param("CLUSTER_NAME", sd.getClusterName(), Param.DataType.STRING, 0));
				paramList.add(new Param("DESCRIPTION", sd.getDescription(), Param.DataType.STRING, 0));
				paramList.add(new Param("ACTION_TYPE", sd.getActionType(), Param.DataType.STRING, 0));

				listParam.add(paramList);
			}
			int[] res = this.poolStore.insertTable((ParamList[]) listParam.toArray(new ParamList[listParam.size()]), "fraud_suboffnet_his");
			logTimeDb("Time to insertQueueHis fraud_sub_input_unlock_his, batchid " + batchId + " total result: " + res.length, timeSt);

			return res;
		} catch (Exception ex) {
			logger.error("ERROR insertQueueHis fraud_suboffnet_his batchid " + batchId, ex);
			logger.error(AppManager.logException(timeSt, ex));
		}
		return null;
	}

	@Override
	public int[] insertQueueOutput(List<Record> listRecords) {
		long timeStart = System.currentTimeMillis();
		PreparedStatement ps = null;
		Connection connection = null;
		String batchId = "";
		try {
			connection = ConnectionPoolManager.getConnection(dbNameCofig);
			ps = connection.prepareStatement(sqlUpdateQueueInput);
			for (Record rc : listRecords) {
				FraudSubInput sd = (FraudSubInput) rc;
				if ("UNBLOCK".equals(sd.getActionType())) {
					batchId = sd.getBatchId();
					ps.setString(1, sd.getResultCode() + "-" + sd.getDescription());
					ps.setLong(2, sd.getFraud_sub_input_id());
					ps.setLong(3, sd.getRollBackId());
					ps.addBatch();
				}
			}
			return ps.executeBatch();
		} catch (Exception ex) {
			logger.error("ERROR updateQueueInput fraud_suboffnet_rollback batchid " + batchId, ex);
			logger.error(AppManager.logException(timeStart, ex));
			return null;
		} finally {
			closeStatement(ps);
			closeConnection(connection);
			logTimeDb("Time to deleteQueue fraud_sub_input, batchid " + batchId, timeStart);
		}
	}

	@Override
	public int[] updateQueueInput(List<Record> listRecords) {
		int[] res = new int[0];
		return res;
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

	@Override
	public void updateSqlMoParam(List<Record> lrc) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	public int[] deleteQueueTimeout(List<String> listId) {
		int[] res = new int[0];
		return res;
	}

	public long getFraudBlockingInfoByIsdn(String isdn) {
		long timeSt = System.currentTimeMillis();
		Connection connection = null;
		String sqlMo = "select fraud_id from fraud_suboffnet_his where isdn =? and err_code ='0' and action_type ='BLOCK' and unblock_status =0 order by process_time desc ";
		PreparedStatement psMo = null;
		ResultSet rs = null;
		long fraudId = 0;
		try {
			connection = ConnectionPoolManager.getConnection(dbNameCofig);
			psMo = connection.prepareStatement(sqlMo);
			if (QUERY_TIMEOUT > 0) {
				psMo.setQueryTimeout(QUERY_TIMEOUT);
			}
			psMo.setString(1, isdn);
			rs = psMo.executeQuery();
			while (rs.next()) {
				fraudId = rs.getLong("fraud_id");
				break;
			}
			logTimeDb("Time to getFraudIdBlockedByProcess isdn " + isdn + " fraudId " + fraudId, timeSt);
			return fraudId;
		} catch (Exception ex) {
			logger.error("ERROR getFraudIdBlockedByProcess isdn " + isdn);
			logger.error(AppManager.logException(timeSt, ex));
			return -1;
		} finally {
			closeStatement(psMo);
			closeConnection(connection);
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

	public int updateUnblockHis(long fraudId, int status, long id) {
		long timeSt = System.currentTimeMillis();
		Connection connection = null;
		int uptResult = 0;
		String sqlMo = "update fraud_suboffnet_his set unblock_status =? where fraud_id = ? and id= ? ";
		PreparedStatement psMo = null;
		try {
			connection = ConnectionPoolManager.getConnection(dbNameCofig);
			psMo = connection.prepareStatement(sqlMo);
			if (QUERY_TIMEOUT > 0) {
				psMo.setQueryTimeout(QUERY_TIMEOUT);
			}
			psMo.setInt(1, status);
			psMo.setLong(2, fraudId);
			psMo.setLong(3, id);
			uptResult = psMo.executeUpdate();
			logTimeDb("Time to updateUnblockHis fraudId " + fraudId + "uptResult: " + uptResult, timeSt);
			return uptResult;
		} catch (Exception ex) {
			logger.error("ERROR updateUnblockHis fraudId" + fraudId + " result -1");
			logger.error(AppManager.logException(timeSt, ex));
			return -1;
		} finally {
			closeStatement(psMo);
			closeConnection(connection);
		}
	}
}
