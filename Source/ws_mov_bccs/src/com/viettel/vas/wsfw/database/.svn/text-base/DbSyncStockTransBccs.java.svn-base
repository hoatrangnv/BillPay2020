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
import com.viettel.vas.wsfw.object.StockModel;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.ResourceBundle;
import org.apache.log4j.Logger;
import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 *
 * Thong tin phien ban
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class DbSyncStockTransBccs extends DbProcessorAbstract {

	private String loggerLabel = DbSyncStockTransBccs.class.getSimpleName() + ": ";
	private PoolStore poolStore;
	private String dbNameCofig;

	public DbSyncStockTransBccs() throws SQLException, Exception {
		this.logger = Logger.getLogger(loggerLabel);
		dbNameCofig = ResourceBundle.getBundle("vas").getString("dbsm");
		poolStore = new PoolStore(dbNameCofig, logger);
	}

	public DbSyncStockTransBccs(String sessionName, Logger logger) throws SQLException, Exception {
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

	public long checkValidPartner(String partnerCode) {
		long timeSt = System.currentTimeMillis();
		Connection connection = null;
		ResultSet rs1 = null;
		String sql = "select partner_id from partner where partner_code =? ";
		PreparedStatement ps = null;
		try {
			connection = ConnectionPoolManager.getConnection("dbsm");
			ps = connection.prepareStatement(sql);
			ps.setString(1, partnerCode.trim());
			if (QUERY_TIMEOUT > 0) {
				ps.setQueryTimeout(QUERY_TIMEOUT);
			}
			rs1 = ps.executeQuery();
			while (rs1.next()) {
				return rs1.getLong("partner_id");
			}
			return 0;
		} catch (Exception ex) {
			logger.error("ERROR checkValidPartner");
			logger.error(AppManager.logException(timeSt, ex));
			return -1;
		} finally {
			closeResultSet(rs1);
			closeStatement(ps);
			closeConnection(connection);
		}
	}

	public StockModel getStockModelInfo(String stockModelCode) {
		long timeSt = System.currentTimeMillis();
		Connection connection = null;
		ResultSet rs1 = null;
		String sql = "select stock_model_id,stock_model_code,stock_type_id,name from stock_model where status =1 and stock_model_code = ?";
		PreparedStatement ps = null;
		StockModel smd = null;
		try {
			connection = ConnectionPoolManager.getConnection("dbsm");
			ps = connection.prepareStatement(sql);
			ps.setString(1, stockModelCode.trim());
			if (QUERY_TIMEOUT > 0) {
				ps.setQueryTimeout(QUERY_TIMEOUT);
			}
			rs1 = ps.executeQuery();
			while (rs1.next()) {
				smd = new StockModel();
				smd.setStockModelId(rs1.getString("stock_model_id"));
				smd.setStockModelCode(rs1.getString("stock_model_code"));
				smd.setName(rs1.getString("name"));
				smd.setStockTypeId(rs1.getString("stock_type_id"));
				return smd;
			}
			return smd;
		} catch (Exception ex) {
			logger.error("ERROR getBccsStockModel");
			logger.error(AppManager.logException(timeSt, ex));
			return smd;
		} finally {
			closeResultSet(rs1);
			closeStatement(ps);
			closeConnection(connection);
		}
	}

	public boolean insertStockTrans(Long stockTransId, Long fromOwnerId, Long reasonId, String contractCode, String batchCode) {
		long timeSt = System.currentTimeMillis();
		Connection connection = null;
		String sql = " insert into sm.stock_trans (stock_trans_id,from_owner_id,from_owner_type,to_owner_id,"
				+ "to_owner_type,create_datetime,stock_trans_type,reason_id,stock_trans_status,syn_status,"
				+ "contract_code,batch_code,operation_type,last_update) values "
				+ "(?,?,?,?,?,"
				+ " sysdate,?,?,?,?,?,"
				+ "?,?,sysdate) ";
		PreparedStatement ps = null;
		try {
			connection = ConnectionPoolManager.getConnection("dbsm");
			ps = connection.prepareStatement(sql);
			ps.setLong(1, stockTransId);//_stockTransId
			ps.setLong(2, fromOwnerId);//_from_owner_id
			ps.setInt(3, 4);//_from_owner_type
			ps.setLong(4, 7282L);//_to_owner_id
			ps.setInt(5, 1);//_to_owner_type
			ps.setInt(6, 2);//_stock_trans_type
			ps.setLong(7, reasonId);//_reason_id
			ps.setInt(8, 2);//_stock_trans_status
			ps.setInt(9, 0);//_syn_status
			ps.setString(10, contractCode);//_contract_code
			ps.setString(11, batchCode);//_batch_code
			ps.setString(12, "INSERT-SYNC");//_operation_type				
			if (QUERY_TIMEOUT > 0) {
				ps.setQueryTimeout(QUERY_TIMEOUT);
			}
			int result = ps.executeUpdate();
			if (result > 0) {
				return true;
			}
			return false;
		} catch (Exception ex) {
			logger.error("ERROR insertStockTrans");
			logger.error(AppManager.logException(timeSt, ex));
			return false;
		} finally {
			closeStatement(ps);
			closeConnection(connection);
		}
	}

	public boolean insertStockTransDetail(Long stockTransId, Long stockModelId, int stateId, int quantityRes, int quantityReal) {
		long timeSt = System.currentTimeMillis();
		Connection connection = null;
		String sql = " insert into sm.stock_trans_detail ("
				+ "stock_trans_detail_id,stock_trans_id,stock_model_id,state_id,"
				+ "quantity_res,quantity_real,create_datetime,operation_type,last_update)"
				+ "values (stock_trans_detail_seq.nextval,?,?,?,?,?,sysdate,?,sysdate) ";
		PreparedStatement ps = null;
		try {
			connection = ConnectionPoolManager.getConnection("dbsm");
			ps = connection.prepareStatement(sql);
			ps.setLong(1, stockTransId);//stockTransId
			ps.setLong(2, stockModelId);//stockModelId
			ps.setInt(3, stateId);//stateId
			ps.setInt(4, quantityRes);//quantity_res
			ps.setInt(5, quantityReal);//quantity_real
			ps.setString(6, "INSERT-SYNC"); //operation_type
			if (QUERY_TIMEOUT > 0) {
				ps.setQueryTimeout(QUERY_TIMEOUT);
			}
			int result = ps.executeUpdate();
			if (result > 0) {
				return true;
			}
			return false;
		} catch (Exception ex) {
			logger.error("ERROR insertStockTransDetail");
			logger.error(AppManager.logException(timeSt, ex));
			return false;
		} finally {
			closeStatement(ps);
			closeConnection(connection);
		}
	}

	public boolean insertStockTransAction(Long stockTransId, String actionCode, int actionType, String note) {
		long timeSt = System.currentTimeMillis();
		Connection connection = null;
		String sql = " insert into stock_trans_action (action_id,stock_trans_id,action_code,action_type,create_datetime,note,"
				+ "username,operation_type,last_update)"
				+ "values ("
				+ "stock_trans_action_seq.nextval,?,?,?,sysdate,?,?,?,sysdate"
				+ ")";
		PreparedStatement ps = null;
		try {
			connection = ConnectionPoolManager.getConnection("dbsm");
			ps = connection.prepareStatement(sql);
			ps.setLong(1, stockTransId);//stockTransId
			ps.setString(2, actionCode);//action_code
			ps.setInt(3, actionType);//action_type
			ps.setString(4, note);//note
			ps.setString(5, "SYNC_ERP");//username
			ps.setString(6, "INSERT_SYNC"); //operation_type
			if (QUERY_TIMEOUT > 0) {
				ps.setQueryTimeout(QUERY_TIMEOUT);
			}
			int result = ps.executeUpdate();
			if (result > 0) {
				return true;
			}
			return false;
		} catch (Exception ex) {
			logger.error("ERROR insertStockTransAction");
			logger.error(AppManager.logException(timeSt, ex));
			return false;
		} finally {
			closeStatement(ps);
			closeConnection(connection);
		}
	}

	public Long getStockTranSequence() {
		long timeSt = System.currentTimeMillis();
		Connection connection = null;
		ResultSet rs1 = null;
		String sql = "select stock_trans_seq.nextval as stock_trans_id from dual";
		PreparedStatement ps = null;
		try {
			connection = ConnectionPoolManager.getConnection("dbsm");
			ps = connection.prepareStatement(sql);
			if (QUERY_TIMEOUT > 0) {
				ps.setQueryTimeout(QUERY_TIMEOUT);
			}
			rs1 = ps.executeQuery();
			while (rs1.next()) {
				return rs1.getLong("stock_trans_id");
			}
			return 0L;
		} catch (Exception ex) {
			logger.error("ERROR getStockTranSequence");
			logger.error(AppManager.logException(timeSt, ex));
			return -1L;
		} finally {
			closeResultSet(rs1);
			closeStatement(ps);
			closeConnection(connection);
		}
	}

	public String getStockTranAction() {
		long timeSt = System.currentTimeMillis();
		Connection connection = null;
		ResultSet rs1 = null;
		String sql = "SELECT TO_CHAR(SYSDATE,'YYYYMMDD_') || ltrim(to_char(mod(TRANS_CODE_SEQ.NEXTVAL,10000),'0000')) AS transCode FROM dual ";
		PreparedStatement ps = null;
		try {
			connection = ConnectionPoolManager.getConnection("dbsm");
			ps = connection.prepareStatement(sql);
			if (QUERY_TIMEOUT > 0) {
				ps.setQueryTimeout(QUERY_TIMEOUT);
			}
			rs1 = ps.executeQuery();
			while (rs1.next()) {
				return rs1.getString("transCode");
			}
			return "";
		} catch (Exception ex) {
			logger.error("ERROR getStockTranAction");
			logger.error(AppManager.logException(timeSt, ex));
			return "";
		} finally {
			closeResultSet(rs1);
			closeStatement(ps);
			closeConnection(connection);
		}
	}

	public boolean cancleStockTrans(Long stockTransId) {
		long timeSt = System.currentTimeMillis();
		Connection connection = null;
		String sql = " update sm.stock_trans set stock_trans_status = 5 where create_datetime > sysdate-5/24/60 and stock_trans_id = ? ";
		PreparedStatement ps = null;
		try {
			connection = ConnectionPoolManager.getConnection("dbsm");
			ps = connection.prepareStatement(sql);
			ps.setLong(1, stockTransId);//stockTransId
			if (QUERY_TIMEOUT > 0) {
				ps.setQueryTimeout(QUERY_TIMEOUT);
			}
			int result = ps.executeUpdate();
			if (result > 0) {
				return true;
			}
			return false;
		} catch (Exception ex) {
			logger.error("ERROR cancleStockTrans");
			logger.error(AppManager.logException(timeSt, ex));
			return false;
		} finally {
			closeStatement(ps);
			closeConnection(connection);
		}
	}

	public boolean insertActionLog(String requestCode, String resultCode, Long stockTransId, String actionType, String description, String actionUser, String ipClient, String impactType, String actionCode) {
		long timeSt = System.currentTimeMillis();
		Connection connection = null;
		String sql = " insert into ws_action_log (action_id,action_type,description,action_user,action_date,action_ip,object_id,impact_type,action_code,request_code,result_code) "
				+ " values(action_log_seq.nextval,?,?,?,sysdate,?,?,?,?,?,?)";
		PreparedStatement ps = null;
		try {
			connection = ConnectionPoolManager.getConnection("dbsm");
			ps = connection.prepareStatement(sql);
			ps.setString(1, actionType);//actionType
			ps.setString(2, description);//description
			ps.setString(3, actionUser);//actionUser
			ps.setString(4, ipClient);//ipClient
			ps.setLong(5, stockTransId);//stockTransId
			ps.setString(6, impactType);//impactType
			ps.setString(7, actionCode);//actionCode
			ps.setString(8, requestCode);//requestCode
			ps.setString(9, resultCode);//resultCode

			if (QUERY_TIMEOUT > 0) {
				ps.setQueryTimeout(QUERY_TIMEOUT);
			}
			if (ps.executeUpdate() > 0) {
				return true;
			}
			return false;
		} catch (Exception ex) {
			logger.error("ERROR insertActionLog");
			logger.error(AppManager.logException(timeSt, ex));
			return false;
		} finally {
			closeStatement(ps);
			closeConnection(connection);
		}
	}

	public boolean insertPartner(String name, String code, int type, String address, String phone, String fax, String contactName) {
		long timeSt = System.currentTimeMillis();
		Connection connection = null;
		String sql = " insert into partner (partner_id,partner_name,partner_type,address,phone,fax,contact_name,sta_date,status,partner_code) "
				+ "values (partner_seq.nextval,?,?,?,?,?,?,sysdate,1,?)";
		PreparedStatement ps = null;
		try {
			connection = ConnectionPoolManager.getConnection("dbsm");
			ps = connection.prepareStatement(sql);
			ps.setString(1, name);//name
			ps.setInt(2, type);//type
			ps.setString(3, address);//address
			ps.setString(4, phone);//phone
			ps.setString(5, fax);//fax
			ps.setString(6, contactName);//contactName
			ps.setString(7, code);//parnerCode
			if (QUERY_TIMEOUT > 0) {
				ps.setQueryTimeout(QUERY_TIMEOUT);
			}
			if (ps.executeUpdate() > 0) {
				return true;
			}
			return false;
		} catch (Exception ex) {
			logger.error("ERROR insertPartner");
			logger.error(AppManager.logException(timeSt, ex));
			return false;
		} finally {
			closeStatement(ps);
			closeConnection(connection);
		}
	}

	public boolean checkExistedRequestCode(String requestCode) {
		long timeSt = System.currentTimeMillis();
		Connection connection = null;
		ResultSet rs1 = null;
		String sql = "select request_code from ws_action_log where request_code =? and result_code = '00' ";
		PreparedStatement ps = null;
		try {
			connection = ConnectionPoolManager.getConnection("dbsm");
			ps = connection.prepareStatement(sql);
			ps.setString(1, requestCode.trim());
			if (QUERY_TIMEOUT > 0) {
				ps.setQueryTimeout(QUERY_TIMEOUT);
			}
			rs1 = ps.executeQuery();
			while (rs1.next()) {
				return true;
			}
			return false;
		} catch (Exception ex) {
			logger.error("ERROR checkExistedRequestCode");
			logger.error(AppManager.logException(timeSt, ex));
			return false;
		} finally {
			closeResultSet(rs1);
			closeStatement(ps);
			closeConnection(connection);
		}
	}

}
