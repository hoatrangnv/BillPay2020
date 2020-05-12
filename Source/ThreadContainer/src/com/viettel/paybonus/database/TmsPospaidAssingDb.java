/*
 * Copyright 2012 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.paybonus.database;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.obj.TmsCatalog;
import com.viettel.paybonus.obj.TmsPospaidAssign;
import com.viettel.threadfw.manager.AppManager;
import com.viettel.threadfw.database.DbProcessorAbstract;
import static com.viettel.threadfw.database.DbProcessorAbstract.QUERY_TIMEOUT;
import com.viettel.vas.util.PoolStore;
import com.viettel.vas.util.obj.Param;
import com.viettel.vas.util.obj.ParamList;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.List;
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
public class TmsPospaidAssingDb extends DbProcessorAbstract {

	private String loggerLabel = TmsPospaidAssingDb.class.getSimpleName() + ": ";
	private PoolStore poolStore;
	private String dbNameCofig;
	private String sqlDeleteMo = "UPDATE TMS_POSPAID_ASSIGN_AUTO SET SCHEDULE_TIME = ADD_MONTHS(SYSDATE,1) WHERE TARGET_CODE =?";

	public TmsPospaidAssingDb() throws SQLException, Exception {
		this.logger = Logger.getLogger(loggerLabel);
		dbNameCofig = "tms";
		poolStore = new PoolStore(dbNameCofig, logger);
	}

	public TmsPospaidAssingDb(String sessionName, Logger logger) throws SQLException, Exception {
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
		TmsPospaidAssign record = new TmsPospaidAssign();
		long timeSt = System.currentTimeMillis();
		try {
			//id,target_code,sql_command
			record.setId(rs.getLong("id"));
			record.setTargetCode(rs.getString("target_code"));
			record.setSqlConmand(rs.getString("sql_command"));
			record.setObject(rs.getString("unit_code"));
			record.setTargetMonth(rs.getString("target_month"));
		} catch (Exception ex) {
			logger.error("ERROR parse TmsPospaidAssign");
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
		int[] res = new int[0];
		try {
			connection = getConnection(dbNameCofig);
			ps = connection.prepareStatement(sqlDeleteMo);
			for (Record rc : listRecords) {
				TmsPospaidAssign sd = (TmsPospaidAssign) rc;
				batchId = sd.getBatchId();
				ps.setString(1, sd.getTargetCode());
				ps.addBatch();
			}
			res = ps.executeBatch();
			return res;
		} catch (Exception ex) {
			logger.error("ERROR updateQueue TMS_POSPAID_ASSIGN_AUTO batchid " + batchId, ex);
			logger.error(AppManager.logException(timeStart, ex));
			return null;
		} finally {
			closeStatement(ps);
			closeConnection(connection);
			logTimeDb("Time to updateQueue TMS_POSPAID_ASSIGN_AUTO, batchid " + batchId + " total result: " + res.length, timeStart);
		}
	}

	@Override
	public int[] insertQueueHis(List<Record> listRecords) {
		List<ParamList> listParam = new ArrayList<ParamList>();
		String batchId = "";
		long timeSt = System.currentTimeMillis();
		try {
			for (Record rc : listRecords) {
				TmsPospaidAssign sd = (TmsPospaidAssign) rc;
				batchId = sd.getBatchId();
				ParamList paramList = new ParamList();
				paramList.add(new Param("ID", "TMS_PA_HIS_SEQ.nextval", Param.DataType.CONST, Param.IN));
				paramList.add(new Param("TARGET_CODE", sd.getTargetCode(), Param.DataType.STRING, Param.IN));
				paramList.add(new Param("VALUE_ASSIGN", sd.getValueAssign(), Param.DataType.DOUBLE, Param.IN));
				paramList.add(new Param("RESULT_CODE", sd.getResultCode(), Param.DataType.STRING, Param.IN));
				paramList.add(new Param("DESCRIPTION", sd.getDescription(), Param.DataType.STRING, Param.IN));
				paramList.add(new Param("NODE_NAME", sd.getNodeName(), Param.DataType.STRING, Param.IN));
				paramList.add(new Param("CLUSTER_NAME", sd.getClusterName(), Param.DataType.STRING, Param.IN));
				paramList.add(new Param("PROCESS_TIME", "SYSDATE", Param.DataType.CONST, Param.IN));
				paramList.add(new Param("DURATION", sd.getDuration(), Param.DataType.LONG, Param.IN));
				paramList.add(new Param("OBJECT_CODE", sd.getObject(), Param.DataType.STRING, Param.IN));
				paramList.add(new Param("TARGET_MONTH", sd.getTargetMonth(), Param.DataType.STRING, Param.IN));
				//OBJECT,TARGET_MONTH
				listParam.add(paramList);
			}
			int[] res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "TMS_POSPAID_ASSIGN_HIS");
			logTimeDb("Time to insertQueueHis TMS_POSPAID_ASSIGN_HIS, batchid " + batchId + " total result: " + res.length, timeSt);
			return res;
		} catch (Exception ex) {
			logger.error("ERROR insertQueueHis TMS_POSPAID_ASSIGN_HIS batchid " + batchId, ex);
			logger.error(AppManager.logException(timeSt, ex));
			return null;
		}
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

	@Override
	public void updateSqlMoParam(List<Record> lrc) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	public int[] deleteQueueTimeout(List<String> listId) {
		return new int[0];
	}

	public boolean checkDuplicateProcess(String targetCode, String objectCode, String targetMonth) {
		Connection connection = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		StringBuilder br = new StringBuilder();
		boolean result = false;
		long startTime = System.currentTimeMillis();
		try {
			connection = getConnection(dbNameCofig);
			String sql = "select target_code from tms_pospaid_assign_his where  result_code = 0 and target_code = ? and object_code = ? and target_month =? ";
			ps = connection.prepareStatement(sql);
			ps.setString(1, targetCode);
			ps.setString(2, objectCode);
			ps.setString(3, targetMonth);

			rs = ps.executeQuery();
			while (rs.next()) {
				result = true;
				break;
			}
			logger.info("End checkDuplicateProcess targetCode: " + targetCode + " result " + result + " time "
					+ (System.currentTimeMillis() - startTime));
		} catch (Exception ex) {
			ex.printStackTrace();
			br.setLength(0);
			br.append(loggerLabel).append(new Date()).
					append("\nERROR checkDuplicateProcess.");
			logger.error(br + ex.toString());
			logger.error(AppManager.logException(startTime, ex));
		} finally {
			closeStatement(ps);
			closeConnection(connection);
			return result;
		}
	}

	public TmsCatalog getCatalogInfo(String targetCode) {
		Connection connection = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		StringBuilder br = new StringBuilder();
		long startTime = System.currentTimeMillis();
		TmsCatalog clog = null;
		try {
			connection = getConnection(dbNameCofig);
			String sql = "select target_id,target_code,target_group,target_type,unit,parent_target_code from tms_catalog  where create_auto =1 and target_code = ? ";
			ps = connection.prepareStatement(sql);
			ps.setString(1, targetCode);
			rs = ps.executeQuery();
			while (rs.next()) {
				clog = new TmsCatalog();
				clog.setTargetId(rs.getLong("target_id"));
				clog.setTargetCode(rs.getString("target_code"));
				clog.setTargetGroup(rs.getString("target_group"));
				clog.setTargetType(rs.getString("target_type"));
				clog.setUnit(rs.getString("unit"));
				clog.setParentTargetCode(rs.getString("parent_target_code"));
				break;
			}
			logger.info("End getCatalogInfo targetCode: " + targetCode + " time "
					+ (System.currentTimeMillis() - startTime));
		} catch (Exception ex) {
			ex.printStackTrace();
			br.setLength(0);
			br.append(loggerLabel).append(new Date()).
					append("\nERROR getCatalogInfo.");
			logger.error(br + ex.toString());
			logger.error(AppManager.logException(startTime, ex));
		} finally {
			closeStatement(ps);
			closeConnection(connection);
			return clog;
		}
	}

	public List<String> getListUnitForAssign(String targetCode) {
		Connection connection = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		StringBuilder br = new StringBuilder();
		long startTime = System.currentTimeMillis();
		List<String> listUnit = new ArrayList<String>();;
		try {
			connection = getConnection(dbNameCofig);
			String sql = "select * from tms_pospaid_assign_unit where service_type = ? and status =1";
			ps = connection.prepareStatement(sql);
			ps.setString(1, targetCode);
			rs = ps.executeQuery();
			while (rs.next()) {
				String unitCode = rs.getString("UNIT_CODE");
				if (unitCode != null && !unitCode.isEmpty()) {
					listUnit.add(unitCode);
				}
			}
			logger.info("End getListUnitForAssign targetCode: " + targetCode + " result listUnit:" + listUnit != null ? listUnit.size() : "0" + " time "
					+ (System.currentTimeMillis() - startTime));
		} catch (Exception ex) {
			ex.printStackTrace();
			br.setLength(0);
			br.append(loggerLabel).append(new Date()).
					append("\nERROR getListUnitForAssign.");
			logger.error(br + ex.toString());
			logger.error(AppManager.logException(startTime, ex));
		} finally {
			closeStatement(ps);
			closeConnection(connection);
			return listUnit;
		}
	}

	public long getAssingInfo(String targetMonth, String unitCode) {
		Connection connection = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		StringBuilder br = new StringBuilder();
		long startTime = System.currentTimeMillis();
		long assignInfoId = 0;
		int status = 1;
		try {
			connection = getConnection(dbNameCofig);
			String sql = "select assign_info_id,status from tms_assign_info where target_month = ? and shop_code =?";
			ps = connection.prepareStatement(sql);
			ps.setString(1, targetMonth);
			ps.setString(2, unitCode);
			rs = ps.executeQuery();
			while (rs.next()) {
				assignInfoId = rs.getLong("assign_info_id");
				status = rs.getInt("status");
				break;
			}
			if(status == 2){
				assignInfoId = -2;
			}
			logger.info("End getAssingInfo targetMonth: " + targetMonth + " result " + assignInfoId + " time "
					+ (System.currentTimeMillis() - startTime));
		} catch (Exception ex) {
			ex.printStackTrace();
			br.setLength(0);
			br.append(loggerLabel).append(new Date()).
					append("\nERROR getAssingInfo.");
			logger.error(br + ex.toString());
			logger.error(AppManager.logException(startTime, ex));
			assignInfoId = -1;
		} finally {
			closeStatement(ps);
			closeConnection(connection);
			return assignInfoId;
		}
	}

	public int insertAssingInfo(long assignId,String unit,String targetMonth) {
		Connection connection = null;
		PreparedStatement ps = null;
		StringBuilder br = new StringBuilder();
		long startTime = System.currentTimeMillis();
		int result = 0;
		try {
			connection = getConnection(dbNameCofig);
			String sql = "insert into tms_assign_info (assign_info_id,user_assign,time_assign,type_assign,file_path,start_period,end_period,status,last_update_time,last_update_user,target_month,shop_code )\n"
					+ "values(?,'SYSTEM',sysdate,1,null,trunc(to_date(?,'dd-MM-yyyy'),'mm'),LAST_DAY(to_date(?,'dd-MM-yyyy')),1,sysdate,'SYSTEM',?,?)";
			ps = connection.prepareStatement(sql);
			ps.setLong(1, assignId);
			ps.setString(2, "01-"+targetMonth);
			ps.setString(3, "01-"+targetMonth);
			ps.setString(4, targetMonth);
			ps.setString(5, unit);
			result = ps.executeUpdate();
			logger.info("End insertAssingInfo assignId: " + assignId + " result " + result + " time "
					+ (System.currentTimeMillis() - startTime));
		} catch (Exception ex) {
			ex.printStackTrace();
			br.setLength(0);
			br.append(loggerLabel).append(new Date()).
					append("\nERROR insertAssingInfo.");
			logger.error(br + ex.toString());
			logger.error(AppManager.logException(startTime, ex));
			result = -1;
		} finally {
			closeStatement(ps);
			closeConnection(connection);
			return result;
		}
	}

	public int insertAssingDetail(long assignId, String shopCode, String staffCode, String targetCode, Double valueAssign, String parrentTargetCode) {
		Connection connection = null;
		PreparedStatement ps = null;
		StringBuilder br = new StringBuilder();
		long startTime = System.currentTimeMillis();
		int result = 0;
		try {
			connection = getConnection(dbNameCofig);
			String sql = "insert into tms_assign_detail (assign_detail_id,assign_info_id,shop_code,staff_code,target_code,value_assign,time_assign,status,parent_target_code)\n"
					+ "values(tms_assign_detail_seq.nextval,?,?,?,?,?,sysdate,1,?)";
			ps = connection.prepareStatement(sql);
			ps.setLong(1, assignId);
			ps.setString(2, shopCode);
			ps.setString(3, staffCode);
			ps.setString(4, targetCode);
			ps.setDouble(5, valueAssign);
			ps.setString(6, parrentTargetCode);
			result = ps.executeUpdate();
			logger.info("End insertAssingDetail assignId: " + assignId + " result " + result + " time "
					+ (System.currentTimeMillis() - startTime));
		} catch (Exception ex) {
			ex.printStackTrace();
			br.setLength(0);
			br.append(loggerLabel).append(new Date()).
					append("\nERROR insertAssingDetail.");
			logger.error(br + ex.toString());
			logger.error(AppManager.logException(startTime, ex));
			result = -1;
		} finally {
			closeStatement(ps);
			closeConnection(connection);
			return result;
		}
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

	public double getAssignValue(String sqlQuery, String unit,String targetMonth) {
		Connection connection = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		StringBuilder br = new StringBuilder();
		long startTime = System.currentTimeMillis();
		double valueAssign = -1;
		try {
			connection = getConnection(dbNameCofig);
			ps = connection.prepareStatement(sqlQuery);
			ps.setString(1,"01-"+targetMonth);
			ps.setString(2, unit);
			rs = ps.executeQuery();
			while (rs.next()) {
				valueAssign = rs.getDouble("charge_assign");
				break;
			}
			logger.info("End getAssignValue sqlQuery: " + sqlQuery + " unit " + unit + " valueAssign " + valueAssign + " time "
					+ (System.currentTimeMillis() - startTime));
		} catch (Exception ex) {
			ex.printStackTrace();
			br.setLength(0);
			br.append(loggerLabel).append(new Date()).
					append("\nERROR getAssignValue.");
			logger.error(br + ex.toString());
			logger.error(AppManager.logException(startTime, ex));
			valueAssign = -1;
		} finally {
			closeStatement(ps);
			closeConnection(connection);
			return valueAssign;
		}
	}

	public Long getSequence(String sequenceName) {
		long timeSt = System.currentTimeMillis();
		ResultSet rs1 = null;
		Connection connection = null;
		Long sequenceValue = null;
		String sqlMo = "select " + sequenceName + ".nextval as sequence from dual";
		PreparedStatement psMo = null;
		try {
			connection = getConnection(dbNameCofig);
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

	public int deleteExistedAssignDetail(long assignId, String object, String targetCode) {
		Connection connection = null;
		PreparedStatement ps = null;
		StringBuilder br = new StringBuilder();
		long startTime = System.currentTimeMillis();
		int result = 0;
		try {
			connection = getConnection(dbNameCofig);
			String sql = "update  tms_assign_detail set status =0 where assign_info_id=? and shop_code=? and target_code=? and status =1";
			ps = connection.prepareStatement(sql);
			ps.setLong(1, assignId);
			ps.setString(2, object);
			ps.setString(3, targetCode);
			result = ps.executeUpdate();
			logger.info("End deleteExistedAssignDetail assignId: " + assignId + " result " + result + " time "
					+ (System.currentTimeMillis() - startTime));
		} catch (Exception ex) {
			ex.printStackTrace();
			br.setLength(0);
			br.append(loggerLabel).append(new Date()).
					append("\nERROR deleteExistedAssignDetail.");
			logger.error(br + ex.toString());
			logger.error(AppManager.logException(startTime, ex));
			result = -1;
		} finally {
			closeStatement(ps);
			closeConnection(connection);
			return result;
		}
	}

}
