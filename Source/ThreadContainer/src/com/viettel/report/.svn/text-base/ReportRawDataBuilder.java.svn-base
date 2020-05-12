/*
 * Copyright 2012 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.report;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.threadfw.manager.AppManager;
import com.viettel.threadfw.database.DbProcessorAbstract;
import com.viettel.vas.util.ConnectionPoolManager;
import com.viettel.vas.util.PoolStore;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.apache.log4j.Logger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Date;
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * Thong tin phien ban
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class ReportRawDataBuilder extends DbProcessorAbstract {

	private String loggerLabel = ReportRawDataBuilder.class.getSimpleName() + ": ";
	private PoolStore poolStore;
	private String dbNameCofig;

	public ReportRawDataBuilder() throws SQLException, Exception {
		this.logger = Logger.getLogger(loggerLabel);
		dbNameCofig = "dbReport";
		poolStore = new PoolStore(dbNameCofig, logger);
	}

	public ReportRawDataBuilder(String sessionName, Logger logger) throws SQLException, Exception {
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
		ReportInput record = new ReportInput();
		long timeSt = System.currentTimeMillis();
		try {
			record.setId(rs.getLong("id"));
			record.setBranch(rs.getString("branch"));
			record.setReportType(rs.getString("report_type"));
		} catch (Exception ex) {
			logger.error("ERROR parse ReportInput");
			logger.error(AppManager.logException(timeSt, ex));
		}
		return record;
	}

	@Override
	public int[] deleteQueue(List<Record> listRecords) {
		return new int[0];
	}

	@Override
	public int[] insertQueueHis(List<Record> listRecords) {
		long timeStart = System.currentTimeMillis();
		PreparedStatement ps = null;
		Connection connection = null;
		String batchId = "";
		try {
			connection = ConnectionPoolManager.getConnection(dbNameCofig);
			ps = connection.prepareStatement("update report_profile_branch set last_process_time= sysdate where id =?");
			for (Record rc : listRecords) {
				rc = (Record) rc;
				ReportInput fr = (ReportInput) rc;
				batchId = fr.getBatchId();
				ps.setLong(1, fr.getId());
				ps.addBatch();
			}
			return ps.executeBatch();
		} catch (Exception ex) {
			this.logger.error("ERROR deleteQueue report_profile_branch  batchid " + batchId, ex);
			this.logger.error(AppManager.logException(timeStart, ex));
			return null;
		} finally {
			closeStatement(ps);
			closeConnection(connection);
			logTimeDb("Time to deleteQueue ReportProfile announcement, batchid " + batchId, timeStart);
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

	public Map<String, String> getDataAsString(ReportProfile rp) {
		Connection connection = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		long startTime = System.currentTimeMillis();
		String sql = rp.getSql();
		if (sql == null || "".equals(sql) || rp.getBranch() == null || "".equals(rp.getBranch())) {
			return null;
		}
		Map<String, String> map = new HashMap<String, String>();

		try {
			String[] branchs = rp.getBranch().split(",");
			for (String br : branchs) {
				StringBuffer result = new StringBuffer();
				connection = getConnection(dbNameCofig);
				ps = connection.prepareStatement(sql);
				ps.setString(1, br);
				rs = ps.executeQuery();

				while (rs.next()) {
					result.append(rs.getString("MESSAGE"));
					result.append("\n");
					result.append("\n");
				}
				map.put(br, result.toString());
				logger.info("End getDataAsString SQL query:" + rp.getSql() + " time "
						+ (System.currentTimeMillis() - startTime));
			}

		} catch (Exception ex) {
			br.setLength(0);
			logger.error(br + ex.toString());
			return null;
		} finally {
			closeStatement(ps);
			closeConnection(connection);
		}
		return map;
	}

	public ReportProfile writeDataToFile(ReportProfile rp, Map<Long, String> data) {
		if (rp != null && rp.getFilePath() != null) {
			String PATH = rp.getFilePath();
			SimpleDateFormat sdf = new SimpleDateFormat("ddMMyyyy");
			for (Map.Entry<Long, String> rawData : data.entrySet()) {
				String directoryName = PATH + File.separator + sdf.format(new Date()) + File.separator + rp.getReportType() + File.separator + rp.getBranch();
				File directory = new File(directoryName);
				if (!directory.exists()) {
					directory.mkdirs();
				}

				String fileNameOut = rp.getFileName();
				File fileDel = new File(directoryName + File.separator + fileNameOut);
				if (fileDel.exists()) {
					fileDel.delete();
				}
				File file = new File(directoryName + File.separator + fileNameOut);
				FileWriter fr = null;
				BufferedWriter br = null;
				try {
					fr = new FileWriter(file, false);
					br = new BufferedWriter(fr);
					br.write(rawData.getValue());
					insertOutputData(rp.getId(), file.getAbsolutePath(), rp.getBranch(), rp.getReportType());
				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					try {
						br.close();
						fr.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}
		return rp;
	}

	public List<String> getListReceiveEmail(String branch, String reportType) {
		Connection connection = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		long startTime = System.currentTimeMillis();
		String sql = "select email from report_email_received where branch = ? and report_type = ?";
//		String sql = "select email from temp_email where branch =?";
		List<String> listEmail = new ArrayList<String>();
		try {
			connection = getConnection(dbNameCofig);
			ps = connection.prepareStatement(sql);
			ps.setString(1, branch);
			ps.setString(2, reportType);
			rs = ps.executeQuery();

			while (rs.next()) {
				listEmail.add(rs.getString("EMAIL"));
			}
			logger.info("End getDataAsString SQL query:" + " time "
					+ (System.currentTimeMillis() - startTime));
			return listEmail;
		} catch (Exception ex) {
			br.setLength(0);
			logger.error(br + ex.toString());
			return null;
		} finally {
			closeStatement(ps);
			closeConnection(connection);
		}
	}

	public void insertOutputData(long rpId, String outFilePath, String branch, String reportType) {
		Connection connection = null;
		PreparedStatement ps = null;
		StringBuilder br = new StringBuilder();
		String sql = "";
		long startTime = System.currentTimeMillis();
		try {
			connection = getConnection(dbNameCofig);
			sql = "insert into report_profile_out (id,report_id,out_path_file,created_datetime,branch,status,report_type) "
					+ " values(report_out_path_seq.nextval,?,?,sysdate,?,1,?) ";
			ps = connection.prepareStatement(sql);
			ps.setLong(1, rpId);
			ps.setString(2, outFilePath);
			ps.setString(3, branch);
			ps.setString(4, reportType);
			ps.executeUpdate();
			logger.info("End insertOutputData outFilePath " + outFilePath + " branch " + branch + " time "
					+ (System.currentTimeMillis() - startTime));
		} catch (Exception ex) {
			br.setLength(0);
			br.append(loggerLabel).append(new Date()).
					append("\nERROR insertOutputData: ").
					append(sql).append("\n")
					.append(" outFilePath ")
					.append(outFilePath)
					.append(" branch ")
					.append(branch);
			logger.error(br + ex.toString());
		} finally {
			closeStatement(ps);
			closeConnection(connection);
		}
	}

	public void removeHisDataByReportId(long id) {
		Connection connection = null;
		PreparedStatement ps = null;
		StringBuilder br = new StringBuilder();
		String sql = "";
		long startTime = System.currentTimeMillis();
		try {
			connection = getConnection(dbNameCofig);
			sql = " update  report_profile_out set status = 0 where report_id = ? ";
			ps = connection.prepareStatement(sql);
			ps.setLong(1, id);
			ps.executeUpdate();
			logger.info("End removeHisData id " + id + " time "
					+ (System.currentTimeMillis() - startTime));
		} catch (Exception ex) {
			br.setLength(0);
			br.append(loggerLabel).append(new Date()).
					append("\nERROR removeHisData: ").
					append(sql).append("\n")
					.append(" id ")
					.append(id);
			logger.error(br + ex.toString());
		} finally {
			closeStatement(ps);
			closeConnection(connection);
		}
	}

	public void removeHisDataByReportOutputID(long id) {
		Connection connection = null;
		PreparedStatement ps = null;
		StringBuilder br = new StringBuilder();
		String sql = "";
		long startTime = System.currentTimeMillis();
		try {
			connection = getConnection(dbNameCofig);
			sql = " update  report_profile_out set status = 0 where id = ? and status =1 ";
			ps = connection.prepareStatement(sql);
			ps.setLong(1, id);
			ps.executeUpdate();
			logger.info("End removeHisData id " + id + " time "
					+ (System.currentTimeMillis() - startTime));
		} catch (Exception ex) {
			br.setLength(0);
			br.append(loggerLabel).append(new Date()).
					append("\nERROR removeHisData: ").
					append(sql).append("\n")
					.append(" id ")
					.append(id);
			logger.error(br + ex.toString());
		} finally {
			closeStatement(ps);
			closeConnection(connection);
		}
	}

	public Map<Long, String> getOutputFilePathByBranch(String branch, String reportType) {
		Connection connection = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		long startTime = System.currentTimeMillis();
		Map<Long, String> map = new HashMap<Long, String>();
		String sql = " select * from report_profile_out where created_datetime > trunc(sysdate) and status =1 and branch = ? and report_type =?  order by report_id asc";
		try {
			connection = getConnection(dbNameCofig);
			ps = connection.prepareStatement(sql);
			ps.setString(1, branch);
			ps.setString(2, reportType);

			rs = ps.executeQuery();
			while (rs.next()) {
				map.put(rs.getLong("id"), rs.getString("out_path_file"));
			}
			logger.info("End getOutputFilePathByBranch SQL query:" + sql + " time "
					+ (System.currentTimeMillis() - startTime));

		} catch (Exception ex) {
			br.setLength(0);
			br.append(loggerLabel).append(new Date()).
					append("\nERROR getOutputFilePathByBranch: ").
					append(sql).append("\n")
					.append(" branch ")
					.append(branch);
			logger.error(br + ex.toString());
			return null;
		} finally {
			closeStatement(ps);
			closeConnection(connection);
		}
		return map;
	}

	public List<ReportProfile> getListReportProfile(String reportType) {
		Connection connection = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		long startTime = System.currentTimeMillis();
		String sql = "select id,report_name,report_type,file_name,file_path,sql_command from report_profile_management where status =1 and  report_type =? ";
		List<ReportProfile> listReportProfile = new ArrayList<ReportProfile>();
		try {
			connection = getConnection(dbNameCofig);
			ps = connection.prepareStatement(sql);
			ps.setString(1, reportType);
			rs = ps.executeQuery();
			while (rs.next()) {
				ReportProfile rp = new ReportProfile();
				rp.setId(rs.getLong("ID"));
				rp.setReportName(rs.getString("report_name"));
				rp.setReportType(rs.getString("report_type"));
				rp.setFileName(rs.getString("file_name"));
				rp.setFilePath(rs.getString("file_path"));
				rp.setSql(rs.getString("sql_command"));
				listReportProfile.add(rp);
			}
			logger.info("End getListBranch SQL query:" + " time "
					+ (System.currentTimeMillis() - startTime));
			return listReportProfile;
		} catch (Exception ex) {
			br.setLength(0);
			logger.error(br + ex.toString());
			return null;
		} finally {
			closeStatement(ps);
			closeConnection(connection);
		}
	}

	public Map<Long, String> getRawData(String sql, String branch, long reportID) {
		Connection connection = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		long startTime = System.currentTimeMillis();
		Map<Long, String> dataMap = new HashMap<Long, String>();
		try {
			StringBuffer result = new StringBuffer();
			connection = getConnection(dbNameCofig);
			ps = connection.prepareStatement(sql);
			ps.setString(1, branch);
			rs = ps.executeQuery();
			while (rs.next()) {
				result.append(rs.getString("MESSAGE"));
				result.append("\n");
				result.append("\n");
			}
			dataMap.put(reportID, result.toString());
			logger.info("End getDataAsString SQL " + " time "
					+ (System.currentTimeMillis() - startTime));
			return dataMap;
		} catch (Exception ex) {
			br.setLength(0);
			logger.error(br + ex.toString());
			return null;
		} finally {
			closeStatement(ps);
			closeConnection(connection);
		}
	}

	public String getReportTitle(String reportType) {
		Connection connection = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		long startTime = System.currentTimeMillis();
		String sql = "select * from report_type where report_type = ?";
		String reportTitle = "IT report";
		try {
			connection = getConnection(dbNameCofig);
			ps = connection.prepareStatement(sql);
			ps.setString(1, reportType);
			rs = ps.executeQuery();
			while (rs.next()) {
				reportTitle = rs.getString("report_title");
				break;
			}
			logger.info("End getReportTitle SQL query:" + " time "
					+ (System.currentTimeMillis() - startTime));
			return reportTitle;
		} catch (Exception ex) {
			br.setLength(0);
			logger.error(br + ex.toString());
			return reportTitle;
		} finally {
			closeStatement(ps);
			closeConnection(connection);
		}
	}

}
