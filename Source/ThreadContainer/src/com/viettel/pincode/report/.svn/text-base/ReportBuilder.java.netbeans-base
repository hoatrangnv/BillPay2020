/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.report;

import com.viettel.paybonus.process.*;
import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.service.EmailUtils;
import java.util.List;
import org.apache.log4j.Logger;
import com.viettel.threadfw.process.ProcessRecordAbstract;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @author LinhNBV
 */
public class ReportBuilder extends ProcessRecordAbstract {

	ReportRawDataBuilder db;
	SimpleDateFormat sdf;

	public ReportBuilder() {
		super();
		logger = Logger.getLogger(LimitControl.class);
	}

	@Override
	public void initBeforeStart() throws Exception {
		db = new ReportRawDataBuilder();
	}

	@Override
	public List<Record> validateContraint(List<Record> listRecord) throws Exception {
		return listRecord;
	}

	@Override
	public List<Record> processListRecord(List<Record> listRecord) throws Exception {
		List<Record> listResult = new ArrayList<Record>();
		for (Record record : listRecord) {
			ReportInput bn = (ReportInput) record;
			listResult.add(bn);

			if (bn.getReportType() == null || "".equals(bn.getReportType())) {
				logger.info("Report type is null......");
				continue;
			}
			List<ReportProfile> listReportProfile = db.getListReportProfile(bn.getReportType());
			if (listReportProfile == null || listReportProfile.isEmpty()) {
				logger.info("Can not find any data......");
				continue;
			}
			//Create report file
			for (ReportProfile rp : listReportProfile) {
				rp.setBranch(bn.getBranch());
				Map<Long, String> dataMap = db.getRawData(rp.getSql(), bn.getBranch(), rp.getId());
				db.writeDataToFile(rp, dataMap);
			}
			//Send email
			List<String> listFilePath = new ArrayList<String>();
			List<Long> listIdFilePath = new ArrayList<Long>();
			List<String> emailReceive = db.getListReceiveEmail(bn.getBranch(), bn.getReportType());
			Map<Long, String> fileMap = db.getOutputFilePathByBranch(bn.getBranch(), bn.getReportType());

			for (Map.Entry<Long, String> entry : fileMap.entrySet()) {
				listFilePath.add(entry.getValue());
				listIdFilePath.add(entry.getKey());
			}

			String reportTite = db.getReportTitle(bn.getReportType());
			if (listFilePath != null && !listFilePath.isEmpty() && emailReceive != null && !emailReceive.isEmpty()) {
				Calendar cal = Calendar.getInstance();
				cal.setTime(new Date());
				cal.add(Calendar.DATE, -1);
				HashMap<String, String> param = new HashMap();
				param.put("EMAIL_SSL", "YES");
				param.put("EMAIL_KEYSTORE_PASSWORD", "45037153e4312bba");
				param.put("EMAIL_HOST", "125.235.240.36");
				param.put("EMAIL_PORT", "465");
				param.put("EMAIL_ADDRESS", "it_report@movitel.co.mz");
				param.put("EMAIL_PASSWORD", "Movitel@2018");
				param.put("EMAIL_KEYSTORE_FILE", "");
				param.put("EMAIL_ATTACHMENT_FILE", "");
				param.put("EMAIL_SUBJECT", bn.getBranch() + "_" + reportTite + new SimpleDateFormat("ddMMyyyy").format(cal.getTime()));
				param.put("EMAIL_CONTENT", "Hi IT Dept. is sending Emola report. This email is sent automatically!");
				for (String em : emailReceive) {
					param.put("SEND_EMAIL", em);
					logger.info("Sending email to......" + em);
					String resultSendEmail = EmailUtils.sendInformEmailWithMultipleAttached(param, listFilePath);
					logger.info("End email to " + em + " result " + resultSendEmail);
				}
			}
			for (Long id : listIdFilePath) {
				db.removeHisDataByReportOutputID(id);
			}
		}
		listRecord.clear();
		return listResult;
	}

	@Override
	public void printListRecord(List<Record> listRecord) throws Exception {
		StringBuilder br = new StringBuilder();
		br.setLength(0);
		br.append("\r\n").
				append("|\tBRANCH\t|");
		for (Record record : listRecord) {
			ReportInput bn = (ReportInput) record;
			br.append("\r\n").
					append("|\t").
					append(bn.getBranch());
		}
		logger.info(br);
	}

	@Override
	public List<Record> processException(List<Record> listRecord, Exception ex
	) {
		ex.printStackTrace();
		logger.warn("TEMPLATE process exception record: " + ex.toString());
		return listRecord;
	}

	@Override
	public boolean startProcessRecord() {
		return true;
	}
}
