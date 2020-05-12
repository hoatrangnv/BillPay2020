/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.TmsPospaidAssingDb;
import com.viettel.paybonus.obj.TmsCatalog;
import com.viettel.paybonus.obj.TmsPospaidAssign;
import java.util.List;
import org.apache.log4j.Logger;
import com.viettel.threadfw.process.ProcessRecordAbstract;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

/**
 *
 * @author dev_bacnx
 */
public class TmsAssignPospaidProcess extends ProcessRecordAbstract {

	TmsPospaidAssingDb db;

	public TmsAssignPospaidProcess() {
		super();
		logger = Logger.getLogger(TmsAssignPospaidProcess.class);
	}

	@Override
	public void initBeforeStart() throws Exception {
		db = new TmsPospaidAssingDb();
	}

	@Override
	public List<Record> validateContraint(List<Record> listRecord) throws Exception {
		for (Record record : listRecord) {
			TmsPospaidAssign moRecord = (TmsPospaidAssign) record;
			moRecord.setNodeName(holder.getNodeName());
			moRecord.setClusterName(holder.getClusterName());
		}
		return listRecord;
	}

	@Override
	public List<Record> processListRecord(List<Record> listRecord) throws Exception {
		List<Record> listResult = new ArrayList<Record>();
		long timeSt;
		for (Record record : listRecord) {
			timeSt = System.currentTimeMillis();
			TmsPospaidAssign bn = (TmsPospaidAssign) record;
			listResult.add(bn);
			if (bn.getTargetCode() == null || bn.getTargetCode().trim().isEmpty()) {
				logger.info("The record is invaid. Target code is empty");
				bn.setResultCode("01");
				bn.setDescription("Target code is empty");
				bn.setDuration(System.currentTimeMillis() - timeSt);
				continue;
			}

			TmsCatalog clog = db.getCatalogInfo(bn.getTargetCode());
			if (clog == null) {
				logger.info("The record is invaid. Target code is invalid");
				bn.setResultCode("03");
				bn.setDescription("Target code is invalid");
				bn.setDuration(System.currentTimeMillis() - timeSt);
				continue;
			}
//			List<String> listUnitAssign = db.getListUnitForAssign("FBB");
//			if (listUnitAssign == null || listUnitAssign.isEmpty()) {
//				logger.info("The list Unit assign is null , target code " + bn.getTargetCode());
//				bn.setResultCode("04");
//				bn.setDescription("The list Unit assign is null");
//				bn.setDuration(System.currentTimeMillis() - timeSt);
//				continue;
//			}

			//for (String unit : listUnitAssign) {
			//String targetMonth = new SimpleDateFormat("MM-yyyy").format(new Date());
			//bn.setTargetMonth(targetMonth);
			boolean checkValidRecode = db.checkDuplicateProcess(bn.getTargetCode(), bn.getObject(), bn.getTargetMonth());
			if (checkValidRecode) {
				logger.info("The record already processed, tarrget code " + bn.getTargetCode());
				bn.setResultCode("02");
				bn.setDescription("The record already processed");
				bn.setDuration(System.currentTimeMillis() - timeSt);
				continue;
			}

			long assignInfoId = db.getAssingInfo(bn.getTargetMonth(),bn.getObject());
			if (assignInfoId == -1) {
				logger.info("Canot check assign info , target code " + bn.getTargetCode());
				bn.setResultCode("05");
				bn.setDescription("Canot check assign info");
				bn.setDuration(System.currentTimeMillis() - timeSt);
				continue;
			}else if (assignInfoId == -2) {
				logger.info("The monthly target has been closed , target code " + bn.getTargetCode());
				bn.setResultCode("10");
				bn.setDescription("The monthly target has been closed");
				bn.setDuration(System.currentTimeMillis() - timeSt);
				continue;
			} else if (assignInfoId == 0) {
				assignInfoId = db.getSequence("TMS_ASSIGN_INFO_SEQ");
				int rsInsertAssignInfo = db.insertAssingInfo(assignInfoId,bn.getObject(),bn.getTargetMonth());
				if (rsInsertAssignInfo <= 0) {
					logger.info("Fail to crate AssignInfo , target code " + bn.getTargetCode());
					bn.setResultCode("06");
					bn.setDescription("Fail to crate AssignInfo");
					bn.setDuration(System.currentTimeMillis() - timeSt);
					continue;
				}
			} else if (assignInfoId < 0) {
				logger.info("Fail to get assign info," + bn.getTargetCode());
				bn.setResultCode("07");
				bn.setDescription("Fail to get assign info");
				bn.setDuration(System.currentTimeMillis() - timeSt);
				continue;
			}
			//long assignId, String shopCode, String staffCode, String targetCode, Double valueAssign, long parrentId
			double assignValue = db.getAssignValue(bn.getSqlConmand(), bn.getObject(),bn.getTargetMonth());
			if (assignValue < 0) {
				logger.info("Value assign is invalid," + bn.getTargetCode());
				bn.setResultCode("08");
				bn.setDescription("Value assign is invalid");
				bn.setDuration(System.currentTimeMillis() - timeSt);
				continue;
			} else if (assignValue > 0) {
				db.deleteExistedAssignDetail(assignInfoId, bn.getObject(), clog.getTargetCode());
				int rsInsertAssignDetail = db.insertAssingDetail(assignInfoId, bn.getObject(), "", clog.getTargetCode(), assignValue, clog.getParentTargetCode());
				if (rsInsertAssignDetail <= 0) {
					logger.info("Creating assign detail has failed," + bn.getTargetCode());
					bn.setResultCode("09");
					bn.setDescription("Creating assign detail has failed");
					bn.setDuration(System.currentTimeMillis() - timeSt);
					continue;
				} else {
					logger.info("Creating assign has done," + bn.getTargetCode());
					bn.setResultCode("0");
					bn.setDescription("Sucessfully");
					bn.setDuration(System.currentTimeMillis() - timeSt);
					continue;
				}
			} else {
				logger.info("Value assign is 0 dont create asign info," + bn.getTargetCode());
				bn.setResultCode("09");
				bn.setDescription("Value assign is 0 dont create asign info");
				bn.setDuration(System.currentTimeMillis() - timeSt);
				continue;
			}

			//}
		}
		listRecord.clear();
		return listResult;
	}

	@Override
	public void printListRecord(List<Record> listRecord) throws Exception {
		StringBuilder br = new StringBuilder();
		SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
		br.setLength(0);
		br.append("\r\n").
				append("|\tID|").
				append("|\tTARRGET_CODE\t|");
		for (Record record : listRecord) {
			TmsPospaidAssign bn = (TmsPospaidAssign) record;
			br.append("\r\n").
					append("|\t").
					append(bn.getId()).
					append("||\t").
					append(bn.getTargetCode()).
					append("||\t");
		}
		logger.info(br);
	}

	@Override
	public List<Record> processException(List<Record> listRecord, Exception ex) {
		return listRecord;
	}

	@Override
	public boolean startProcessRecord() {
		return true;
	}
}
