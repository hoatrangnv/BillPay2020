/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbVipSubCheckValidSub;
import com.viettel.paybonus.obj.VipSubDetail;
import java.util.List;
import org.apache.log4j.Logger;
import com.viettel.threadfw.process.ProcessRecordAbstract;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.ResourceBundle;

/**
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class VipSubCheckValidSub extends ProcessRecordAbstract {

	DbVipSubCheckValidSub db;
	String msg;
	String isdn;

	public VipSubCheckValidSub() {
		super();
		logger = Logger.getLogger(VipSubCheckValidSub.class);
	}

	@Override
	public void initBeforeStart() throws Exception {
		db = new DbVipSubCheckValidSub();
	}

	@Override
	public List<Record> validateContraint(List<Record> listRecord) throws Exception {
		return listRecord;
	}

	@Override
	public List<Record> processListRecord(List<Record> listRecord) throws Exception {
		List<Record> listResult = new ArrayList<Record>();
		String custName;
		String msgWarn;
		for (Record record : listRecord) {
			VipSubDetail bn = (VipSubDetail) record;
			listResult.add(bn);
			if (bn.getIsdn() == null || bn.getIsdn().trim().length() == 0) {
				logger.warn("Subcriber invalid - Cannot get ISDN subscriber vip_sub_id " + bn.getVipSubInfoId() + " vip_sub_detail_id " + bn.getVipSubDetailId());
				bn.setSubStatus(8);
				continue;
			}
			String isdn = bn.getIsdn().trim();
			//check normal sub
			boolean checkNormalSub = db.checkNormalSub(isdn);
			if (!checkNormalSub) {
				logger.warn("Subcriber invalid - Subscriber not active normal " + isdn + " vip_sub_id " + bn.getVipSubInfoId() + " vip_sub_detail_id " + bn.getVipSubDetailId());
				bn.setSubStatus(1);
				continue;
			}
			boolean checkNewProfileNew = db.checkCorrectProfile(isdn);
			if (!checkNewProfileNew) {
				boolean checkNewOldProfile = db.checkCorrectOldProfile(isdn);
				if (!checkNewOldProfile) {
					logger.warn("Subcriber invalid - Profile incorrect isdn " + isdn + " vip_sub_id " + bn.getVipSubInfoId() + " vip_sub_detail_id " + bn.getVipSubDetailId());
					bn.setSubStatus(2);
					continue;
				}
			}
			logger.warn("Subcriber valid " + isdn + " vip_sub_id " + bn.getVipSubInfoId() + " vip_sub_detail_id " + bn.getVipSubDetailId());
			bn.setSubStatus(0);
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
				append("|\tVIP_SUB_INFO_ID|").
				append("|\tVIP_SUB_DETAIL_ID|").
				append("|\tISDN|");
		for (Record record : listRecord) {
			VipSubDetail bn = (VipSubDetail) record;
			br.append("\r\n").
					append("|\t").
					append(bn.getId()).
					append(bn.getVipSubDetailId()).
					append(bn.getIsdn());
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
