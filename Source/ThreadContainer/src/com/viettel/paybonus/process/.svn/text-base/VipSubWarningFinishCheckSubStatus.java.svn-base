/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbVipSubWarning;
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
public class VipSubWarningFinishCheckSubStatus extends ProcessRecordAbstract {

	DbVipSubWarning db;
	String msg;
	String isdn;
	String msgWarn;
	String isdns;

	public VipSubWarningFinishCheckSubStatus() {
		super();
		logger = Logger.getLogger(VipSubWarningFinishCheckSubStatus.class);
		msgWarn = ResourceBundle.getBundle("configPayBonus").getString("vip_sub_message_finish_check_sub");
		isdns = ResourceBundle.getBundle("configPayBonus").getString("vip_sub_isdn_finish_check_sub");
	}

	@Override
	public void initBeforeStart() throws Exception {
		db = new DbVipSubWarning();
	}

	@Override
	public List<Record> validateContraint(List<Record> listRecord) throws Exception {
		return listRecord;
	}

	@Override
	public List<Record> processListRecord(List<Record> listRecord) throws Exception {
		List<Record> listResult = new ArrayList<Record>();
		String mssSendToStaff = "";
		for (Record record : listRecord) {
			VipSubDetail bn = (VipSubDetail) record;
			listResult.add(bn);
			String[] info = db.getResultCheckSubs(bn.getId());
			if (info == null || checkNull(info[0]) || checkNull(info[1])
					|| checkNull(info[2]) || checkNull(info[3]) || checkNull(info[4])) {
				logger.error("Cannot get check sub information vipsub " + bn.getId() + " total_subs" + info[0] + "total_valid" + info[1] + "total_invalid" + info[2]
						+ "cust_name" + info[3] + "create_user" + info[4]);
				continue;
			}
//            Step2 send sms
			mssSendToStaff = msgWarn;
			mssSendToStaff = mssSendToStaff.replace("%CUSNAME%", info[3]);
			mssSendToStaff = mssSendToStaff.replace("%TOTALSUB%", info[0]);
			mssSendToStaff = mssSendToStaff.replace("%TOTALSUCCESS%", info[1]);
			mssSendToStaff = mssSendToStaff.replace("%TOTALFAIL%", info[2]);

			String contactStaff = db.getContactOfStaff(info[4]);
			if (checkNull(contactStaff)) {
				logger.error("Cannot get staff contact..." + bn.getId() + " saffCode " + info[4]);
				continue;
			}
			if (isdns != null && isdns.trim().length() > 0) {
				String[] isdnList = isdns.split("\\|");
				if(isdnList != null && isdnList.length >0){
					for(String n: isdnList){
						db.sendSms(n, mssSendToStaff, "86904");
					}
				}
			}
			db.sendSms(contactStaff, mssSendToStaff, "86904");
			db.updateWarningCheckSubStatus(bn.getId());
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
				append("|\tVIP_SUB_INFO_ID|");
		for (Record record : listRecord) {
			VipSubDetail bn = (VipSubDetail) record;
			br.append("\r\n").
					append("|\t").
					append(bn.getId());
		}
		logger.info(br);
	}

	@Override
	public List<Record> processException(List<Record> listRecord, Exception ex) {
//        logger.warn("TEMPLATE process exception record: " + ex.toString());
//        for (Record record : listRecord) {
//            logger.info("TEMPLATE let convert to recort type you want and then set errCode, errDesc at here");
////            MoRecord moRecord = (MoRecord) record;
////            moRecord.setMessage("Thao tac that bai!");
////            moRecord.setErrCode("-5");
//        }
		return listRecord;
	}

	@Override
	public boolean startProcessRecord() {
		return true;
	}

	public boolean checkNull(String s) {
		if (s == null || s.length() == 0) {
			return true;
		} else {
			return false;
		}
	}

}
