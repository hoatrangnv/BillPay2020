/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbFraudExtBlocker;
import com.viettel.paybonus.obj.FraudSubInput;
import com.viettel.paybonus.service.Exchange;
import com.viettel.threadfw.manager.AppManager;
import java.util.List;
import org.apache.log4j.Logger;
import com.viettel.threadfw.process.ProcessRecordAbstract;
import com.viettel.vas.util.ExchangeClientChannel;
import java.util.ArrayList;

/**
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class FraudExtBlocker extends ProcessRecordAbstract {

	Exchange pro;
	DbFraudExtBlocker db;
	String[] mscs;

	public FraudExtBlocker() {
		super();
		logger = Logger.getLogger(FraudExtBlocker.class);
	}

	@Override
	public void initBeforeStart() throws Exception {
		pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
		db = new DbFraudExtBlocker();
		mscs = new String[]{"gmsc_hw_1", "gmsc_hw_2"};
	}

	@Override
	public List<Record> validateContraint(List<Record> listRecord) throws Exception {
		return listRecord;
	}

	@Override
	public List<Record> processListRecord(List<Record> listRecord) throws Exception {
		List<Record> listResult = new ArrayList<Record>();
		String isdn = "";
		String extCode = "";
		for (Record record : listRecord) {
			FraudSubInput bn = (FraudSubInput) record;
			listResult.add(bn);
			String description = "";
			String errorCode = "";
			if (bn.getIsdn() == null || bn.getIsdn().trim().length() == 0) {
				logger.info("Input invalid, isdn " + bn.getIsdn());
				bn.setResultCode("01");
				bn.setDescription("Input invalid, the isdn is null or empty");
				continue;
			}
			isdn = bn.getIsdn().trim().startsWith("258") ? bn.getIsdn().trim().substring(3) : bn.getIsdn().trim();
			if ("84".equals(isdn.substring(0, 2)) || "85".equals(isdn.substring(0, 2))) {
				extCode = "VOD";//Vodacom
			} else if ("82".equals(isdn.substring(0, 2)) || "83".equals(isdn.substring(0, 2))) {
				extCode = "MCE";//Mcel
			}
			if (extCode.length() == 0 || isdn.length() != 9) {
				logger.info("Isdn does not belong external network " + bn.getIsdn());
				bn.setResultCode("02");
				bn.setDescription("Isdn does not belong external network");
				continue;
			}
			int blockResult = 0;
			if ("BLOCK".equals(bn.getActionType())) {
				//<editor-fold defaultstate="collapsed" desc="BLOCK SUB">
				bn.setCommand("GMSC_HW_ADD_CLRDSG");
				for (String msc : mscs) {
					String blockStatus = pro.blockExternalSub(isdn, extCode, msc);
					if ((!"0".equals(blockStatus)) && (!"414024".equals(blockStatus)) && (!"245952".equals(blockStatus))) {
						logger.info("|Block subscriber has failed, isdn " + bn.getIsdn() + " on GMSC " + msc);
						errorCode += "|E03";
						description += "|Block subscriber has failed on MSC " + msc + " status " + blockStatus;
					} else {
						logger.info("Block suscriber done successfully, isdn " + bn.getIsdn() + " on GMSC " + msc);
						errorCode += "|0";
						description += "|Block suscriber done successfully on MSC " + msc;
						blockResult++;
					}
				}
				//Check sub status on GMSC
				if (blockResult == 2) {
					bn.setResultCode("0");
				} else {
					bn.setResultCode(errorCode);
				}

				bn.setDescription(description);
				//</editor-fold> 
			} else if ("UNBLOCK".equals(bn.getActionType()) || "UNBLOCK_CYCLLE".equals(bn.getActionType())) {
				//<editor-fold defaultstate="collapsed" desc="OPEN SUB">
				description = "";
				long frauId = 0;
				if ("UNBLOCK".equals(bn.getActionType())) {//rollback manuel
					frauId = db.getFraudBlockingInfoByIsdn(isdn);
					bn.setFraud_sub_input_id(frauId);
				}
				if (bn.getFraud_sub_input_id() <= 0) {
					logger.info("Cannot find blocked information, isdn " + bn.getIsdn());
					bn.setResultCode("04");
					bn.setDescription("Cannot find blocked information");
				} else {
					//Save fraud id in the his table
					bn.setCommand("GMSC_HW_RMV_CLRDSG");
					int unBlockResult = 0;
					for (String msc : mscs) {
						String unblockStatus = pro.unblockExternalSub(isdn, extCode, msc);
						if ((!"0".equals(unblockStatus)) && !"245954".equals(unblockStatus)) {
							logger.info("|Unblock subscriber has failed, isdn " + bn.getIsdn() + " on GMSC " + msc);
							errorCode += "|E04";
							description += "|Unblock subscriber has failed on MSC " + msc + " status " + unblockStatus;
						} else {
							logger.info("Unblock suscriber done successfully, isdn " + bn.getIsdn() + " on GMSC " + msc);
							errorCode += "|0";
							description += "|Unblock suscriber done successfully on MSC " + msc;
							unBlockResult++;
						}
					}
					//Check sub status on GMSC
					if (unBlockResult == 2) {
						bn.setResultCode("0");
						//update result for current record
						if ("UNBLOCK_CYCLLE".equals(bn.getActionType())) {
							int updateRes = db.updateUnblockHis(bn.getFraud_sub_input_id(), 1, bn.getRollBackId());//processed done
							logger.info("Update result process " + bn.getIsdn() + " fraud id " + bn.getFraud_sub_input_id() + " result " + updateRes);
						}
					} else {
						bn.setResultCode(errorCode);
						//update result for current record
						if ("UNBLOCK_CYCLLE".equals(bn.getActionType())) {
							int updateRes = db.updateUnblockHis(bn.getFraud_sub_input_id(), 2, bn.getRollBackId());//processed but failed
							logger.info("Update result process " + bn.getIsdn() + " fraud id " + bn.getFraud_sub_input_id() + " result " + updateRes);
						}
					}
					bn.setDescription(description);
				}
				//</editor-fold>
			} else {
				logger.info("Cannot get action type of this subsctiber " + bn.getIsdn());
				bn.setResultCode("99");
				bn.setDescription("Cannot get action type of this subsctiber");
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
				append("|\tFRAUD_SUB_INPUT_ID|").
				append("|\tISDN\t|").
				append("|\tACTION_TYPE\t|");
		for (Record record : listRecord) {
			FraudSubInput bn = (FraudSubInput) record;
			br.append("\r\n").
					append("|\t").
					append(bn.getFraud_sub_input_id()).
					append("||\t").
					append(bn.getIsdn()).
					append("||\t").
					append(bn.getActionType());
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
