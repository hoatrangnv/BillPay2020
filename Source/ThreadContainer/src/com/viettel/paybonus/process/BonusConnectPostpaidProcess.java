/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbBonusConnectPostpaid;
import com.viettel.paybonus.obj.Agent;
import com.viettel.paybonus.obj.BonusConnectPostPaid;
import com.viettel.paybonus.obj.SpecialProductMB;
import com.viettel.paybonus.service.Exchange;
import com.viettel.threadfw.manager.AppManager;
import java.util.List;
import org.apache.log4j.Logger;
import com.viettel.threadfw.process.ProcessRecordAbstract;
import com.viettel.vas.util.ExchangeClientChannel;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.ResourceBundle;

/**
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class BonusConnectPostpaidProcess extends ProcessRecordAbstract {

	Exchange pro;
	DbBonusConnectPostpaid db;
	String bonusConnPospaidSmsSuccess;

	public BonusConnectPostpaidProcess() {
		super();
		logger = Logger.getLogger(BonusConnectPostpaidProcess.class);
	}

	@Override
	public void initBeforeStart() throws Exception {
		pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
		db = new DbBonusConnectPostpaid();
		bonusConnPospaidSmsSuccess = ResourceBundle.getBundle("configPayBonus").getString("bonusConnPospaidSmsSuccess");
	}

	@Override
	public List<Record> validateContraint(List<Record> listRecord) throws Exception {
		return listRecord;
	}

	@Override
	public List<Record> processListRecord(List<Record> listRecord) throws Exception {
		List<Record> listResult = new ArrayList<Record>();
		long timeSt;
		String actionCodeBonusEmola = "615";
		SimpleDateFormat sdf = new SimpleDateFormat("ddMMyyyyHHmmssSSS");
		long valuesBonusForStaff = 0;
		for (Record record : listRecord) {
			timeSt = System.currentTimeMillis();
			BonusConnectPostPaid bn = (BonusConnectPostPaid) record;
			listResult.add(bn);
			/*Validate input*/
			if (isNullOrEmpty(bn.getIsdn())
					|| isNullOrEmpty(bn.getStaffCode())
					|| isNullOrEmpty(bn.getShopCode())
					|| isNullOrEmpty(bn.getProductCode())
					|| isNullOrEmpty(bn.getImsi())
					|| isNullOrEmpty(bn.getIsdn())
					|| bn.getActionAuditId() == null
					|| bn.getActionAuditId() < 0) {
				logger.info("Input invalid " + retNull(bn.getIsdn()) + " imsi " + retNull(bn.getImsi()) + " staff" + retNull(bn.getStaffCode()) + "-" + retNull(bn.getShopCode()));
				bn.setResultCode("E01");
				bn.setDescription("Input invalid");
				continue;
			}
			/*Check status of isnd on OCS*/
			boolean checkExitsOnOcs = pro.checkIsdnExistOnOCS(bn.getIsdn(), bn.getImsi().trim());
			if (!checkExitsOnOcs) {
				logger.info("Check info in OCS not match isdn and imsi " + bn.getIsdn() + " imsi " + bn.getImsi() + " staff" + bn.getStaffCode() + "-" + bn.getShopCode());
				bn.setResultCode("E02");
				bn.setDescription("This numnber doesn't exist on OCS or the current IMSI not match on OCS");
				continue;
			}
			/*Check process status*/
			boolean isProcessed = db.checkAlreadyProcessed(bn.getIsdn());
			if (isProcessed) {
				logger.info("This isdn is processed " + bn.getIsdn() + " imsi " + bn.getImsi() + " staff" + bn.getStaffCode() + "-" + bn.getShopCode());
				bn.setResultCode("E03");
				bn.setDescription("This numnber already processed");
				continue;
			}
			/*Get commitsion from DB*/
			SpecialProductMB bonusConfig = db.getSpecialProductMBByProduct(bn.getProductCode());
			if (bonusConfig == null
					|| bonusConfig.getMonthlyFee() <= 0
					|| bonusConfig.getAgentMoney() <= 0
					|| bonusConfig.getBonusOther() <= 0) {
				logger.info("The configuration invalid " + bn.getIsdn() + " imsi " + bn.getImsi() + " staff" + bn.getStaffCode() + "-" + bn.getShopCode() + " Config " + bonusConfig.toString());
				bn.setResultCode("E04");
				bn.setDescription("The configuration invalid");
				continue;
			}
			/*Calculate the commmitsion value*/
			if ("CORPORATE".equals(bn.getShopCode().toUpperCase())) {
				BigDecimal number = new BigDecimal((bonusConfig.getMonthlyFee() / 1.17) * (bonusConfig.getBonusAgent() / 100));
				valuesBonusForStaff = number.longValue();
				logger.info("Get configuration bonus for staff belong corporate " + bn.getIsdn() + " imsi " + bn.getImsi() + " staff" + bn.getStaffCode() + "-" + bn.getShopCode());
			} else {
				BigDecimal number = new BigDecimal((bonusConfig.getMonthlyFee() / 1.17) * (bonusConfig.getBonusOther() / 100));
				valuesBonusForStaff = number.longValue();
				logger.info("Get configuration bonus for staff NOT belong corporate " + bn.getIsdn() + " imsi " + bn.getImsi() + " staff" + bn.getStaffCode() + "-" + bn.getShopCode());
			}
			/*Bonus for staff*/
			if (valuesBonusForStaff > 0) {
				Agent staffInfo = db.getAgentInfoByUser(bn.getStaffCode());
				if (staffInfo.getIsdnWallet() != null && staffInfo.getIsdnWallet().length() > 0 && valuesBonusForStaff > 0) {
					String eWalletResponse = pro.callEwallet(bn.getActionAuditId(), staffInfo.getChannelTypeId(), staffInfo.getIsdnWallet(), valuesBonusForStaff, actionCodeBonusEmola, bn.getStaffCode(), sdf.format(new Date()), db);
					if ("01".equals(eWalletResponse)) {
						logger.info("Pay Bonus for staff success for actionId " + bn.getActionAuditId() + " isdnEmola "
								+ staffInfo.getIsdnWallet() + " amount " + valuesBonusForStaff + bn.getIsdn() + " imsi " + bn.getImsi() + " staff" + bn.getStaffCode() + "-" + bn.getShopCode());
						bn.setResultCode("0");
						bn.setDescription("Bonus successfull " + valuesBonusForStaff);
						bn.setBonusValues(valuesBonusForStaff);
						String mssTmp = bonusConnPospaidSmsSuccess.replaceAll("FAST_PRODUCT", bn.getProductCode()).replaceAll("BONUS_VALUE", valuesBonusForStaff+"");
						db.sendSms(staffInfo.getIsdnWallet(), mssTmp, "86904");
					} else {
						logger.error("Pay Bonus fail for staff actionId " + bn.getActionAuditId()
								+ " isdnEmola " + staffInfo.getIsdnWallet() + " amount " + valuesBonusForStaff + bn.getIsdn() + " imsi " + bn.getImsi() + " staff" + bn.getStaffCode() + "-" + bn.getShopCode());
						bn.setResultCode("E05");
						bn.setDescription("Bonus failed result" + eWalletResponse);
					}
				} else {//Bonus failed. Don't have isdn wallet, cannot pay comission
					logger.warn(bn.getStaffCode() + "Bonus failed. Don't have isdn wallet, cannot pay comission " + bn.getActionAuditId() + ", duration: " + (System.currentTimeMillis() - timeSt));
					bn.setResultCode("E06");
					bn.setDescription("Cannot get agent information");
				}

			} else {//Bonus failed. Cannot get bonus value
				bn.setResultCode("E07");
				bn.setDescription("The bonus values invalid " + valuesBonusForStaff);
				logger.warn("Staff " + bn.getStaffCode() + " Configuration invalid ActionAuditId" + bn.getActionAuditId() + ", duration: " + (System.currentTimeMillis() - timeSt));
			}
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
				append("|\tACTION_AUDIT_ID|").
				append("|\tisdn\t|").
				append("|\tsub_id\t|").
				append("|\timsi\t|").
				append("|\tuser_created\t|").
				append("|\tshop_code\t|").
				append("|\tproduct_code\t|").
				append("|\tserial\t|");
		for (Record record : listRecord) {
			BonusConnectPostPaid bn = (BonusConnectPostPaid) record;
			br.append("\r\n").
					append("|\t").
					append(bn.getActionAuditId()).
					append("||\t").
					append(bn.getIsdn()).
					append("||\t").
					append(bn.getSubId()).
					append("||\t").
					append(bn.getImsi()).
					append("||\t").
					append(bn.getStaffCode()).
					append("||\t").
					append(bn.getShopCode()).
					append("||\t").
					append(bn.getProductCode()).
					append("||\t").
					append(bn.getSerial());
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

	private static boolean isNullOrEmpty(String s) {
		if (s == null || s.trim().length() == 0) {
			return true;
		} else {
			return false;
		}
	}

	private static String retNull(String s) {
		if (s == null || s.trim().length() == 0) {
			return "";
		} else {
			return s.trim();
		}
	}

}
