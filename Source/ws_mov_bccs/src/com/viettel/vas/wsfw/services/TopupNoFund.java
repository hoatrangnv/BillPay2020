/*
 * Copyright (C) 2010 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.vas.wsfw.services;

import com.viettel.im.database.BO.InvoiceListBean;
import com.viettel.im.database.DAO.InvoiceListDAO;
import com.viettel.vas.util.ExchangeClientChannel;
import com.viettel.vas.wsfw.common.WebserviceAbstract;
import javax.jws.WebMethod;
import javax.jws.WebParam;
import javax.jws.WebService;
import com.viettel.vas.wsfw.common.Common;
import com.viettel.vas.wsfw.common.Vas;
import com.viettel.vas.wsfw.database.DbPost;
import com.viettel.vas.wsfw.database.DbPre;
import com.viettel.vas.wsfw.database.DbProcessor;
import com.viettel.vas.wsfw.database.IMSessionFactory;
import com.viettel.vas.wsfw.object.Subscriber;
import com.viettel.vas.wsfw.object.Topup;
import com.viettel.vas.wsfw.object.TransLog;
import com.viettel.vas.wsfw.object.UserInfo;
import com.viettel.data.ws.utils.Exchange;
import com.viettel.data.ws.utils.Service;
import com.viettel.data.ws.utils.Utils;
import com.viettel.smsfw.manager.AppManager;
import com.viettel.vas.wsfw.object.ProductMonthlyFee;
import com.viettel.vas.wsfw.object.SubAdslLLPrepaid;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;
import org.hibernate.Session;

/**
 *
 * @author minhnh@viettel.com.vn
 * @since Jun 4, 2013
 * @version 1.0
 */
@WebService
public class TopupNoFund extends WebserviceAbstract {

	DbProcessor db;
	DbPre dbPre;
	DbPost dbPost;
	Exchange exch;
	Service service;
	String sStaffIdBillPay;
	String staffCode;
	String groupId;
	long staffIdBillPay;
	long shopIdBillPay;
	String shopId;
	SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
	SimpleDateFormat sdf2 = new SimpleDateFormat("dd/MM/yyyy");
	String msgFtthNotEnoughMoney;
	String msgFtthFail;
	String msgFtthSuccess;
	String packageFamilia;

	public TopupNoFund() throws Exception {
		super("TopupNoFund");
		try {
			sStaffIdBillPay = ResourceBundle.getBundle("vas").getString("invoiceStaffIdBillPay");
			staffIdBillPay = Long.valueOf(sStaffIdBillPay);
			shopId = ResourceBundle.getBundle("vas").getString("invoiceShopIdBillPay");
			shopIdBillPay = Long.valueOf(shopId);
			staffCode = ResourceBundle.getBundle("vas").getString("invoiceStaffCode");
			groupId = ResourceBundle.getBundle("vas").getString("invoiceGroupId");
			packageFamilia = ResourceBundle.getBundle("vas").getString("PCKG_FTTH_MOBILE");
			dbPre = new DbPre("cm_pre", logger);
			dbPost = new DbPost("cm_pos", logger);
			db = new DbProcessor("dbtopup", logger);
			exch = new Exchange(ExchangeClientChannel.getInstance("../etc/exchange_client.cfg").getInstanceChannel(), logger);
			service = new Service(ExchangeClientChannel.getInstance("../etc/service_client.cfg").getInstanceChannel(), logger);
			msgFtthNotEnoughMoney = ResourceBundle.getBundle("vas").getString("msgFtthNotEnoughMoney");
			msgFtthFail = ResourceBundle.getBundle("vas").getString("msgFtthFail");
			msgFtthSuccess = ResourceBundle.getBundle("vas").getString("msgFtthSuccess");
			if (mapUser == null || mapUser.size() <= 0) {
				mapUser = db.getUserInfo();
			}
		} catch (Exception e) {
			logger.error("Fail init webservice Recharge");
			logger.error(e);
		}
	}

	@WebMethod(operationName = "topup")
	public String topup(
			@WebParam(header = true, name = "client", targetNamespace = "") String client,
			@WebParam(name = "requestId", targetNamespace = "") String requestId,
			@WebParam(name = "msisdn", targetNamespace = "") String msisdn,
			@WebParam(name = "ammount", targetNamespace = "") String amount,
			@WebParam(name = "userName") String wsuser,
			@WebParam(name = "passWord") String wspassword,
			@WebParam(name = "branch") String branch) {
		TransLog tran = new TransLog();
		tran.setClient(wsuser);
		tran.setIsdn(msisdn);
		tran.setMoney(amount);
		tran.setTransType(Vas.Topup.TRANS_TYPE_TOPUP);
		tran.setWsCode("TopupNotFund");
		tran.setRequestId(requestId);
		tran.setInput(msisdn + "|" + amount);
		long timeStart = System.currentTimeMillis();
		tran.setStartTime(new Timestamp(timeStart));
		try {
			logger.info("Start process topup for sub " + msisdn + " client " + wsuser);
//        step 1: validate input
			if (msisdn == null || "".equals(msisdn.trim())
					|| amount == null || "".equals(amount.trim())
					|| wsuser == null || "".equals(wsuser.trim())
					|| wspassword == null || "".equals(wspassword.trim())
					|| requestId == null || "".equals(requestId.trim())) {
				logger.warn("Invalid input sub " + msisdn);
				tran.setDuration(System.currentTimeMillis() - timeStart);
				tran.setOutput(Vas.Topup.INPUT_ERROR + "|" + "The input format is invalid");
				tran.setResultCode(Vas.Topup.INPUT_ERROR);
				db.insertTopupLog(tran);
				return Vas.Topup.INPUT_ERROR + "|The input format is invalid";
			}
//        step 2: validate ip
			String ip = getIpClient();
			if (ip == null || "".equals(ip.trim())) {
				logger.warn("Can not get ip for sub " + msisdn);
				tran.setDuration(System.currentTimeMillis() - timeStart);
				tran.setOutput(Vas.Topup.INPUT_ERROR + "|" + "The remote IP is not allowed");
				tran.setResultCode(Vas.Topup.INPUT_ERROR);
				db.insertTopupLog(tran);
				return Vas.Topup.INPUT_ERROR + "|The remote IP is not allowed";
			}
			tran.setIpRemote(ip);
			UserInfo user = authenticate(db, wsuser, wspassword, ip);
			if (user == null || user.getId() < 0) {
				logger.warn("Invalid account " + msisdn);
				tran.setDuration(System.currentTimeMillis() - timeStart);
				tran.setOutput(Vas.Topup.INPUT_ERROR + "|" + "Invalid account");
				tran.setResultCode(Vas.Topup.INPUT_ERROR);
				db.insertTopupLog(tran);
				return Vas.Topup.INPUT_ERROR + "|Invalid accoun";
			}
//        Validate money
			Long money = 0l;
			try {
				money = Long.parseLong(amount);
//                money = Math.abs(money);
				if (money > 100000 || money <= 0) { //Fix maximum is 10.000 mt
					logger.warn("Can not recharge due to the value is too big " + msisdn + " " + money);
					tran.setDuration(System.currentTimeMillis() - timeStart);
					tran.setOutput(Vas.Topup.INVALID_CHARGING_AMOUNT + "|" + "Money too big");
					tran.setResultCode(Vas.Topup.INVALID_CHARGING_AMOUNT);
					db.insertTopupLog(tran);
					return Vas.Topup.INVALID_CHARGING_AMOUNT + "|Money too big";
				}
			} catch (Exception e) {
				logger.warn("Invalid money " + msisdn + " " + e.toString());
				tran.setDuration(System.currentTimeMillis() - timeStart);
				tran.setOutput(Vas.Topup.INVALID_CHARGING_AMOUNT + "|" + "Invalid money");
				tran.setResultCode(Vas.Topup.INVALID_CHARGING_AMOUNT);
				db.insertTopupLog(tran);
				return Vas.Topup.INVALID_CHARGING_AMOUNT + "|Money invalid";
			}
//            Huynq13 20180827 start add to support pay by ReferenceId for postpaid
			boolean isReference = false;
			if (Utils.checkIsReference(msisdn)) {
				isReference = true;
			} else if (!msisdn.startsWith(Common.config.countryCode)) {
				msisdn = Common.config.countryCode + msisdn;
			}
// Check postpaid mobile
//            Subscriber posSub = dbPost.getSubInfoMobile(msisdn, false);
//            if (posSub == null) {
//                logger.warn("Fail get post sub " + msisdn);
//                tran.setDuration(System.currentTimeMillis() - timeStart);
//                tran.setOutput(Vas.Topup.DATABASE_ERROR + "|" + "Server is too busy");
//                tran.setResultCode(Vas.Topup.DATABASE_ERROR);
//                db.insertTopupLog(tran);
//                return Vas.Topup.DATABASE_ERROR + "|Server is too busy";
//            }
			long contractId = 0;
			boolean isVipsub = false;
			if (isReference) {
				//Bacnx 20201030 add payment for Vipsub
				if (msisdn.startsWith("2")) {
					isVipsub = true;
				} else {
					contractId = db.getContractMobileByRefer(msisdn);
				}
			} else {
				contractId = db.getContractMobile(msisdn, msisdn);
			}

			if (isVipsub) {
				//<editor-fold defaultstate="collapsed" desc="VIPSUP">
				tran.setSubType(Vas.Constanst.VIPSUB + "");
				Map<String, Double> map = new HashMap<String, Double>();
				contractId = db.getVipSubInfoByRefer(msisdn);
				if (contractId == 0) {
					logger.warn("Cannot find reference Id in vipsub system " + msisdn);
					tran.setDuration(System.currentTimeMillis() - timeStart);
					tran.setOutput(Vas.Topup.FAIL_RECHARGE + "|" + "VipsubInfo not found");
					tran.setResultCode(Vas.Topup.FAIL_RECHARGE);
					db.insertTopupLog(tran);
					return Vas.Topup.FAIL_RECHARGE + "|VipsubInfo not found";
				}

				//Get total amount 
				long totalRemainMoney = db.getMoneyPrepaidOfVipsub(contractId);
				if (totalRemainMoney == -2) {
					logger.warn("Error when check prepaid money " + contractId);
					tran.setDuration(System.currentTimeMillis() - timeStart);
					tran.setOutput(Vas.Topup.FAIL_RECHARGE + "|" + "Error when check prepaid money");
					tran.setResultCode(Vas.Topup.FAIL_RECHARGE);
					db.insertTopupLog(tran);
					return Vas.Topup.FAIL_RECHARGE + "|Error when check prepaid money";
				} else if (totalRemainMoney > 0) {
					logger.warn("This contract have prepaid money " + contractId + " totalRemainMoney " + totalRemainMoney);
					money += totalRemainMoney;
				}

				//insert into prepaid table
				if (totalRemainMoney == -1) {
					int insertPrepaid = db.insertVipSubPaymentRemain(contractId, money, 0);
					logger.warn("Result insert vipsub info " + contractId + " insertPrepaid " + insertPrepaid);
				} else {
					int updatePrepaid = db.updateVipsubPaymentPrepaid(contractId, money, 0);
					logger.warn("Result update vipsub info " + contractId + " updatePrepaid " + updatePrepaid);
				}

				tran.setDuration(System.currentTimeMillis() - timeStart);
				tran.setOutput(Vas.Topup.SUCCESSFUL + "|The transaction was done successfully");
				tran.setResultCode(Vas.Topup.SUCCESSFUL);
				db.insertTopupLog(tran);
				return Vas.Topup.SUCCESSFUL + "|The transaction was done successfully";

//</editor-fold>
			} else {
				//<editor-fold defaultstate="collapsed" desc="MOBILE,FBB">
				if (contractId > 0) {
					tran.setSubType(Vas.Constanst.POSTPAID + "");
					logger.info("Start topup for postpaid sub " + msisdn + " money " + money);
//                long contractId = db.getContractMobile(msisdn.substring(3), msisdn);
					Session ss = com.viettel.vas.wsfw.database.IMSessionFactory.getSession();
					ss.getTransaction().begin();
					InvoiceListDAO invoiceListUtils = new InvoiceListDAO(ss);
					List invoiceListList = invoiceListUtils.getAvailableInvoiceList(shopIdBillPay,
							staffIdBillPay);
					if (invoiceListList != null && invoiceListList.size() > 0) {
						InvoiceListBean invoiceList = (InvoiceListBean) invoiceListList.get(0);
						String blockNo = invoiceListUtils.getBlockNoFormatByBookType(invoiceList.getSerialNo(),
								invoiceList.getBlockNo(), invoiceList.getCurrInvoiceNo());
						String invoiceNumber = invoiceListUtils.getInvoiceNoFormatByBookType(invoiceList.getSerialNo(),
								invoiceList.getBlockNo(), invoiceList.getCurrInvoiceNo());
						long resultTopup;
//                    20180917 modify to separate collection_staff if payment by Bank, fix collection_staff_id = 1380839, BANK_PAYMENT
						if (isReference) {
							resultTopup = db.genBillPay(contractId, money, "9", 1380839l, "2", invoiceList.getSerialNo(),
									blockNo, invoiceNumber, 0l, "1.1.1.1", Long.valueOf(groupId),
									"BANK_PAYMENT", msisdn);
						} else {
							resultTopup = db.genBillPay(contractId, money, "9", staffIdBillPay, "2", invoiceList.getSerialNo(),
									blockNo, invoiceNumber, 0l, "1.1.1.1", Long.valueOf(groupId),
									staffCode, msisdn);
						}
						if (resultTopup == 0) {
							logger.info("Payment call package pck_pay113 success for sub " + msisdn + " money " + money
									+ " now update invoice to used, invoice_list_id" + invoiceList.getInvoiceListId());
							long invoiceUsed = invoiceListUtils.updateInvoiceToUsing(shopIdBillPay, staffIdBillPay,
									invoiceList.getSerialNo(), invoiceList.getBlockNo(), invoiceList.getCurrInvoiceNo());
							ss.getTransaction().commit();
							ss.flush();
							ss.close();
							logger.info("Update invoice success for sub " + msisdn + " invoiceUsed " + invoiceUsed);
							tran.setInvoiceListId(invoiceList.getInvoiceListId() + "|" + invoiceUsed);
							tran.setDuration(System.currentTimeMillis() - timeStart);
							tran.setOutput(Vas.Topup.SUCCESSFUL + "|" + "Payment success");
							tran.setResultCode(Vas.Topup.SUCCESSFUL);
							db.insertTopupLog(tran);
							return Vas.Topup.SUCCESSFUL + "|The transaction was done successfully";
						} else {
							logger.warn("Fail to topup for sub " + msisdn);
							tran.setDuration(System.currentTimeMillis() - timeStart);
							tran.setOutput(Vas.Topup.FAIL_RECHARGE + "|" + "Fail call package pck_pay113 to topup");
							tran.setResultCode(Vas.Topup.FAIL_RECHARGE);
							db.insertTopupLog(tran);
							return Vas.Topup.FAIL_RECHARGE + "|Fail to topup";
						}
					} else {
						logger.warn("Don't have invoice so can not make bill pay for sub " + msisdn);
						tran.setDuration(System.currentTimeMillis() - timeStart);
						tran.setOutput(Vas.Topup.FAIL_RECHARGE + "|" + "Out of invoice");
						tran.setResultCode(Vas.Topup.FAIL_RECHARGE);
						db.insertTopupLog(tran);
						return Vas.Topup.FAIL_RECHARGE + "|Out of invoice";
					}
				} else {
					logger.info("Not postpaid sub, now check prepaid sub " + msisdn);
					// Lay thong tin thue bao mobile tra truoc
					Subscriber preSub = dbPre.getSubInfoMobile(msisdn, false);
					if (preSub == null) {
						logger.warn("Fail get pre sub " + msisdn);
						tran.setDuration(System.currentTimeMillis() - timeStart);
						tran.setOutput(Vas.Topup.DATABASE_ERROR + "|" + "Server is too busy");
						tran.setResultCode(Vas.Topup.DATABASE_ERROR);
						db.insertTopupLog(tran);
						return Vas.Topup.DATABASE_ERROR + "|Server is too busy";
					}
					if (!preSub.getMsisdn().equals("NO_INFO_SUB")) {
						//        20180428 Huynq add to check active status on OCS
//                    20200103 uncomment this step follow request from CC
						String activeStatus = exch.checkActiveStatusOnOCS(msisdn);
						if (activeStatus == null || activeStatus.trim().equals("1") || activeStatus.trim().equals("5")) {
							logger.warn("Not active so not support recharge " + msisdn);
							tran.setDuration(System.currentTimeMillis() - timeStart);
							tran.setOutput(Vas.ResultCode.INVALID_INPUT + "|" + "Sub not yet active");
							tran.setResultCode(Vas.ResultCode.INVALID_INPUT + "");
							db.insertTopupLog(tran);
							return Vas.Topup.NOT_EXISTS + "|Sub not yet active";
						}
						tran.setSubType(Vas.Constanst.PREPAID + "");
						logger.info("Topup for prepaid modbile subscriber: " + msisdn);
						Topup result = new Topup();
						//Bacnx 20191014 modify command belong party code
						String command = "";
						String partyCode = "";
						if (branch != null && !"".equals(branch.trim()) && !"?".equals(branch.trim())) {
							command = "OCSHW_PAYMENT_PARTYCODE";
							partyCode = branch.trim();
						} else {
							command = "OCSHW_PAYMENT";
						}
						result = exch.topupPrePaidByPartyCode(msisdn, money + "", command, partyCode);
						if (result == null || !"0".equals(result.getErr())) {
							logger.warn("Topup fail for pre sub " + msisdn + " money " + money);
							tran.setDuration(System.currentTimeMillis() - timeStart);
							tran.setOutput(Vas.Topup.FAIL_RECHARGE + "|" + "Could not topup");
							tran.setResultCode(Vas.Topup.FAIL_RECHARGE);
							db.insertTopupLog(tran);
							return Vas.Topup.FAIL_RECHARGE + "|Could not topup";
						} else {
							logger.warn("Topup success for pre sub " + msisdn + " money " + money);
							tran.setDuration(System.currentTimeMillis() - timeStart);
							tran.setOutput(Vas.Topup.SUCCESSFUL + "|" + "Topup success for pre sub");
							tran.setResultCode(Vas.Topup.SUCCESSFUL);
							db.insertTopupLog(tran);
							return Vas.Topup.SUCCESSFUL + "|The transaction was done successfully";
						}
					} else {
						logger.info("Check info ADSL sub " + msisdn);
						// Lay thong tin thue bao ADSL
//                            Subscriber adslSub = dbPost.getSubInfoADSL(msisdn);
//                            if (adslSub == null) {
//                                logger.warn("Fail get adsl sub " + msisdn);
//                                tran.setDuration(System.currentTimeMillis() - timeStart);
//                                tran.setOutput(Vas.Topup.DATABASE_ERROR + "|" + "Server is too busy");
//                                tran.setResultCode(Vas.Topup.DATABASE_ERROR);
//                                db.insertTopupLog(tran);
//                                return Vas.Topup.DATABASE_ERROR + "|Server is too busy";
//                            }
						if (isReference) {
							contractId = db.getContractFbbByRefer(msisdn);
						} else {
							contractId = db.getContractFbb(msisdn, msisdn);
						}
						if (contractId > 0) {
							tran.setSubType(Vas.Constanst.FIX_BROADBAND + "");
							//                    Huynq13 20180827 check FTTH prepaid, remember if FTTH prepaid must set SUB_TYPE = 5 to make sale trans
//                                Is FTTH prepaid if msisdn is account not isReference and normal in sub_adsl_ll_prepaid table
							SubAdslLLPrepaid subPrepaid;
							subPrepaid = db.checkFtthPrepaid(contractId, msisdn);
							if (subPrepaid != null && subPrepaid.getExpireTime() != null) {
								tran.setSubType(Vas.Constanst.FIX_BROADBAND_PREPAID + "");
								logger.info(msisdn + " is prepaid FTTH, and now process for modifing expire_time ");
//                                    Calculate money fee range base on product and discount of range prepaid
								ProductMonthlyFee product = db.getMonthlyFeeFtthPre(amount, subPrepaid.getNewProductCode());
								if (product == null || product.getMonthlyFee() <= 0) {
									logger.info(msisdn + " is prepaid FTTH, but can not get product monthly fee");
									tran.setDuration(System.currentTimeMillis() - timeStart);
									tran.setOutput(Vas.Topup.INPUT_ERROR + "|" + "is prepaid FTTH, but can not get product monthly fee");
									tran.setResultCode(Vas.Topup.INPUT_ERROR);
									db.insertTopupLog(tran);
									String msg = msgFtthFail.replace("%ACCOUNT%", subPrepaid.getAccount());
									db.sendSms(subPrepaid.getTelFax(), msg, "866123123");
									db.insertActionAudit(subPrepaid.getId(), msisdn,
											"Extent fail because can not get product monthly fee account " + msisdn + " old time "
											+ sdf.format(subPrepaid.getExpireTime())
											+ " money " + money, subPrepaid.getSubId(), ip);
									return Vas.Topup.INPUT_ERROR + "|is prepaid FTTH, but can not get product monthly fee";
								}
								long moneyOneMonth = product.getMonthlyFee();
//                                    Check less than one month fee
								if (money < moneyOneMonth) {
									logger.info(msisdn + " is prepaid FTTH, but money less than one month fee");
									tran.setDuration(System.currentTimeMillis() - timeStart);
									tran.setOutput(Vas.Topup.INVALID_CHARGING_AMOUNT + "|" + "is prepaid FTTH, but money less than one month fee");
									tran.setResultCode(Vas.Topup.INVALID_CHARGING_AMOUNT);
									db.insertTopupLog(tran);
									String msg = msgFtthNotEnoughMoney.replace("%ACCOUNT%", subPrepaid.getAccount());
									msg = msg.replace("%MONEY%", (moneyOneMonth - money) + "");
									db.sendSms(subPrepaid.getTelFax(), msg, "866123123");
									db.insertActionAudit(subPrepaid.getId(), msisdn,
											"Extent fail because money less than one month fee account " + msisdn + " old time "
											+ sdf.format(subPrepaid.getExpireTime())
											+ " money " + money, subPrepaid.getSubId(), ip);
									return Vas.Topup.INVALID_CHARGING_AMOUNT + "|is prepaid FTTH, but money less than one month fee"; //Remember can not change this error_code because it related to PaymentByBank
								}
//                                    Get money for 3 month
								double disCountPercent;
								double noDiscountNoTax;
								double amountDiscount;
								double amountBeforeTax;
								double tax;
								double amount3Month;
								double amount6Month;
								double amount12Month;
								long convertMonth = 0;
								long remainMoney = 0;
								long convertDay = 0;
								disCountPercent = Double.parseDouble(product.getMapDiscount().get("3")) / 100;
								noDiscountNoTax = moneyOneMonth * 3 / 1.17;
								amountDiscount = noDiscountNoTax * disCountPercent; // Discount after tax
								amountBeforeTax = noDiscountNoTax - amountDiscount;
								tax = 0.17 * amountBeforeTax; //fix tax = 17%     
								amount3Month = amountBeforeTax + tax;
//                                    Check money between one month and three month fee
								if (money < amount3Month) {
									convertMonth = money / moneyOneMonth;
									remainMoney = money - moneyOneMonth * convertMonth;
									convertDay = (remainMoney * 30) / moneyOneMonth;
									logger.info(msisdn + " has money between fee of 1 month " + moneyOneMonth
											+ " and fee of 3 month " + amount3Month + " convertMonth " + convertMonth
											+ " remainMoney " + remainMoney + " convertDay " + convertDay);
								} else {
									disCountPercent = Double.parseDouble(product.getMapDiscount().get("6")) / 100;
									noDiscountNoTax = moneyOneMonth * 6 / 1.17;
									amountDiscount = noDiscountNoTax * disCountPercent; // Discount after tax
									amountBeforeTax = noDiscountNoTax - amountDiscount;
									tax = 0.17 * amountBeforeTax; //fix tax = 17%     
									amount6Month = amountBeforeTax + tax;
									if (money < amount6Month) {
										convertMonth = 3;
										remainMoney = money - (long) amount3Month;
										convertDay = (remainMoney * 90) / (long) amount3Month;
										logger.info(msisdn + " has money between fee of 3 month " + amount3Month
												+ " and fee of 6 month " + amount6Month + " convertMonth " + convertMonth
												+ " remainMoney " + remainMoney + " convertDay " + convertDay);
									} else {
										disCountPercent = Double.parseDouble(product.getMapDiscount().get("12")) / 100;
										noDiscountNoTax = moneyOneMonth * 12 / 1.17;
										amountDiscount = noDiscountNoTax * disCountPercent; // Discount after tax
										amountBeforeTax = noDiscountNoTax - amountDiscount;
										tax = 0.17 * amountBeforeTax; //fix tax = 17%     
										amount12Month = amountBeforeTax + tax;
										if (money < amount12Month) {
											convertMonth = 6;
											remainMoney = money - (long) amount6Month;
											convertDay = (remainMoney * 180) / (long) amount6Month;
											logger.info(msisdn + " has money between fee of 6 month " + amount6Month
													+ " and fee of 12 month " + amount12Month + " convertMonth " + convertMonth
													+ " remainMoney " + remainMoney + " convertDay " + convertDay);
										} else {
											convertMonth = 12;
											remainMoney = money - (long) amount12Month;
											convertDay = (remainMoney * 360) / (long) amount12Month;
											logger.info(msisdn + " has money over fee of 12 month " + amount12Month
													+ " convertMonth " + convertMonth
													+ " remainMoney " + remainMoney + " convertDay " + convertDay);
										}
									}
								}
//                                        open sub
								String center = db.getCenter(msisdn);
								if (center == null || center.trim().length() <= 0) {
									logger.warn("Can not get center of account " + msisdn + " so set default center = 1");
									center = "1";
								}
								String resActive = service.activeFBB(subPrepaid, center, msisdn);
								if (resActive == null || !"0".equals(resActive)) {
									logger.warn("Fail active FBB Prepaid sub " + msisdn);
									tran.setDuration(System.currentTimeMillis() - timeStart);
									tran.setOutput(Vas.Topup.DATABASE_ERROR + "|" + "Can not active for FBB prepaid sub");
									tran.setResultCode(Vas.Topup.DATABASE_ERROR);
									db.insertTopupLog(tran);
									String msg = msgFtthFail.replace("%ACCOUNT%", subPrepaid.getAccount());
									db.sendSms(subPrepaid.getTelFax(), msg, "866123123");
									db.insertActionAudit(subPrepaid.getId(), msisdn,
											"Extent fail because Can not active for account " + msisdn + " old time "
											+ sdf.format(subPrepaid.getExpireTime())
											+ " money " + money, subPrepaid.getSubId(), ip);
									return Vas.Topup.DATABASE_ERROR + "|Can not active for FBB prepaid sub";
								}
//                                        modify expire_time, block_time
								Calendar cal = Calendar.getInstance();
								Date today = new Date();
								if (subPrepaid.getExpireTime().after(today)) {
									logger.info("The expire_time after today so calculate time base on expiretime sub " + msisdn);
									cal.setTime(subPrepaid.getExpireTime());
								} else {
									logger.info("The expire_time before today so calculate time base on today sub " + msisdn);
									cal.setTime(today);
								}
								cal.add(Calendar.MONTH, (int) convertMonth);
								cal.add(Calendar.DATE, (int) convertDay);
								int resExtend = db.updateExpireFtthPre(subPrepaid.getId(), msisdn, cal.getTime());
								if (resExtend <= 0) {
									logger.warn("Fail update expire_time for FBB Prepaid sub " + msisdn);
									tran.setDuration(System.currentTimeMillis() - timeStart);
									tran.setOutput(Vas.Topup.DATABASE_ERROR + "|" + "Fail to extend for FBB prepaid sub");
									tran.setResultCode(Vas.Topup.DATABASE_ERROR);
									db.insertTopupLog(tran);
									String msg = msgFtthFail.replace("%ACCOUNT%", subPrepaid.getAccount());
									db.sendSms(subPrepaid.getTelFax(), msg, "866123123");
									db.insertActionAudit(subPrepaid.getId(), msisdn,
											"Extent fail because Can not update new expire time for account " + msisdn + " old time "
											+ sdf.format(subPrepaid.getExpireTime())
											+ " money " + money, subPrepaid.getSubId(), ip);
									return Vas.Topup.DATABASE_ERROR + "|Fail to extend for FBB prepaid sub";
								}
//                                        save action audit
								db.insertActionAudit(subPrepaid.getId(), msisdn,
										"Extent expire_time of FTTH Pre account " + msisdn + " old time "
										+ sdf.format(subPrepaid.getExpireTime())
										+ " new time " + sdf.format(cal.getTime()) + " money " + money + " addMonth " + convertMonth
										+ " addDay " + convertDay, subPrepaid.getSubId(), ip);
								db.updateSubAdslLL(subPrepaid.getAccount());
								tran.setDuration(System.currentTimeMillis() - timeStart);
								tran.setOutput(Vas.Topup.SUCCESSFUL + "|"
										+ "Extent success for FTTH Pre " + " old time "
										+ sdf.format(subPrepaid.getExpireTime()) + " new time " + sdf.format(cal.getTime()));
								tran.setResultCode(Vas.Topup.SUCCESSFUL);
								db.insertTopupLog(tran);
								String msg = msgFtthSuccess.replace("%ACCOUNT%", subPrepaid.getAccount());
								msg = msg.replace("%EXPIRE%", sdf2.format(cal.getTime()));
								db.sendSms(subPrepaid.getTelFax(), msg, "866123123");
								//tannh20190803 start: add data FTTH Family 
								long addmonth = 0;
								addmonth = convertMonth + convertDay / 30;
								String[] arrPackage = packageFamilia.split("\\|");
								for (String pck : arrPackage) {
									if (subPrepaid.getNewProductCode().equals(pck)) {
										Date dateExpire = subPrepaid.getExpireTime();// all done
										Date sysDate = new Date();
										if (sysDate.before(dateExpire)) {
											db.renewalSubMbFtth(subPrepaid.getSubId(), cal.getTime(), addmonth, true);
										} else {
											db.renewalSubMbFtth(subPrepaid.getSubId(), cal.getTime(), addmonth, false);
										}
										break;
									}
								}
								//Create sale trans

								//tannh20190803 end: add data FTTH Family 
								return Vas.Topup.SUCCESSFUL + "|Extent success for FTTH Pre";
							} else {
//                                If not prepaid must check have postpaid FBB account
								if (db.checkFtthPospaid(contractId, msisdn)) {
									logger.info("Topup for adsl sub " + msisdn);
									logger.info("Start topup for adsl leadline sub " + msisdn + " money " + money);
//                                long contractId = db.getContractFbb(msisdn.substring(3), msisdn);
									Session ss = IMSessionFactory.getSession();
									ss.getTransaction().begin();
									InvoiceListDAO invoiceListUtils = new InvoiceListDAO(ss);
									List invoiceListList = invoiceListUtils.getAvailableInvoiceList(shopIdBillPay, staffIdBillPay);
									if (invoiceListList != null && invoiceListList.size() > 0) {
										InvoiceListBean invoiceList = (InvoiceListBean) invoiceListList.get(0);
										String blockNo = invoiceListUtils.getBlockNoFormatByBookType(invoiceList.getSerialNo(),
												invoiceList.getBlockNo(), invoiceList.getCurrInvoiceNo());
										String invoiceNumber = invoiceListUtils.getInvoiceNoFormatByBookType(invoiceList.getSerialNo(),
												invoiceList.getBlockNo(), invoiceList.getCurrInvoiceNo());
										long resultTopup;
//                    20180917 modify to separate collection_staff if payment by Bank, fix collection_staff_id = 1380839, BANK_PAYMENT
										if (isReference) {
											resultTopup = db.genBillPay(contractId, (long) money, "9", 1380839l, "2", invoiceList.getSerialNo(),
													blockNo, invoiceNumber, 0l, "1.1.1.1", Long.valueOf(groupId),
													"BANK_PAYMENT", msisdn);
										} else {
											resultTopup = db.genBillPay(contractId, (long) money, "9", staffIdBillPay, "2", invoiceList.getSerialNo(),
													blockNo, invoiceNumber, 0l, "1.1.1.1", Long.valueOf(groupId),
													staffCode, msisdn);
										}
										if (resultTopup == 0) {
											logger.info("Payment call pck_pay113 success for sub " + msisdn + " money " + money
													+ " now update invoice to used, invoice_list_id" + invoiceList.getInvoiceListId());
											long invoiceUsed = invoiceListUtils.updateInvoiceToUsing(shopIdBillPay, staffIdBillPay,
													invoiceList.getSerialNo(), invoiceList.getBlockNo(), invoiceList.getCurrInvoiceNo());
											ss.getTransaction().commit();
											ss.flush();
											ss.close();
											logger.info("Update invoice success for sub " + msisdn + " invoiceUsed " + invoiceUsed);
											tran.setInvoiceListId(invoiceList.getInvoiceListId() + "|" + invoiceUsed);
											tran.setDuration(System.currentTimeMillis() - timeStart);
											tran.setOutput(Vas.Topup.SUCCESSFUL + "|" + "Payment success");
											tran.setResultCode(Vas.Topup.SUCCESSFUL);
											db.insertTopupLog(tran);
											return Vas.Topup.SUCCESSFUL + "|The transaction was done successfully";
										} else {
											logger.warn("Fail to topup for adls leadline sub " + msisdn);
											tran.setDuration(System.currentTimeMillis() - timeStart);
											tran.setOutput(Vas.Topup.FAIL_RECHARGE + "|" + "Fail call package pck_pay113 to topup");
											tran.setResultCode(Vas.Topup.FAIL_RECHARGE);
											db.insertTopupLog(tran);
											return Vas.Topup.FAIL_RECHARGE + "|Fail to topup";
										}
									} else {
										logger.warn("Don't have invoice so can not make bill pay for sub " + msisdn);
										tran.setDuration(System.currentTimeMillis() - timeStart);
										tran.setOutput(Vas.Topup.FAIL_RECHARGE + "|" + "Out of invoice");
										tran.setResultCode(Vas.Topup.FAIL_RECHARGE);
										db.insertTopupLog(tran);
										return Vas.Topup.FAIL_RECHARGE + "|Out of invoice";
									}
								} else {
									logger.warn(msisdn + " not prepaid and postpaid FBB contractId " + contractId);
									tran.setDuration(System.currentTimeMillis() - timeStart);
									tran.setOutput(Vas.Topup.NOT_EXISTS + "|" + "not prepaid and postpaid FBB");
									tran.setResultCode(Vas.Topup.NOT_EXISTS);
									db.insertTopupLog(tran);
									return Vas.Topup.NOT_EXISTS + "|not prepaid and postpaid FBB";
								}
							}
						}
					}
				}
//</editor-fold>
			}

			logger.info("No infomation for charge " + msisdn);
			tran.setDuration(System.currentTimeMillis() - timeStart);
			tran.setOutput(Vas.Topup.NOT_EXISTS + "|" + "The isdn does not exists");
			tran.setResultCode(Vas.Topup.NOT_EXISTS);
			db.insertTopupLog(tran);
			return Vas.Topup.NOT_EXISTS + "|The isdn does not exists";
		} catch (Exception e) {
			logger.error("[!!!] Error topup for sub " + msisdn, e);
			logger.error(AppManager.logException(timeStart, e));
			tran.setDuration(System.currentTimeMillis() - timeStart);
			tran.setOutput(Vas.Topup.EXCEPTION + "|" + "Exception");
			tran.setResultCode(Vas.Topup.EXCEPTION);
			db.insertTopupLog(tran);
			return Vas.Topup.EXCEPTION + "|Unexpected exception";
		}
	}
}
