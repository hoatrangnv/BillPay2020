///*
// * To change this template, choose Tools | Templates
// * and open the template in the editor.
// */
//package com.viettel.paybonus.process;
//
//import com.viettel.cluster.agent.integration.Record;
//import com.viettel.paybonus.database.DbEliteConvertProduct;
//import com.viettel.paybonus.obj.EliteConvertProduct;
//import com.viettel.paybonus.obj.IntegrationProduct;
//import com.viettel.paybonus.service.Exchange;
//import com.viettel.paybonus.service.Service;
//import com.viettel.threadfw.manager.AppManager;
//import java.util.List;
//import org.apache.log4j.Logger;
//import com.viettel.threadfw.process.ProcessRecordAbstract;
//import com.viettel.vas.util.ExchangeClientChannel;
//import java.text.SimpleDateFormat;
//import java.util.ArrayList;
//import java.util.Calendar;
//import java.util.Collections;
//import java.util.Comparator;
//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.Map;
//import java.util.ResourceBundle;
//
///**
// *
// * @author HuyNQ13
// * @version 1.0
// * @since 24-03-2016
// */
//public class EliteConvertProductProcess extends ProcessRecordAbstract {
//
//	Exchange pro;
//	Service services;
//	DbEliteConvertProduct db;
//	String eliteConvertProductStr;
//	String eliteSubProductBStr;
//	String eliteSubProductA1B2;
//	List<HashMap> lstProductA1ToA;
//	List<HashMap> lstProductA1B;
//	List<HashMap> lstProductA1B2;
//
//	public EliteConvertProductProcess() {
//		super();
//		logger = Logger.getLogger(EliteConvertProductProcess.class);
//	}
//
//	@Override
//	public void initBeforeStart() throws Exception {
//		pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
//		db = new DbEliteConvertProduct("dbElite", logger);
//		eliteConvertProductStr = ResourceBundle.getBundle("configPayBonus").getString("eliteConvertProduct");
//		eliteSubProductBStr = ResourceBundle.getBundle("configPayBonus").getString("eliteSubProductB");
//		eliteSubProductA1B2 = ResourceBundle.getBundle("configPayBonus").getString("eliteSubProductA1B2");
//		lstProductA1ToA = getPricePlanCode(eliteConvertProductStr);
//		lstProductA1B = getPricePlanCode(eliteSubProductBStr);
//		lstProductA1B2= getPricePlanCode(eliteSubProductA1B2);
//
//	}
//
//	@Override
//	public List<Record> validateContraint(List<Record> listRecord) throws Exception {
//		return listRecord;
//	}
//
//	@Override
//	public List<Record> processListRecord(List<Record> listRecord) throws Exception {
//		List<Record> listResult = new ArrayList<Record>();
//		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
//		long timeSt;
//		String newProduct = "";
//		String productB = "";
//		String productB2 = "";//buy more
//		String expireTimeB = "";
//
//		for (Record record : listRecord) {
//			timeSt = System.currentTimeMillis();
//			EliteConvertProduct bn = (EliteConvertProduct) record;
//			listResult.add(bn);
//			if (db.checkAlreadyProcessed(bn.getIsdn())) {
//				
//				continue;
//			}
//			
//			bn.setDescription("");
//			String msisdn = bn.getIsdn() != null && bn.getIsdn().startsWith("258") ? bn.getIsdn() : "258" + bn.getIsdn();
//			if (bn.getID() == null || bn.getID().trim().length() < 1 || bn.getProductA1() == null || bn.getProductA1().trim().length() < 1) {
//				logger.info("Skip.Cannot get infomation, isdn " + bn.getIsdn() + ",old product:" + bn.getProductA1());
//				bn.setDescription("Skip.Cannot get infomation");
//				bn.setResultCode("E02");
//				continue;
//			}
//			newProduct = getRelationshipProduct(bn.getProductA1(), lstProductA1ToA);
//			if (newProduct == null || newProduct.length() < 1) {
//				logger.info("Skip.Cannot get new product, isdn " + bn.getIsdn() + ",old product:" + bn.getProductA1());
//				bn.setDescription("Skip.Cannot get new product");
//				bn.setResultCode("E03");
//				continue;
//			}
//
//			//remove PricePlan A'
//			logger.info("Start remove PricePlan,isdn: " + bn.getIsdn() + ",PricePlan:" + bn.getProductA1());
//			String rmPP = pro.removePrice(msisdn, bn.getProductA1());
//			logger.info("Remove PricePlan " + bn.getProductA1() + " sub " + bn.getIsdn() + " result " + rmPP);
//			if (!"0".equals(rmPP) & !"102010227".equals(rmPP)) {
//				bn.setDescription("\n1.Remove price plan failed-->Error code:" + rmPP);
//			} else {
//				bn.setDescription("\n1.Remove price plan successfully-->Error code:" + rmPP);
//			}
//
//			productB = getRelationshipProduct(bn.getProductA1(), lstProductA1B);
//			productB2= getRelationshipProduct(bn.getProductA1(), lstProductA1B2);
//			
//			//get config B and B'
//			if ((productB != null && productB.length() > 0) && (productB2 != null && productB2.length() > 0)) {
//				logger.info("Get prouduct B,B2 belong product A:" + productB+",productB2:"+productB2);
//				List<IntegrationProduct> lstproductIntegration = pro.getIntegrationProduct(pro.queryIntegration(bn.getIsdn()), productB,productB2);
//				IntegrationProduct productIntegration = getIntegrationProductMaxExpireDate(lstproductIntegration);
//				if (productIntegration != null) {
//					expireTimeB = productIntegration.getExpiredDate();
//					logger.info("Get expiretime B:" + expireTimeB);
//					//add price plan with new expire date
//					bn.setExpireDateB(expireTimeB);
//					logger.info("Start add PricePlan,isdn: " + bn.getIsdn() + ",PricePlan New :" + bn.getProductA() + ",expire date:" + expireTimeB);
//					String addPrice = pro.addPriceV3(msisdn, bn.getProductA(), expireTimeB, "20370101000000");
//					if (!"0".equals(addPrice)) {
//						logger.info("Add PricePlan failed " + bn.getProductA() + " sub " + bn.getIsdn() + " result " + addPrice);
//						bn.setDescription(bn.getDescription() + "\n2.Add price plan failed-->Error code:" + addPrice);
//						bn.setProductB(productIntegration.getId());
//						bn.setResultCode("E01");
//					} else {
//						logger.info("Add PricePlan successfully " + bn.getProductA() + " sub " + bn.getIsdn() + " result " + addPrice);
//						bn.setDescription(bn.getDescription() + "\n2.Add price plan successfully-->Error code:" + addPrice);
//						bn.setResultCode(addPrice);
//						bn.setProductB(productIntegration.getId());
//						bn.setEffectiveDateA(expireTimeB);
//					}
//					continue;
//				} else {
//
//					logger.info("NOT Exist prouduct B belong product A:" + productB);
//					//add price plan with new expire date
//					Calendar cal = Calendar.getInstance();
//					cal.add(Calendar.DATE, 1);
//					expireTimeB = sdf.format(cal.getTime());
//					bn.setExpireDateB(expireTimeB);
//					logger.info("Start add PricePlan,isdn: " + bn.getIsdn() + ",PricePlan New :" + bn.getProductA() + ",expire date:" + expireTimeB);
//					String addPrice = pro.addPriceV3(msisdn, bn.getProductA(), expireTimeB, "20370101000000");
//					if (!"0".equals(addPrice)) {
//						logger.info("Add PricePlan failed" + bn.getProductA() + " sub " + bn.getIsdn() + " result " + addPrice);
//						bn.setDescription(bn.getDescription() + "\n2.Add price plan failed-->Error code:" + addPrice);
//						bn.setResultCode("E01");
//						bn.setEffectiveDateA(expireTimeB);
//						continue;
//					} else {
//						logger.info("Add PricePlan successfully " + bn.getProductA() + " sub " + bn.getIsdn() + " result " + addPrice);
//						bn.setDescription(bn.getDescription() + "\n2.Add price plan successfully-->Error code:" + addPrice);
//						bn.setResultCode(addPrice);
//						bn.setEffectiveDateA(expireTimeB);
//						continue;
//					}
//				}
//
//			} else {
//
//				logger.info("Fail to get B product " + bn.getProductA() + " sub " + bn.getIsdn());
//				bn.setDescription(bn.getDescription() + "\n2.Fail to get B product.Missing config");
//				bn.setResultCode("E04");
//				continue;
//			}
//
//		}
//
//		listRecord.clear();
//		return listResult;
//	}
//
//	@Override
//	public void printListRecord(List<Record> listRecord) throws Exception {
//		StringBuilder br = new StringBuilder();
//		br.setLength(0);
//		br.append("\r\n").
//				append("|\tID|").
//				append("|\tISDN|").
//				append("|\tstatus|").
//				append("|\tPRODUCT_A|").
//				append("|\tPRODUCT_A1|").
//				append("|\tPRODUCT_B|");
//		for (Record record : listRecord) {
//			EliteConvertProduct bn = (EliteConvertProduct) record;
//			br.append("\r\n").
//					append("||\t").
//					append(bn.getId()).
//					append("||\t").
//					append(bn.getIsdn()).
//					append("||\t").
//					append(bn.getStatus()).
//					append("||\t").
//					append(bn.getProductA()).
//					append("||\t").
//					append(bn.getProductA1()).
//					append("||\t").
//					append(bn.getProductB());
//
//		}
//		logger.info(br);
//	}
//
//	@Override
//	public List<Record> processException(List<Record> listRecord, Exception ex) {
//		return listRecord;
//	}
//
//	@Override
//	public boolean startProcessRecord() {
//		return true;
//	}
//
//	public List<HashMap> getPricePlanCode(String eliteConfigPP) {
//		List<HashMap> lstPP = new ArrayList<HashMap>();
//		try {
//			if (eliteConfigPP != null && eliteConfigPP.length() > 0) {
//				String[] arrPP = eliteConfigPP.split("\\|");
//				if (arrPP != null && arrPP.length > 0) {
//					for (int i = 0; i < arrPP.length && arrPP[i].contains(":"); i++) {
//						HashMap map = new HashMap();
//						map.put(arrPP[i].split("\\:")[0], arrPP[i].split("\\:")[1]);
//						lstPP.add(map);
//					}
//				}
//			}
//		} catch (Exception e) {
//			logger.error("EliteChangePck :" + ">>getPricePlanCodeByProductCode:" + e.getMessage());
//			return new ArrayList<HashMap>();
//		}
//		return lstPP;
//	}
//
//	/**
//	 * Get the new product that is want to change
//	 *
//	 * @param oldProduct
//	 * @return
//	 */
//	public String getRelationshipProduct(String oldProduct, List<HashMap> lstPP) {
//		String ppCode = "";
//		try {
//			if (lstPP != null && lstPP.size() > 0) {
//				for (HashMap<String, String> map : lstPP) {
//					for (Map.Entry<String, String> entry : map.entrySet()) {
//						if (entry.getKey().equalsIgnoreCase(oldProduct)) {
//							return entry.getValue();
//						}
//					}
//				}
//			}
//		} catch (Exception ex) {
//			logger.error("EliteConvertProductProcess -> getTargetProduct" + ex.getMessage());
//			return "";
//		}
//		return ppCode;
//
//	}
//
//	public String getMaxExpireTime(List<IntegrationProduct> lstproductIntegration) {
//		if (lstproductIntegration != null) {
//			for (IntegrationProduct item : lstproductIntegration) {
//
//			}
//		}
//		return null;
//
//	}
//
//	public IntegrationProduct getIntegrationProductMaxExpireDate(List<IntegrationProduct> lstproductIntegration) {
//		try {
//			if (lstproductIntegration != null && lstproductIntegration.size() > 0) {
//				Collections.sort(lstproductIntegration, new Comparator<IntegrationProduct>() {
//					SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
//					int rs = -1;
//
//					@Override
//					public int compare(IntegrationProduct o1, IntegrationProduct o2) {
//						try {
//							rs = sdf.parse(o1.getExpiredDate()).compareTo(sdf.parse(o2.getExpiredDate()));
//						} catch (Exception e) {
//							e.printStackTrace();
//						}
//						return rs;
//					}
//				});
//				return lstproductIntegration.get(lstproductIntegration.size()-1);
//			}
//		} catch (Exception e) {
//			logger.error("getMaxExpireDateOfListProduct -> Exception" + e.getMessage());
//			return null;
//		}
//		return null;
//	}
//
//}
