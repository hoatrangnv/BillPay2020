/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbConnectKitProcessor;
import com.viettel.paybonus.obj.Bonus;
import com.viettel.paybonus.obj.Comission;
import com.viettel.paybonus.obj.Customer;
import com.viettel.paybonus.obj.Price;
import com.viettel.paybonus.obj.QueryInforResponse;
import com.viettel.paybonus.obj.ResponseWallet;
import com.viettel.paybonus.obj.StockModel;
import com.viettel.paybonus.service.BankTransferUtils;
import com.viettel.paybonus.service.EWalletUtil;
import com.viettel.paybonus.service.Exchange;
import com.viettel.paybonus.service.Service;
import com.viettel.threadfw.manager.AppManager;
import java.util.List;
import org.apache.log4j.Logger;
import com.viettel.threadfw.process.ProcessRecordAbstract;
import com.viettel.vas.util.ExchangeClientChannel;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.ResourceBundle;

/**
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class KitConnect extends ProcessRecordAbstract {

    Exchange pro;
    Service services;
    DbConnectKitProcessor db;
    //LinhNBV modified on September 04 2017: Add variables for message
    String msgFalseImageRegistrationPoint;
    String msgFalseInfoRegistrationPoint;
    String msgFalseBothRegistrationPoint;
    String msgDimImageRegistrationPoint;
    String msgMissingStudentCard;
    String msgInvalidStudentCard;
    String msgMissingImage;
    String msgFalseCustomer;
    String msgTrueCustomer;
    //LinhNBV add message register sim fail (20180606)
    String msgRegSIMActiveFail;
    //LinhNBV add message when cc staff disapprove info
    String msgDisapprove;
    String urlPaymentVoucher;
    String keyPaymentVoucher;
    String lstIsdnReceiveError;
    String[] arrIsdnReceiveError;
    String connectSimSucessfully;
    String comissionConnectSuccessfully;
    String comissionForOwner;
    String connectSystemFail;
    String reasonIdNotFound;
    String saleServiceCodeNotFound;
    String saleServicePriceNotFound;
    String priceIsdnNotFound;
    String transactionError;
    String productCodeNotFound;
    String specialProductFreeIsdn;
    String kitBonusListTaskForce;
    String[] listTaskForce;
    SimpleDateFormat sdf2 = new SimpleDateFormat("dd/MM/yyyy");
    String connectKitInvalidProfile;
    String vasCodeAmountMoney;
    String[] arrVasCodeAmountMoney;
    String vasCodeAmountRate;
    ArrayList<HashMap> lstMapVasCodeAmount;
    String pricePlanForBranch;
    String schemaDbForBranch;
    String schemaDbForFbPackageBranch;
    String vasCodeConnection;
    String[] arrVasCodeConnection;
    ArrayList<HashMap> lstMapVasCodeConnection;
    String vasCodeParam;
    String[] arrVasCodeParam;
    ArrayList<HashMap> lstMapVasCodeParam;
    String vasCodeActionType;
    String[] arrVasCodeActionType;
    ArrayList<HashMap> lstMapVasCodeActionType;
    String vasCodeChannel;
    String[] arrVasCodeChannel;
    ArrayList<HashMap> lstMapVasCodeChannel;
    String eliteMsgProductMap;
    String[] arrEliteMsgProductMap;
    ArrayList<HashMap> lstEliteMsgProductMap;
    String agentEmola;
    String[] arrAgentEmola;
    ArrayList<HashMap> lstMapAgentEmola;
    String kitConnectBonusMoneyPrepaidMonth;
    String[] arrBonusMoneyPrepaidMonth;
    String kitConnectUpdateTransCodeFail;
    String bonusRateForPrepaidMonth;
    String kitConnectHandsetNotFound;
    String kitConnectPriceHandsetNotFound;
    String kitConnectSerialHandsetNotFound;
    String kitConnectSmsSaleHandsetManager;
    String bonus4GMsgConnNew;

    public KitConnect() {
        super();
        logger = Logger.getLogger(KitConnect.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
        services = new Service(ExchangeClientChannel.getInstance("../etc/service_client.cfg").getInstanceChannel(), logger);
        db = new DbConnectKitProcessor();
        msgFalseImageRegistrationPoint = ResourceBundle.getBundle("configPayBonus").getString("msgFalseImageRegistrationPoint");
        msgFalseInfoRegistrationPoint = ResourceBundle.getBundle("configPayBonus").getString("msgFalseInfoRegistrationPoint");
        msgFalseBothRegistrationPoint = ResourceBundle.getBundle("configPayBonus").getString("msgFalseBothRegistrationPoint");
        msgDimImageRegistrationPoint = ResourceBundle.getBundle("configPayBonus").getString("msgDimImageRegistrationPoint");
        msgMissingStudentCard = ResourceBundle.getBundle("configPayBonus").getString("msgMissingStudentCard");
        msgInvalidStudentCard = ResourceBundle.getBundle("configPayBonus").getString("msgInvalidStudentCard");
        msgMissingImage = ResourceBundle.getBundle("configPayBonus").getString("msgMissingImage");
        msgFalseCustomer = ResourceBundle.getBundle("configPayBonus").getString("msgFalseCustomer");
        msgTrueCustomer = ResourceBundle.getBundle("configPayBonus").getString("msgTrueCustomer");
        msgRegSIMActiveFail = ResourceBundle.getBundle("configPayBonus").getString("msgRegSIMActiveFail");

        msgDisapprove = ResourceBundle.getBundle("configPayBonus").getString("msgCCDisapprovesInfo");
        urlPaymentVoucher = ResourceBundle.getBundle("configPayBonus").getString("urlPaymentVoucher");
        keyPaymentVoucher = ResourceBundle.getBundle("configPayBonus").getString("keyPaymentVoucher");
        lstIsdnReceiveError = ResourceBundle.getBundle("configPayBonus").getString("lstIsdnReceiveError");
        arrIsdnReceiveError = lstIsdnReceiveError.split("\\|");

        connectSimSucessfully = ResourceBundle.getBundle("configPayBonus").getString("connectSimSucessfully");
        comissionConnectSuccessfully = ResourceBundle.getBundle("configPayBonus").getString("comissionConnectSuccessfully");
        comissionForOwner = ResourceBundle.getBundle("configPayBonus").getString("comissionForOwner");
        connectSystemFail = ResourceBundle.getBundle("configPayBonus").getString("connectSystemFail");

        reasonIdNotFound = ResourceBundle.getBundle("configPayBonus").getString("reasonIdNotFound");
        saleServiceCodeNotFound = ResourceBundle.getBundle("configPayBonus").getString("saleServiceCodeNotFound");
        saleServicePriceNotFound = ResourceBundle.getBundle("configPayBonus").getString("saleServicePriceNotFound");
        priceIsdnNotFound = ResourceBundle.getBundle("configPayBonus").getString("priceIsdnNotFound");
        transactionError = ResourceBundle.getBundle("configPayBonus").getString("transactionError");
        productCodeNotFound = ResourceBundle.getBundle("configPayBonus").getString("productCodeNotFound");
        specialProductFreeIsdn = ResourceBundle.getBundle("configPayBonus").getString("specialProductFreeIsdn");
        kitBonusListTaskForce = ResourceBundle.getBundle("configPayBonus").getString("kitBonusListTaskForce");
        listTaskForce = kitBonusListTaskForce.split("\\|");
        connectKitInvalidProfile = ResourceBundle.getBundle("configPayBonus").getString("connectKitInvalidProfile");
        vasCodeAmountMoney = ResourceBundle.getBundle("configPayBonus").getString("vasCodeAmountMoney");
        arrVasCodeAmountMoney = vasCodeAmountMoney.split("\\|");
        agentEmola = ResourceBundle.getBundle("configPayBonus").getString("KitConnectSpecialAgentEmola");
        arrAgentEmola = agentEmola.split("\\|");
        vasCodeAmountRate = ResourceBundle.getBundle("configPayBonus").getString("vasCodeAmountRate");
        pricePlanForBranch = ResourceBundle.getBundle("configPayBonus").getString("pricePlanForBranch");
        schemaDbForBranch = ResourceBundle.getBundle("configPayBonus").getString("schemaDbForBranch");
        schemaDbForFbPackageBranch = ResourceBundle.getBundle("configPayBonus").getString("schemaDbForFbPackageBranch");

        vasCodeConnection = ResourceBundle.getBundle("configPayBonus").getString("vasCodeConnection");
        arrVasCodeConnection = vasCodeConnection.split("\\|");
        lstMapVasCodeConnection = new ArrayList<HashMap>();
        for (String vasConn : arrVasCodeConnection) {
            String[] arrConn = vasConn.split("\\:");
            HashMap map = new HashMap();
            map.put(arrConn[0].trim().toUpperCase(), arrConn[1].trim());
            lstMapVasCodeConnection.add(map);
        }

        vasCodeParam = ResourceBundle.getBundle("configPayBonus").getString("vasCodeParam");
        arrVasCodeParam = vasCodeParam.split("\\|");
        lstMapVasCodeParam = new ArrayList<HashMap>();
        for (String vasConn : arrVasCodeParam) {
            String[] arrConn = vasConn.split("\\:");
            HashMap map = new HashMap();
            map.put(arrConn[0].trim().toUpperCase(), arrConn[1].trim());
            lstMapVasCodeParam.add(map);
        }

        vasCodeActionType = ResourceBundle.getBundle("configPayBonus").getString("vasCodeActionType");
        arrVasCodeActionType = vasCodeActionType.split("\\|");
        lstMapVasCodeActionType = new ArrayList<HashMap>();
        for (String vasConn : arrVasCodeActionType) {
            String[] arrConn = vasConn.split("\\:");
            HashMap map = new HashMap();
            map.put(arrConn[0].trim().toUpperCase(), arrConn[1].trim());
            lstMapVasCodeActionType.add(map);
        }

        vasCodeChannel = ResourceBundle.getBundle("configPayBonus").getString("vasCodeChannel");
        arrVasCodeChannel = vasCodeChannel.split("\\|");
        lstMapVasCodeChannel = new ArrayList<HashMap>();
        for (String vasConn : arrVasCodeChannel) {
            String[] arrConn = vasConn.split("\\:");
            HashMap map = new HashMap();
            map.put(arrConn[0].trim().toUpperCase(), arrConn[1].trim());
            lstMapVasCodeChannel.add(map);
        }

        lstMapVasCodeAmount = new ArrayList<HashMap>();
        for (String vasAmount : arrVasCodeAmountMoney) {
            String[] tmp = vasAmount.split("\\:");
            HashMap map = new HashMap();
            map.put(tmp[0].trim().toUpperCase(), tmp[1].trim());
            lstMapVasCodeAmount.add(map);
        }

        lstMapAgentEmola = new ArrayList<HashMap>();
        for (String vasAmount : arrAgentEmola) {
            String[] tmp = vasAmount.split("\\:");
            HashMap map = new HashMap();
            map.put(tmp[0].trim().toUpperCase(), tmp[1].trim());
            lstMapAgentEmola.add(map);
        }

        eliteMsgProductMap = ResourceBundle.getBundle("configPayBonus").getString("eliteMsgProductMap");
        arrEliteMsgProductMap = eliteMsgProductMap.split("\\|");
        lstEliteMsgProductMap = new ArrayList<HashMap>();
        for (String tmp : arrEliteMsgProductMap) {
            String[] arrTmp = tmp.split("\\:");
            HashMap map = new HashMap();
            map.put(arrTmp[0].trim(), arrTmp[1].trim());
            lstEliteMsgProductMap.add(map);
        }
        kitConnectBonusMoneyPrepaidMonth = ResourceBundle.getBundle("configPayBonus").getString("kitConnectBonusMoneyPrepaidMonth");
        arrBonusMoneyPrepaidMonth = kitConnectBonusMoneyPrepaidMonth.split("\\|");
        kitConnectUpdateTransCodeFail = ResourceBundle.getBundle("configPayBonus").getString("kitConnectUpdateTransCodeFail");
        bonusRateForPrepaidMonth = ResourceBundle.getBundle("configPayBonus").getString("bonusRateForPrepaidMonth");
        kitConnectHandsetNotFound = ResourceBundle.getBundle("configPayBonus").getString("kitConnectHandsetNotFound");
        kitConnectPriceHandsetNotFound = ResourceBundle.getBundle("configPayBonus").getString("kitConnectPriceHandsetNotFound");
        kitConnectSerialHandsetNotFound = ResourceBundle.getBundle("configPayBonus").getString("kitConnectSerialHandsetNotFound");
        kitConnectSmsSaleHandsetManager = ResourceBundle.getBundle("configPayBonus").getString("kitConnectSmsSaleHandsetManager");
        bonus4GMsgConnNew = ResourceBundle.getBundle("configPayBonus").getString("bonus4GMsgConnNew");

    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        for (Record record : listRecord) {
            Bonus moRecord = (Bonus) record;
            moRecord.setNodeName(holder.getNodeName());
            moRecord.setClusterName(holder.getClusterName());
        }
        return listRecord;
    }

    @Override
    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
        List<Record> listResult = new ArrayList<Record>();
        String staffCode;
        long timeSt;
        String productCode;
        String openFlagMODIGPRSResult;
        String orgRequestId;
        SimpleDateFormat sdf = new SimpleDateFormat("ddMMyyyyHHmmssSSS");
        Calendar cal;
        boolean isSim4G;
        String tmpProduct;
        boolean isPrepaidMonth;
        double bonusForCustomer;
//        Long priceOfServices;
        Long priceOfProduct;
        double totalCommissionForPrepaidMonth;
        Double priceOfHandset;
//        double discountHandset;
        Price priceOfHandsetObj;
        String smsSaleHandsetForManager;
        String isdnOwnerOfChannel;

        for (Record record : listRecord) {
            timeSt = System.currentTimeMillis();
            productCode = "";
            staffCode = "";
            orgRequestId = "";
            openFlagMODIGPRSResult = "";
            isSim4G = false;
            tmpProduct = "";
            isPrepaidMonth = false;
            Bonus bn = (Bonus) record;
            bonusForCustomer = 0;
//            priceOfServices = 0L;
            priceOfProduct = 0L;
            priceOfHandset = 0.0;
//            discountHandset = 0;
            totalCommissionForPrepaidMonth = 0;
            priceOfHandsetObj = null;
            smsSaleHandsetForManager = "";
            isdnOwnerOfChannel = "";
            listResult.add(bn);
            if ("0".equals(bn.getResultCode())) {
                if (bn.getPrepaidMonth() == 0) {
                    bn.setPrepaidMonth(1);
                }
                if (bn.getPrepaidMonth() > 1) {
                    bn.setPrepaidMonth(bn.getPrepaidMonth() - 1);//Because PrePaidMonth = Current Month + Prepaid Month
                    isPrepaidMonth = true;
                }
                staffCode = bn.getUserName();
                if ("".equals(staffCode)) {
                    logger.warn("CreateStaff in Sub_Profile_Info is null or empty, id " + bn.getId());
                    bn.setResultCode("01");
                    bn.setDescription("CreateStaff in Sub_Profile_Info is null or empty");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    db.deleteSubProfile(bn.getId(), bn.getIsdnCustomer());
                    continue;
                } else {
                    bn.setStaffCode(staffCode);
                }
                if (bn.getIsdnCustomer() == null || bn.getIsdnCustomer().trim().length() <= 0) {
                    logger.warn("ISDN is null or empty, id " + bn.getId());
                    bn.setResultCode("02");
                    bn.setDescription("ISDN is null or empty");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    db.deleteSubProfile(bn.getId(), bn.getIsdnCustomer());
                    continue;
                }
                if (bn.getSimSerial() == null || bn.getSimSerial().trim().length() <= 0) {
                    logger.warn("SimSerial is null or empty, id " + bn.getId());
                    bn.setResultCode("05");
                    bn.setDescription("SimSerial is null or empty");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    db.deleteSubProfile(bn.getId(), bn.getIsdnCustomer());
                    continue;
                }
//                Get and check ProductCode mapping
                productCode = db.getProductCode(bn.getSimSerial(), bn.getIsdnCustomer());
                if (productCode == null || productCode.trim().length() <= 0) {
                    int rollbackSubMb = db.destroySubMbRecord(3L, bn.getSimSerial(), bn.getIsdnCustomer());
                    int rollbackIsdn = db.updateStockIsdn(1L, "SYSTEM_AUTO", bn.getIsdnCustomer());
                    int rollbackSim = db.updateStockSim(0L, 1L, bn.getSimSerial(), bn.getIsdnCustomer());
                    int rollbackSubIdNo = db.updateSubIdNo(bn.getCustId(), bn.getIsdnCustomer());
                    db.deleteSubProfile(bn.getId(), bn.getIsdnCustomer());
                    logger.warn("Result rollback: SubMb " + rollbackSubMb + " stockIsdnMobile: "
                            + rollbackIsdn + " stockSim: " + rollbackSim + " subIdNo: " + rollbackSubIdNo
                            + " for serial: " + bn.getSimSerial() + " isdn " + bn.getIsdnCustomer());
                    String tel = db.getTelByStaffCode(staffCode);
                    String message = productCodeNotFound;
                    db.sendSms(tel, message, "86142");
                    bn.setResultCode("02");
                    bn.setDescription("productCode is null in sub_mb table");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    continue;
                }
                bn.setProductCode(productCode);
                for (int i = 0; i < lstEliteMsgProductMap.size(); i++) {
                    if (lstEliteMsgProductMap.get(i).containsKey(productCode)) {
                        tmpProduct = lstEliteMsgProductMap.get(i).get(productCode).toString();
                        logger.info("Product will be replace: " + tmpProduct + ", newProduct: " + productCode);
                        break;
                    }
                }
                if (tmpProduct.isEmpty()) {
                    tmpProduct = productCode;
                }
                //LinhNBV start modified on June 06 2018: Connect if serial is SIM.                
                //Tinh phi dau noi + phi so >>> Tru tien eMola
                Long totalAmount = 0L;
                priceOfProduct = db.getPriceProductConnectKit(productCode);
                if (priceOfProduct == null) {
                    int rollbackSubMb = db.destroySubMbRecord(3L, bn.getSimSerial(), bn.getIsdnCustomer());
                    int rollbackIsdn = db.updateStockIsdn(1L, "SYSTEM_AUTO", bn.getIsdnCustomer());
                    int rollbackSim = db.updateStockSim(0L, 1L, bn.getSimSerial(), bn.getIsdnCustomer());
                    int rollbackSubIdNo = db.updateSubIdNo(bn.getCustId(), bn.getIsdnCustomer());
                    db.deleteSubProfile(bn.getId(), bn.getIsdnCustomer());
                    logger.warn("Can not find priceProduct, Result rollback: SubMb " + rollbackSubMb + " stockIsdnMobile: " + rollbackIsdn
                            + " stockSim: " + rollbackSim + " subIdNo: " + rollbackSubIdNo + " for serial: " + bn.getSimSerial()
                            + " isdn " + bn.getIsdnCustomer() + ", productCode: " + productCode);
                    String tel = db.getTelByStaffCode(staffCode);
                    db.sendSms(tel, saleServicePriceNotFound, "86142");
                    bn.setResultCode("23");
                    bn.setDescription("Can not find priceProduct");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    continue;
                }
                if (isPrepaidMonth) {
//                    priceOfServices = saleServicesPrice.getPrice();
                    double rateBonusPrepaidMonth = 0;
                    try {
                        rateBonusPrepaidMonth = Double.parseDouble(bonusRateForPrepaidMonth);
                    } catch (NumberFormatException ex) {
                        ex.printStackTrace();
                        rateBonusPrepaidMonth = 0;
                    }
                    totalCommissionForPrepaidMonth = rateBonusPrepaidMonth * priceOfProduct * bn.getPrepaidMonth();
                    logger.info("Subscriber have value prepaidMonth: " + bn.getPrepaidMonth() + ", bonus more for prepaid month, " + totalCommissionForPrepaidMonth
                            + "isdn: " + bn.getIsdnCustomer() + ", staffCode: " + staffCode);
                    //If prepaidMonth >> saleServicesPrice = price * (prepaidMonth + currentMonth)
//                    priceOfServices = priceOfServices * (bn.getPrepaidMonth() + 1);
                    totalAmount = totalAmount + priceOfProduct * (bn.getPrepaidMonth() + 1);
                    logger.info("Subscriber have prepaidMonth value: " + bn.getPrepaidMonth() + ", newSaleServicesPrice: "
                            + (priceOfProduct * (bn.getPrepaidMonth() + 1)) + ", isdn:  " + bn.getIsdnCustomer());
                } else {
                    totalAmount = totalAmount + priceOfProduct;
                }

                Long priceVasCode = 0L;
                if (bn.getVasCode() != null && !bn.getVasCode().isEmpty()) {
                    logger.info("start get price of add-on vasCode: " + bn.getVasCode() + ", isdn: " + bn.getIsdnCustomer());
                    priceVasCode = db.getPriceProductConnectKit(bn.getVasCode());
                    if (priceVasCode == null) {
                        int rollbackSubMb = db.destroySubMbRecord(3L, bn.getSimSerial(), bn.getIsdnCustomer());
                        int rollbackIsdn = db.updateStockIsdn(1L, "SYSTEM_AUTO", bn.getIsdnCustomer());
                        int rollbackSim = db.updateStockSim(0L, 1L, bn.getSimSerial(), bn.getIsdnCustomer());
                        int rollbackSubIdNo = db.updateSubIdNo(bn.getCustId(), bn.getIsdnCustomer());
                        db.deleteSubProfile(bn.getId(), bn.getIsdnCustomer());
                        logger.warn("Can not find priceProduct, Result rollback: SubMb " + rollbackSubMb + " stockIsdnMobile: " + rollbackIsdn
                                + " stockSim: " + rollbackSim + " subIdNo: " + rollbackSubIdNo + " for serial: " + bn.getSimSerial()
                                + " isdn " + bn.getIsdnCustomer() + ", vasCode: " + bn.getVasCode());
                        String tel = db.getTelByStaffCode(staffCode);
                        db.sendSms(tel, saleServicePriceNotFound, "86142");
                        bn.setResultCode("23");
                        bn.setDescription("Can not find price of add-on");
                        bn.setDuration(System.currentTimeMillis() - timeSt);
                        continue;
                    }
                }
                totalAmount = totalAmount + priceVasCode;
                if (bn.getMainPrice() != null && !bn.getMainPrice().isEmpty()) {
                    long moneyValue = db.getMoneyValueBasedOnMainPrice(bn.getMainProduct(), bn.getMainPrice());
                    totalAmount = totalAmount + moneyValue;
                }
                Long reasonId = 201007L;//EMOLA_CONNECT_NEW 20191227 don't need get reasonId by productCode, now fix a reason
                //Get Price service of Isdn
                Price priceOfIsdn = db.getPriceIsdnToConnect(bn.getIsdnCustomer(), productCode);
                if (priceOfIsdn == null) {
                    int rollbackSubMb = db.destroySubMbRecord(3L, bn.getSimSerial(), bn.getIsdnCustomer());
                    int rollbackIsdn = db.updateStockIsdn(1L, "SYSTEM_AUTO", bn.getIsdnCustomer());
                    int rollbackSim = db.updateStockSim(0L, 1L, bn.getSimSerial(), bn.getIsdnCustomer());
                    int rollbackSubIdNo = db.updateSubIdNo(bn.getCustId(), bn.getIsdnCustomer());
                    db.deleteSubProfile(bn.getId(), bn.getIsdnCustomer());
                    //Update status sub_id_no ve 0
                    logger.warn("Can not find price, Result rollback: SubMb " + rollbackSubMb + " stockIsdnMobile: " + rollbackIsdn
                            + " stockSim: " + rollbackSim + " subIdNo: " + rollbackSubIdNo + " for serial: " + bn.getSimSerial()
                            + " isdn " + bn.getIsdnCustomer());
                    String tel = db.getTelByStaffCode(staffCode);
                    db.sendSms(tel, priceIsdnNotFound, "86142");
                    bn.setResultCode("27");
                    bn.setDescription("Can not find price of isdn");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    continue;
                }
                Double priceIsdn = 0.0;
                Double discountAmount = 0.0;
                if (priceOfIsdn.getPrice() > priceOfProduct) {
                    priceIsdn = priceOfIsdn.getPrice();
                } else {
                    priceIsdn = 0.0;
                    discountAmount = priceOfIsdn.getPrice();
                }
                logger.info("Price of Isdn: " + priceIsdn + ", isdn: " + bn.getIsdnCustomer() + ", priceOfProduct: " + priceOfProduct);
                totalAmount = totalAmount + priceIsdn.longValue();
                logger.info("total amount  will be make revenue and charge: " + totalAmount + " includeing: priceProduct: " + priceOfProduct + ", priceVasCode: " + priceVasCode + ", mainPrice: " + bn.getMainPrice()
                        + ", product: " + productCode + ", vasCode: " + bn.getVasCode() + ", isdn: " + bn.getIsdnCustomer()
                        + ", priceOfIsdn: " + priceOfIsdn);
                //get price of handset...
                if (bn.getHandsetModel() != null && !bn.getHandsetModel().isEmpty()) {
                    String mainProductConnect = "";
                    if (bn.getMainProduct() != null && !bn.getMainProduct().isEmpty()) {
                        mainProductConnect = db.getMainProductConnectKit(bn.getMainProduct());
                        if (mainProductConnect.isEmpty()) {
                            int rollbackSubMb = db.destroySubMbRecord(3L, bn.getSimSerial(), bn.getIsdnCustomer());
                            int rollbackIsdn = db.updateStockIsdn(1L, "SYSTEM_AUTO", bn.getIsdnCustomer());
                            int rollbackSim = db.updateStockSim(0L, 1L, bn.getSimSerial(), bn.getIsdnCustomer());
                            int rollbackSubIdNo = db.updateSubIdNo(bn.getCustId(), bn.getIsdnCustomer());
                            db.deleteSubProfile(bn.getId(), bn.getIsdnCustomer());
                            logger.warn("Cannot get mainProduct, result rollback: SubMb " + rollbackSubMb + " stockIsdnMobile: "
                                    + rollbackIsdn + " stockSim: " + rollbackSim + " subIdNo: " + rollbackSubIdNo
                                    + " for serial: " + bn.getSimSerial() + " isdn " + bn.getIsdnCustomer());
                            String tel = db.getTelByStaffCode(staffCode);
                            String message = productCodeNotFound;
                            db.sendSms(tel, message, "86142");
                            bn.setResultCode("40");
                            bn.setDescription("Cannot get mainProduct");
                            bn.setDuration(System.currentTimeMillis() - timeSt);
                            continue;
                        }
                    }
                    logger.info("start calculate price of handset with discount, no need to pay bonus for customer: "
                            + bn.getHandsetModel() + ", sub:  " + bn.getIsdnCustomer());
                    String[] arrHandsetInfo = bn.getHandsetModel().split("\\|");
                    String stockModelCode = arrHandsetInfo[0].trim();
                    String priceType = arrHandsetInfo[1].trim();
                    String shopPath = db.getShopPathByStaffCode(staffCode);
                    String[] arrShopPath = shopPath.split("\\_");//_7282_shopConfig_shopChildren
                    String shopId = "";
                    if (arrShopPath.length > 2) {
                        shopId = arrShopPath[2];
                    } else {
                        shopId = arrShopPath[1];
                    }
                    String pricePolicy = db.getPricePolicyHandset(mainProductConnect, stockModelCode, shopId);
//                        Step 1: Get information of stockModel by stockModelId
                    Long stockModelId = db.getStockModelId(stockModelCode);
                    StockModel stockModel = db.findStockModelById(stockModelId);
                    if (stockModel != null) {
//                            Step 2: find price for sale (saleRetail)
//                        Long pricePolicy = db.getPricePolicyByStaffCode(staffCode);
                        String priceTypeConfig = db.getBasedConfigConnectKit(staffCode, "revenue_type_handset");
                        String[] arrPriceTypeConfig = priceTypeConfig.split("\\|");
                        for (String tmp : arrPriceTypeConfig) {
                            String[] arrTmp = tmp.split("\\-");
                            if (arrTmp[1].trim().equals(mainProductConnect) && "1".equals(arrTmp[0].trim())) {
                                logger.info("Main product: " + mainProductConnect + ", Price and discount is Special, "
                                        + "firstly to assign price to retail price, isdn: " + bn.getIsdnCustomer());
                                priceType = "1";
                                pricePolicy = "1";
                                break;
                            }
                        }
                        priceOfHandsetObj = db.getPriceByStockModelCode(stockModelCode, priceType, pricePolicy);
                        if (priceOfHandsetObj != null) {
                            if (!"1".equals(priceType)) {
                                logger.info("Follow promotion price, price of handset is promotion: " + priceOfHandsetObj.getPrice() + ", priceType: " + priceType + ", pricePolicy: " + pricePolicy
                                        + bn.getHandsetModel() + ", discount handset will pay after check profile, isdn: " + bn.getIsdnCustomer());
                            } else {
                                logger.info("Follow special price, price of handset is retail: " + priceOfHandsetObj.getPrice() + ", priceType: " + priceType + ", pricePolicy: " + pricePolicy
                                        + bn.getHandsetModel() + ", discount handset will pay after check profile, isdn: " + bn.getIsdnCustomer());
                            }
//                            double discountAmountHandset = db.getDiscountForHandset(productCode);
                            priceOfHandset = priceOfHandsetObj.getPrice();// - discountHandset;
                            logger.info("Price of Handset " + stockModelCode + ":" + priceOfHandset + ", handsetModel: "
                                    + bn.getHandsetModel() + ", sub:  " + bn.getIsdnCustomer());
//                            Get serial of handset
                            String handsetSerial = db.getSerialOfHandset(stockModelCode, staffCode);
                            if (handsetSerial.isEmpty()) {
                                logger.info("cannot find serial of handset: "
                                        + stockModelCode + ", of staff: " + staffCode + ", sub:  " + bn.getIsdnCustomer());
                                for (String isdn : arrIsdnReceiveError) {
                                    db.sendSms(isdn, "cannot find serial handset: " + stockModelCode + ", of staff: " + staffCode
                                            + ", isdnCustomer: " + bn.getIsdnCustomer(), "86142");
                                }
                                int rollbackSubMb = db.destroySubMbRecord(3L, bn.getSimSerial(), bn.getIsdnCustomer());
                                int rollbackIsdn = db.updateStockIsdn(1L, "SYSTEM_AUTO", bn.getIsdnCustomer());
                                int rollbackSim = db.updateStockSim(0L, 1L, bn.getSimSerial(), bn.getIsdnCustomer());
                                int rollbackSubIdNo = db.updateSubIdNo(bn.getCustId(), bn.getIsdnCustomer());
                                db.deleteSubProfile(bn.getId(), bn.getIsdnCustomer());
                                //Update status sub_id_no ve 0
                                logger.warn("Can not find serial of handset, Result rollback: SubMb " + rollbackSubMb + " stockIsdnMobile: " + rollbackIsdn
                                        + " stockSim: " + rollbackSim + " subIdNo: " + rollbackSubIdNo + " for serial: " + bn.getSimSerial()
                                        + " isdn " + bn.getIsdnCustomer());
                                String tel = db.getTelByStaffCode(staffCode);
                                db.sendSms(tel, kitConnectSerialHandsetNotFound, "86142");
                                bn.setResultCode("39");
                                bn.setDescription("Cannot find serial of handset");
                                bn.setDuration(System.currentTimeMillis() - timeSt);
                                continue;
                            }
                            bn.setHandsetSerial(handsetSerial);
                        } else {
                            logger.info("cannot find price for saleRetail by serial: "
                                    + bn.getHandsetModel() + ", sub:  " + bn.getIsdnCustomer());
                            for (String isdn : arrIsdnReceiveError) {
                                db.sendSms(isdn, "cannot find price for saleRetail, serialHanset: " + bn.getHandsetModel()
                                        + ", isdnCustomer: " + bn.getIsdnCustomer(), "86142");
                            }
                            int rollbackSubMb = db.destroySubMbRecord(3L, bn.getSimSerial(), bn.getIsdnCustomer());
                            int rollbackIsdn = db.updateStockIsdn(1L, "SYSTEM_AUTO", bn.getIsdnCustomer());
                            int rollbackSim = db.updateStockSim(0L, 1L, bn.getSimSerial(), bn.getIsdnCustomer());
                            int rollbackSubIdNo = db.updateSubIdNo(bn.getCustId(), bn.getIsdnCustomer());
                            db.deleteSubProfile(bn.getId(), bn.getIsdnCustomer());
                            //Update status sub_id_no ve 0
                            logger.warn("Can not find price, Result rollback: SubMb " + rollbackSubMb + " stockIsdnMobile: " + rollbackIsdn
                                    + " stockSim: " + rollbackSim + " subIdNo: " + rollbackSubIdNo + " for serial: " + bn.getSimSerial()
                                    + " isdn " + bn.getIsdnCustomer());
                            String tel = db.getTelByStaffCode(staffCode);
                            db.sendSms(tel, kitConnectPriceHandsetNotFound, "86142");
                            bn.setResultCode("38");
                            bn.setDescription("Cannot find stockModel by stockModelCode");
                            bn.setDuration(System.currentTimeMillis() - timeSt);
                            continue;
                        }
                    } else {
                        logger.info("cannot find stockModel by stockModelCode: "
                                + stockModelCode + ", sub:  " + bn.getIsdnCustomer());
                        for (String isdn : arrIsdnReceiveError) {
                            db.sendSms(isdn, "Cannot find stockModel by stockModelCode: " + stockModelCode
                                    + ", isdnCustomer: " + bn.getIsdnCustomer(), "86142");
                        }
                        int rollbackSubMb = db.destroySubMbRecord(3L, bn.getSimSerial(), bn.getIsdnCustomer());
                        int rollbackIsdn = db.updateStockIsdn(1L, "SYSTEM_AUTO", bn.getIsdnCustomer());
                        int rollbackSim = db.updateStockSim(0L, 1L, bn.getSimSerial(), bn.getIsdnCustomer());
                        int rollbackSubIdNo = db.updateSubIdNo(bn.getCustId(), bn.getIsdnCustomer());
                        db.deleteSubProfile(bn.getId(), bn.getIsdnCustomer());
                        //Update status sub_id_no ve 0
                        logger.warn("Can not find price, Result rollback: SubMb " + rollbackSubMb + " stockIsdnMobile: " + rollbackIsdn
                                + " stockSim: " + rollbackSim + " subIdNo: " + rollbackSubIdNo + " for serial: " + bn.getSimSerial()
                                + " isdn " + bn.getIsdnCustomer());
                        String tel = db.getTelByStaffCode(staffCode);
                        db.sendSms(tel, kitConnectHandsetNotFound, "86142");
                        bn.setResultCode("37");
                        bn.setDescription("Cannot find stockModel by stockModelCode");
                        bn.setDuration(System.currentTimeMillis() - timeSt);
                        continue;
                    }
                    String staffOwnerCode = db.getOwnerStaffOfChannel(staffCode);
                    if (staffOwnerCode != null && !staffOwnerCode.isEmpty()) {
                        isdnOwnerOfChannel = db.getTelByStaffCode(staffOwnerCode);
                        logger.info("start send sms for manager of channel: " + staffCode + ", owner: " + staffOwnerCode
                                + bn.getHandsetModel() + ", isdn:  " + bn.getIsdnCustomer());
                        smsSaleHandsetForManager = kitConnectSmsSaleHandsetManager.replace("XXX", staffCode)
                                .replace("YYY", stockModelCode).replace("ZZZ", mainProductConnect);
                        if (bn.getVasCode() != null && !bn.getVasCode().isEmpty()) {
                            smsSaleHandsetForManager = smsSaleHandsetForManager.replace("WWW", bn.getVasCode().trim());
                        } else {
                            smsSaleHandsetForManager = smsSaleHandsetForManager.replace("WWW", productCode.trim());
                        }
                        long remainHandset = db.getRemainHandsetInStock(stockModelCode, staffOwnerCode);
                        smsSaleHandsetForManager = smsSaleHandsetForManager.replace("TTT", String.valueOf(remainHandset));
                    }
                }
                //Payment voucher via eMola.
                if ((totalAmount + priceOfHandset) > 0) {
                    if (bn.getPayMethod() == 0 && bn.getBankName() != null && bn.getBankName().length() > 0
                            && bn.getBankTranAmount() != null && bn.getBankTranAmount().length() > 0
                            && bn.getBankTranCode() != null && bn.getBankTranCode().length() > 0) {
                        logger.info("start update transcode: " + bn.getBankTranCode() + ", bankName: " + bn.getBankName() + ", amount: "
                                + bn.getBankTranAmount() + ", isdn " + bn.getIsdnCustomer());
                        if (Double.parseDouble(bn.getBankTranAmount()) >= totalAmount + priceOfHandset) {
                            String checkTrans = BankTransferUtils.callWSSOAPupdateTrans(bn.getBankTranCode(), bn.getBankName(), bn.getBankTranAmount(),
                                    staffCode, staffCode);
                            if ("00".equalsIgnoreCase(checkTrans)) {
                                logger.info("Update TransCode successfully, transcode: " + bn.getBankTranCode() + ", bankName: " + bn.getBankName() + ", amount: "
                                        + bn.getBankTranAmount() + ", isdn " + bn.getIsdnCustomer());
                            } else {
                                logger.info("Update TransCode unsuccessfully, start rollback...., transcode: " + bn.getBankTranCode() + ", bankName: " + bn.getBankName() + ", amount: "
                                        + bn.getBankTranAmount() + ", isdn " + bn.getIsdnCustomer());
                                int rollbackSubMb = db.destroySubMbRecord(3L, bn.getSimSerial(), bn.getIsdnCustomer());
                                int rollbackIsdn = db.updateStockIsdn(1L, "SYSTEM_AUTO", bn.getIsdnCustomer());
                                int rollbackSim = db.updateStockSim(0L, 1L, bn.getSimSerial(), bn.getIsdnCustomer());
                                int rollbackSubIdNo = db.updateSubIdNo(bn.getCustId(), bn.getIsdnCustomer());
                                db.deleteSubProfile(bn.getId(), bn.getIsdnCustomer());
                                //rollbackTrans
                                BankTransferUtils.callWSSOAProllbackTrans(bn.getBankTranCode(), bn.getBankName(),
                                        bn.getBankTranAmount(), staffCode, staffCode);
                                //Update status sub_id_no ve 0
                                logger.warn("Fail to update transCode, Result rollback: SubMb " + rollbackSubMb + " stockIsdnMobile: " + rollbackIsdn
                                        + " stockSim: " + rollbackSim + " subIdNo: " + rollbackSubIdNo + " for serial: " + bn.getSimSerial()
                                        + " isdn " + bn.getIsdnCustomer());
                                String tel = db.getTelByStaffCode(staffCode);
                                db.sendSms(tel, kitConnectUpdateTransCodeFail, "86142");
                                bn.setResultCode("32");
                                bn.setDescription("Fail to update TransCode");
                                bn.setDuration(System.currentTimeMillis() - timeSt);
                                continue;
                            }
                        } else {
                            logger.info("Total amount (include handset): " + totalAmount + ", less than bankTransAmount: " + bn.getBankTranAmount()
                                    + ", isdn " + bn.getIsdnCustomer());
                            int rollbackSubMb = db.destroySubMbRecord(3L, bn.getSimSerial(), bn.getIsdnCustomer());
                            int rollbackIsdn = db.updateStockIsdn(1L, "SYSTEM_AUTO", bn.getIsdnCustomer());
                            int rollbackSim = db.updateStockSim(0L, 1L, bn.getSimSerial(), bn.getIsdnCustomer());
                            int rollbackSubIdNo = db.updateSubIdNo(bn.getCustId(), bn.getIsdnCustomer());
                            db.deleteSubProfile(bn.getId(), bn.getIsdnCustomer());
                            //rollbackTrans
                            BankTransferUtils.callWSSOAProllbackTrans(bn.getBankTranCode(), bn.getBankName(),
                                    bn.getBankTranAmount(), staffCode, staffCode);
                            //Update status sub_id_no ve 0
                            logger.warn("BankTransAmount less than totalAmount, Result rollback: SubMb " + rollbackSubMb + " stockIsdnMobile: " + rollbackIsdn
                                    + " stockSim: " + rollbackSim + " subIdNo: " + rollbackSubIdNo + " for serial: " + bn.getSimSerial()
                                    + " isdn " + bn.getIsdnCustomer());
                            String tel = db.getTelByStaffCode(staffCode);
                            db.sendSms(tel, kitConnectUpdateTransCodeFail, "86142");
                            bn.setResultCode("35");
                            bn.setDescription("BankTransAmount less than totalAmount.");
                            bn.setDuration(System.currentTimeMillis() - timeSt);
                            continue;
                        }
                    } else if (bn.getPayMethod() == 2) {
                        logger.info("staffCode: " + staffCode + " using POS to connect charge money, so no need charge money and clear debit (later), "
                                + "referenceId: " + bn.getReferenceId() + ", isdn " + bn.getIsdnCustomer());
                    } else {
                        logger.info("start call eMola to pay saleServices, isdn " + bn.getIsdnCustomer());
                        ResponseWallet responseWallet = null;
//                    20190103 check if user belong VIP_AGENT that mean staff_owner_id is S99_VIP_AGENT then not deduct eMola on user, money will be collect daily and deduct on enterprice account of Vip Agent
                        String vipAgentCode = db.getOwnerStaffOfChannel(staffCode);
                        String emolaEnterPriseAgent = "";
                        for (int i = 0; i < lstMapAgentEmola.size(); i++) {
                            if (lstMapAgentEmola.get(i).containsKey(vipAgentCode)) {
                                emolaEnterPriseAgent = lstMapAgentEmola.get(i).get(vipAgentCode).toString();
                                break;
                            }
                        }
                        if (emolaEnterPriseAgent != null && emolaEnterPriseAgent.trim().length() > 0) {
                            logger.info("Staff " + staffCode + " belong KitConnectSpecialAgentEmola, so will deduct on enterprice emola account of Agent "
                                    + vipAgentCode + " sub " + bn.getIsdnCustomer() + ", emolaEnterPriseAgent: " + emolaEnterPriseAgent);
                            QueryInforResponse accountInfo = EWalletUtil.queryEnterpriseAccountInfo(emolaEnterPriseAgent);
                            if (accountInfo == null || !"1".equals(accountInfo.getStatus()) || !"CO".equals(accountInfo.getChannelCode())
                                    || accountInfo.getCustomerCode() == null || accountInfo.getProviderCode() == null
                                    || accountInfo.getCustomerCode().isEmpty() || accountInfo.getProviderCode().isEmpty()) {
                                logger.info("Invalid account enterprise " + bn.getKitBatchId() + " isdnEnterprise " + emolaEnterPriseAgent);
                                responseWallet = new ResponseWallet();
                                responseWallet.setResponseCode("00");
                                responseWallet.setResponseMessage("Invalid account enterprise " + emolaEnterPriseAgent);
                            } else {
                                responseWallet = EWalletUtil.chargeEmolaEnterpriseV2(sdf.format(new Date()) + bn.getKitBatchId(), db,
                                        emolaEnterPriseAgent, totalAmount * 1.00, logger, emolaEnterPriseAgent, staffCode, accountInfo.getProviderCode(), accountInfo.getCustomerCode());
                            }
//                            responseWallet = EWalletUtil.chargeEmolaEnterprise(sdf.format(new Date()) + bn.getIsdnCustomer(), db,
//                                    emolaEnterPriseAgent, totalAmount.doubleValue(), logger, bn.getIsdnCustomer(), staffCode);
                        } else {
                            responseWallet = EWalletUtil.paymentVoucher(db, bn, (totalAmount + priceOfHandset), logger, bn.getIsdnCustomer());
//                        for testing...
//                            responseWallet = new ResponseWallet();
//                            responseWallet.setResponseCode("01");
                        }
                        if (responseWallet == null || !"01".equals(responseWallet.getResponseCode())) {
                            //Pay not success >>> so we will revert all....
                            int rollbackSubMb = db.destroySubMbRecord(3L, bn.getSimSerial(), bn.getIsdnCustomer());
                            int rollbackIsdn = db.updateStockIsdn(1L, "SYSTEM_AUTO", bn.getIsdnCustomer());
                            int rollbackSim = db.updateStockSim(0L, 1L, bn.getSimSerial(), bn.getIsdnCustomer());
                            int rollbackSubIdNo = db.updateSubIdNo(bn.getCustId(), bn.getIsdnCustomer());
                            db.deleteSubProfile(bn.getId(), bn.getIsdnCustomer());
                            //Update status sub_id_no ve 0
                            logger.warn("Fail to charge Emola, Result rollback: SubMb " + rollbackSubMb + " stockIsdnMobile: " + rollbackIsdn
                                    + " stockSim: " + rollbackSim + " subIdNo: " + rollbackSubIdNo + " for serial: " + bn.getSimSerial()
                                    + " isdn " + bn.getIsdnCustomer());
                            String tel = db.getTelByStaffCode(staffCode);
                            db.sendSms(tel, transactionError, "86142");
                            bn.setResultCode("28");
                            bn.setDescription("Fail to charge Emola");
                            bn.setDuration(System.currentTimeMillis() - timeSt);
                            continue;
                        }
                        orgRequestId = responseWallet.getRequestId();//RequestId using when revertTransaction
                    }
                } else {
                    logger.info("Total money less than 0 so not charge " + totalAmount + " isdn " + bn.getIsdnCustomer());
                }
                //Pay success >>> active 
                //thanh cong >>> dau noi
                //Active on Provisioning
                String simInfo = db.getImsiEkiSimBySerial(bn.getSimSerial(), bn.getIsdnCustomer());
                if (simInfo != null && simInfo.length() > 0) {
                    String[] info = simInfo.split("\\|");
                    String mainProduct = db.getMainProduct(productCode, bn.getIsdnCustomer());
                    if (mainProduct == null) {
                        logger.error("Fail to getMainProduct " + bn.getIsdnCustomer());
                        int rollbackSubMb = db.destroySubMbRecord(3L, bn.getSimSerial(), bn.getIsdnCustomer());
                        int rollbackIsdn = db.updateStockIsdn(1L, "SYSTEM_AUTO", bn.getIsdnCustomer());
                        int rollbackSim = db.updateStockSim(0L, 1L, bn.getSimSerial(), bn.getIsdnCustomer());
                        int rollbackSubIdNo = db.updateSubIdNo(bn.getCustId(), bn.getIsdnCustomer());
                        db.deleteSubProfile(bn.getId(), bn.getIsdnCustomer());
                        //Update status sub_id_no ve 0
                        logger.warn("Can not find mainProduct, Result rollback: SubMb " + rollbackSubMb + " stockIsdnMobile: "
                                + rollbackIsdn + " stockSim: " + rollbackSim + " subIdNo: "
                                + rollbackSubIdNo + " for serial: " + bn.getSimSerial()
                                + " isdn " + bn.getIsdnCustomer());
                        //Revert money
                        if (bn.getPayMethod() == 1) {
                            EWalletUtil.revertTransaction(db, bn, logger, orgRequestId, bn.getIsdnCustomer());
                        }
                        String tel = db.getTelByStaffCode(staffCode);
                        db.sendSms(tel, msgRegSIMActiveFail.replace("%XXX%", bn.getSimSerial()), "86142");
                        bn.setResultCode("29");
                        bn.setDescription("Can not find price plan code to active on Provisioning.");
                        bn.setDuration(System.currentTimeMillis() - timeSt);
                        continue;
                    }
                    Long k4sNO = db.getK4SNO(info[0]);
                    if (k4sNO == null) {
                        logger.info("k4sNO is null so set default k4sNO = 2 " + bn.getIsdnCustomer());
                        k4sNO = 2L;
                    }
//                    LinhNBV 20190117: Check SIM 4G or 3G. StockModelId SIM 4G: 207607
                    isSim4G = db.isSim4G(bn.getSimSerial(), 207607L);
                    String checkKI = pro.checkKiSim("258" + bn.getIsdnCustomer(), info[0]);
                    //Error when query KI >>> KI not load...etc....
                    if (!"0".equals(checkKI)) {
                        if ("ERR3048".equals(checkKI)) {
                            //KI not load >>> Add KI
                            String addKI = pro.addKiSim("258" + bn.getIsdnCustomer(), info[1], info[0], k4sNO + "", isSim4G);
                            if (!"0".equals(addKI)) {
                                logger.error("Fail to connectKit --- add KI fail" + bn.getIsdnCustomer());
                                int rollbackSubMb = db.destroySubMbRecord(3L, bn.getSimSerial(), bn.getIsdnCustomer());
                                int rollbackIsdn = db.updateStockIsdn(1L, "SYSTEM_AUTO", bn.getIsdnCustomer());
                                int rollbackSim = db.updateStockSim(0L, 1L, bn.getSimSerial(), bn.getIsdnCustomer());
                                int rollbackSubIdNo = db.updateSubIdNo(bn.getCustId(), bn.getIsdnCustomer());
                                db.deleteSubProfile(bn.getId(), bn.getIsdnCustomer());
                                //Update status sub_id_no ve 0
                                logger.warn("Fail to connectKit --- add KI fail, Result rollback: SubMb " + rollbackSubMb + " stockIsdnMobile: "
                                        + rollbackIsdn + " stockSim: " + rollbackSim + " subIdNo: " + rollbackSubIdNo + " for serial: "
                                        + bn.getSimSerial() + " isdn " + bn.getIsdnCustomer());
                                //Revert money
                                if (bn.getPayMethod() == 1) {
                                    EWalletUtil.revertTransaction(db, bn, logger, orgRequestId, bn.getIsdnCustomer());
                                }
                                String tel = db.getTelByStaffCode(staffCode);
                                db.sendSms(tel, msgRegSIMActiveFail.replace("%XXX%", bn.getSimSerial()), "86142");
                                logger.info("Add KI to blank SIM fail.");
                                bn.setResultCode("34");
                                bn.setDescription("Add KI fail, errorCode: " + checkKI);
                                bn.setDuration(System.currentTimeMillis() - timeSt);
                                continue;
                            }
                        } else {
                            logger.error("Fail to connectKit ---- Error isn't KI not load" + bn.getIsdnCustomer());
                            int rollbackSubMb = db.destroySubMbRecord(3L, bn.getSimSerial(), bn.getIsdnCustomer());
                            int rollbackIsdn = db.updateStockIsdn(1L, "SYSTEM_AUTO", bn.getIsdnCustomer());
                            int rollbackSim = db.updateStockSim(0L, 1L, bn.getSimSerial(), bn.getIsdnCustomer());
                            int rollbackSubIdNo = db.updateSubIdNo(bn.getCustId(), bn.getIsdnCustomer());
                            db.deleteSubProfile(bn.getId(), bn.getIsdnCustomer());
                            //Update status sub_id_no ve 0
                            logger.warn("Fail to connectKit ---- Error not is KI not load, Result rollback: SubMb " + rollbackSubMb + " stockIsdnMobile: "
                                    + rollbackIsdn + " stockSim: " + rollbackSim + " subIdNo: " + rollbackSubIdNo + " for serial: "
                                    + bn.getSimSerial() + " isdn " + bn.getIsdnCustomer());
//                            //Revert money
                            if (bn.getPayMethod() == 1) {
                                EWalletUtil.revertTransaction(db, bn, logger, orgRequestId, bn.getIsdnCustomer());
                            }
                            String tel = db.getTelByStaffCode(staffCode);
                            db.sendSms(tel, msgRegSIMActiveFail.replace("%XXX%", bn.getSimSerial()), "86142");
                            logger.info("Query fail Error isn't KI not load.");
                            bn.setResultCode("33");
                            bn.setDescription("Query KI not success, errorCode: " + checkKI);
                            bn.setDuration(System.currentTimeMillis() - timeSt);
                            continue;
                        }
                    }
//                    LinhNBV 20190117: 
                    String tplId = db.getTPLID(bn.getIsdnCustomer(), isSim4G);
                    if (tplId == null) {
                        //Cannot find template id >>> cannot connect kit
                        logger.error("Fail to TPLID " + bn.getIsdnCustomer());
                        int rollbackSubMb = db.destroySubMbRecord(3L, bn.getSimSerial(), bn.getIsdnCustomer());
                        int rollbackIsdn = db.updateStockIsdn(1L, "SYSTEM_AUTO", bn.getIsdnCustomer());
                        int rollbackSim = db.updateStockSim(0L, 1L, bn.getSimSerial(), bn.getIsdnCustomer());
                        int rollbackSubIdNo = db.updateSubIdNo(bn.getCustId(), bn.getIsdnCustomer());
                        db.deleteSubProfile(bn.getId(), bn.getIsdnCustomer());
                        //Update status sub_id_no ve 0
                        logger.warn("Can not find TPLID, Result rollback: SubMb " + rollbackSubMb + " stockIsdnMobile: "
                                + rollbackIsdn + " stockSim: " + rollbackSim + " subIdNo: "
                                + rollbackSubIdNo + " for serial: " + bn.getSimSerial()
                                + " isdn " + bn.getIsdnCustomer());
                        //Revert money
                        if (bn.getPayMethod() == 1) {
                            EWalletUtil.revertTransaction(db, bn, logger, orgRequestId, bn.getIsdnCustomer());
                        }
                        String tel = db.getTelByStaffCode(staffCode);
                        db.sendSms(tel, msgRegSIMActiveFail.replace("%XXX%", bn.getSimSerial()), "86142");
                        bn.setResultCode("36");
                        bn.setDescription("Can not find TPLID to connect KIT.");
                        bn.setDuration(System.currentTimeMillis() - timeSt);
                        continue;
                    }
                    String resultActive = services.connectKit(mainProduct, productCode, "258" + bn.getIsdnCustomer(),
                            bn.getSimSerial(), info[0], info[1], db, bn.getShopCode(), bn.getUserName(),
                            String.valueOf(k4sNO), tplId);
                    //For testing...
                    //String resultActive = "0";
                    if (!"0".equals(resultActive)) {
                        logger.error("Fail to connectKit " + bn.getIsdnCustomer());
                        int rollbackSubMb = db.destroySubMbRecord(3L, bn.getSimSerial(), bn.getIsdnCustomer());
                        int rollbackIsdn = db.updateStockIsdn(1L, "SYSTEM_AUTO", bn.getIsdnCustomer());
                        int rollbackSim = db.updateStockSim(0L, 1L, bn.getSimSerial(), bn.getIsdnCustomer());
                        int rollbackSubIdNo = db.updateSubIdNo(bn.getCustId(), bn.getIsdnCustomer());
                        db.deleteSubProfile(bn.getId(), bn.getIsdnCustomer());
                        //Update status sub_id_no ve 0
                        logger.warn("Fail to connectKit, Result rollback: SubMb " + rollbackSubMb + " stockIsdnMobile: "
                                + rollbackIsdn + " stockSim: " + rollbackSim + " subIdNo: " + rollbackSubIdNo + " for serial: "
                                + bn.getSimSerial() + " isdn " + bn.getIsdnCustomer());
                        //Revert money
                        if (bn.getPayMethod() == 1) {
                            EWalletUtil.revertTransaction(db, bn, logger, orgRequestId, bn.getIsdnCustomer());
                        }
                        String tel = db.getTelByStaffCode(staffCode);
                        db.sendSms(tel, msgRegSIMActiveFail.replace("%XXX%", bn.getSimSerial()), "86142");
                        logger.info("Active on provisioning fail.");
                        bn.setResultCode("30");
                        bn.setDescription("Active fail, errorCode: " + resultActive);
                        bn.setDuration(System.currentTimeMillis() - timeSt);
                        continue;
                    } else {
                        db.sendSms(bn.getIsdnCustomer(), bonus4GMsgConnNew, "86142");
                        logger.info("Connect Kit successfully " + bn.getIsdnCustomer()
                                + " now save log, make sale trans, register Vas and paybonus");
                        bn.setResultCode("0");
                        bn.setDescription("Connect Kit successfully now save log, make sale trans, register Vas and paybonus");
                    }
//                    db.insertLogRegisterInfo(bn.getStaffCode(), bn.getIsdnCustomer());                    
                    if (db.checkVipProductConnectKit(productCode)) {
                        String strTplId3G, strTmpId4G;
                        if (priceOfProduct >= 1200) {
                            strTmpId4G = "1";
                            strTplId3G = "36";
                        } else if (priceOfProduct >= 500) {
                            strTmpId4G = "2";
                            strTplId3G = "37";
                        } else {
                            strTmpId4G = "3";
                            strTplId3G = "38";
                        }
//                        LinhNBV 20190617: Only call MODI_GPRS for VIP sim
                        if (!"38".equals(strTplId3G)) {
                            openFlagMODIGPRSResult = pro.activeFlagMODIGPRS("258" + bn.getIsdnCustomer(), strTplId3G);
                            if ("0".equals(openFlagMODIGPRSResult)) {
                                logger.info("Open flag MODI_GPRS successfully for sub when connect kit  " + bn.getIsdnCustomer());
                            } else {
                                logger.warn("Fail to open flag MODI_GPRS for sub when connect kit  " + bn.getIsdnCustomer());
                            }
                        } else {
                            logger.info("Template Id = 38, no need MODI GPRS, isdn: " + bn.getIsdnCustomer());
                        }

                        if (isSim4G && !"3".equals(strTmpId4G)) {
                            logger.info("productCode is Vip and is Sim 4G, start add command MOD_TPLOPTGPRS with tmpId = 1, isdn: " + bn.getIsdnCustomer());
//                            LinhNBV 20200403: Edit QoS for subscriber: >= 1200 - 1 | >= 500 - 2 | >= 200 - 3
                            String modTPLOPTGPRSResult = pro.modTPLOPTGPRS("258" + bn.getIsdnCustomer(), "TRUE", strTmpId4G, "TRUE");
                            if ("0".equals(modTPLOPTGPRSResult)) {
                                logger.info("Open flag MOD_TPLOPTGPRS successfully for sub when connect kit  " + bn.getIsdnCustomer());
                            } else {
                                logger.warn("Fail to open flag MOD_TPLOPTGPRS for sub when connect kit  " + bn.getIsdnCustomer());
                            }
                        } else {
                            logger.info("Subscriber is not sim 4G or strTmpId4G = 3, no need MOD_ TPLOPTGPRS, isdn: " + bn.getIsdnCustomer());
                        }
                        if (!"DATA_SIM".equals(productCode)) {
                            db.insertDataKitVas(bn.getIsdnCustomer(), bn.getSimSerial(), staffCode.toUpperCase(), productCode);
                        } else {
                            logger.warn("Product code:  " + productCode + " is not VIP. No need to insert kit_vas table. " + bn.getIsdnCustomer());
                        }
                    }
//                    Step 3: Chan BAOC, BAIC
                    blockOneWay(bn.getIsdnCustomer(), isSim4G);
                    //Update status SIM (SOLD,HLR), ISDN --> (USING))
                    db.updateStockIsdn(2L, "SYSTEM_AUTO", bn.getIsdnCustomer());
                    db.updateStockSim(2L, 2L, bn.getSimSerial(), bn.getIsdnCustomer());
                    //Save Log actionAudit
                    db.insertActionAudit(bn.getIsdnCustomer(), bn.getSimSerial(),
                            "Connect KIT success for " + bn.getIsdnCustomer() + " .", bn.getPkId());
                } else {
                    bn.setResultCode("31");
                    bn.setDescription("Cannot find imsi, eki from stock sim: " + bn.getSimSerial());
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    int rollbackSubMb = db.destroySubMbRecord(3L, bn.getSimSerial(), bn.getIsdnCustomer());
                    int rollbackIsdn = db.updateStockIsdn(1L, "SYSTEM_AUTO", bn.getIsdnCustomer());
                    int rollbackSim = db.updateStockSim(0L, 1L, bn.getSimSerial(), bn.getIsdnCustomer());
                    db.deleteSubProfile(bn.getId(), bn.getIsdnCustomer());
                    if (bn.getPayMethod() == 1) {
                        EWalletUtil.revertTransaction(db, bn, logger, orgRequestId, bn.getIsdnCustomer());
                    }
                    logger.warn("Cannot find imsi, Result rollback for can not find sim on stock_sim: SubMb" + rollbackSubMb
                            + " stockIsdnMobile: " + rollbackIsdn + " stockSim: "
                            + rollbackSim + " for serial: " + bn.getSimSerial()
                            + " isdn " + bn.getIsdnCustomer());
                    continue;
                }
                if (bn.getHandsetModel() != null && !bn.getHandsetModel().isEmpty()) {
                    logger.info("start sale handset with discount, no need to pay bonus for customer: "
                            + bn.getHandsetModel() + ", sub:  " + bn.getIsdnCustomer());
                    String[] arrHandsetInfo = bn.getHandsetModel().split("\\|");
                    String stockModelCode = arrHandsetInfo[0].trim();
//                    String priceType = arrHandsetInfo[1].trim();
//                        Step 1: Get information of stockModel by stockModelId
                    Long stockModelId = db.getStockModelId(stockModelCode);
                    StockModel stockModel = db.findStockModelById(stockModelId);
                    if (stockModel != null) {
//                            Step 3.1: Make SaleTrans, SaleTransDetail, SaleTransSerial...
                        Long saleTransId = db.getSequence("SALE_TRANS_SEQ", "dbsm");
                        String strTempId = db.getShopIdStaffIdByStaffCode(staffCode);//SHOP_ID|STAFF_ID
                        String[] arrTempId = strTempId.split("\\|");
                        int rsMakeSaleTrans = db.insertSaleTrans(saleTransId, Long.valueOf(arrTempId[0]), Long.valueOf(arrTempId[1]),
                                0L, 0L, priceOfHandset, 0L,
                                "", 200351L, orgRequestId, bn.getPayMethod(), "", 0, 0, bn.getActionProfileId());
                        if (rsMakeSaleTrans != 1) {
                            //Insert fail, insert log, send sms
                            db.insertLogMakeSaleTransFail(saleTransId, bn.getIsdnCustomer(), bn.getSimSerial(), Long.valueOf(arrTempId[0]),
                                    Long.valueOf(arrTempId[1]), 0L, 0L,
                                    200351L, 0L, 0L, "SALE_TRANS");
                            for (String isdn : arrIsdnReceiveError) {
                                db.sendSms(isdn, "Make saleTrans fail, more detail in table: kit_make_sale_trans_fail. saleTransId: "
                                        + saleTransId, "86142");
                            }
                        }
                        //Make sale trans order if payment by bankTransfer...
                        if (bn.getPayMethod() == 0 && bn.getBankName() != null && bn.getBankName().length() > 0
                                && bn.getBankTranAmount() != null && bn.getBankTranAmount().length() > 0
                                && bn.getBankTranCode() != null && bn.getBankTranCode().length() > 0) {
                            int rsMakeSaleTransOrder = db.insertSaleTransOrder(bn.getBankName(), bn.getBankTranAmount(), bn.getBankTranCode(), saleTransId);
                            if (rsMakeSaleTransOrder != 1) {
                                //Insert fail, insert log, send sms
                                db.insertLogMakeSaleTransFail(saleTransId, bn.getIsdnCustomer(), bn.getSimSerial(), Long.valueOf(arrTempId[0]),
                                        Long.valueOf(arrTempId[1]), 0L, 0L,
                                        reasonId, 0L, 0L, "SALE_TRANS_ORDER");
                                for (String isdn : arrIsdnReceiveError) {
                                    db.sendSms(isdn, "Make saleTransOrder fail, more detail in table: kit_make_sale_trans_fail. saleTransId: "
                                            + saleTransId + ", kitBatchId: " + bn.getKitBatchId(), "86142");
                                }
                            }
                        }
                        //Make sale trans detail
                        //1. Detail for SaleServices
                        Long saleTransDetailSaleService = db.getSequence("SALE_TRANS_DETAIL_SEQ", "dbsm");
                        int rsSaleTransDetailSaleServices = db.insertSaleTransDetail(saleTransDetailSaleService, saleTransId, String.valueOf(stockModelId), String.valueOf(priceOfHandsetObj.getPriceId()), "",
                                "", "7", "Handset", stockModel.getAccountingModelCode(), stockModel.getAccountingModelName(), "", "",
                                stockModel.getAccountingModelCode(), stockModel.getAccountingModelName(), "", "17",
                                String.valueOf(priceOfHandset), "", priceOfHandset, 0.0, 1L);
                        if (rsSaleTransDetailSaleServices != 1) {
                            //Insert fail, insert log, send sms
                            db.insertLogMakeSaleTransFail(saleTransId, bn.getIsdnCustomer(), bn.getSimSerial(), Long.valueOf(arrTempId[0]),
                                    Long.valueOf(arrTempId[1]), 0L, 0L,
                                    200351L, saleTransDetailSaleService, 0L, "SALE_TRANS_DETAIL");
                            for (String isdn : arrIsdnReceiveError) {
                                db.sendSms(isdn, "Make saleTransDetail for SaleServices fail, more detail in table: kit_make_sale_trans_fail. saleTransDetailId: " + saleTransDetailSaleService, "86142");
                            }
                        }
                        //Make saleTransSerial
                        Long saleTransSerialId = db.getSequence("SALE_TRANS_SERIAL_SEQ", "dbsm");
                        int rsSaleTransSerial = db.insertSaleTransSerial(saleTransSerialId, saleTransDetailSaleService, stockModelId, bn.getHandsetSerial());
                        if (rsSaleTransSerial != 1) {
                            //Insert fail, insert log, send sms
                            db.insertLogMakeSaleTransFail(0L, bn.getIsdnCustomer(), bn.getSimSerial(), Long.valueOf(arrTempId[0]),
                                    Long.valueOf(arrTempId[1]), 0L, 0L,
                                    200351L, saleTransDetailSaleService, saleTransSerialId, "SALE_TRANS_SERIAL");
                            for (String isdn : arrIsdnReceiveError) {
                                db.sendSms(isdn, "Make saleTransSerial fail, more detail in table: kit_make_sale_trans_fail. saleTransSerialId: " + saleTransSerialId, "86142");
                            }
                        }
//                                Step 4: Minus stockTotal
                        int expStockTotal = db.expStockTotal(staffCode, stockModelId, 1L);
                        if (expStockTotal != 1) {
                            //Insert fail, insert log, send sms
                            for (String isdn : arrIsdnReceiveError) {
                                db.sendSms(isdn, "expStockTotal fail, serialHanset: " + bn.getHandsetSerial()
                                        + ", isdnCustomer: " + bn.getIsdnCustomer(), "86142");
                            }
                        }
//                                Step 5: update stock_hand >> status = 0 >> already sale...
                        int updateSeialExp = db.updateSeialExp(staffCode, stockModelId, bn.getHandsetSerial());
                        if (updateSeialExp != 1) {
                            //Insert fail, insert log, send sms
                            for (String isdn : arrIsdnReceiveError) {
                                db.sendSms(isdn, "updateSeialExp fail, serialHanset: " + bn.getHandsetSerial()
                                        + ", isdnCustomer: " + bn.getIsdnCustomer(), "86142");
                            }
                        }
//                        } 
                    }

                }
                //Check bonus for prepaidMonth
                if (isPrepaidMonth) {
                    Customer customer = db.getCustomerByCustId(bn.getCustId());
                    pro.createEmolaForCustomer(customer, bn.getIsdnCustomer(), db);
//                     else {
                    String strPercentBonus = "";
                    for (String tmp : arrBonusMoneyPrepaidMonth) {
                        String[] arrTmp = tmp.split("\\:");
                        int configMonth;
                        try {
                            configMonth = Integer.parseInt(arrTmp[0]);
                        } catch (NumberFormatException ex) {
                            ex.printStackTrace();
                            configMonth = 0;
                        }
                        if (bn.getPrepaidMonth() >= configMonth) {
                            strPercentBonus = arrTmp[1];
                            logger.info("Subscriber have prepaidMonth value: " + bn.getPrepaidMonth() + ", percentBonus: "
                                    + strPercentBonus + ", sub:  " + bn.getIsdnCustomer());
                            break;
                        }
                    }
                    float percentBonus;
                    try {
                        percentBonus = Float.valueOf(strPercentBonus);
                    } catch (NumberFormatException ex) {
                        ex.printStackTrace();
                        logger.info("Have exeption when parse percentBonus so auto set default percentBonus is zero, ex:"
                                + ex.getMessage() + ", sub:  " + bn.getIsdnCustomer());
                        percentBonus = 0.0f;
                    }
                    bonusForCustomer = priceOfProduct * (bn.getPrepaidMonth()) * percentBonus;
                    logger.info("Subscriber have prepaidMonth value: " + bn.getPrepaidMonth() + ", percentBonus: "
                            + strPercentBonus + ", bonusForCustomer: " + bonusForCustomer + ", sub:  " + bn.getIsdnCustomer());
//                    }
                    //insert table for control prepaidMonth
                    int rsPrepaid = db.insertKitElitePrepaid(bn.getIsdnCustomer(), bn.getPrepaidMonth(), staffCode, productCode);
                    if (rsPrepaid == 1) {
                        logger.info("insert kit_elite_prepaid successfully, prepaidMonth: " + bn.getPrepaidMonth() + ", isdn: "
                                + bn.getIsdnCustomer() + ", staffCode: " + staffCode);
                    } else {
                        logger.info("insert kit_elite_prepaid unsuccessfully, prepaidMonth: " + bn.getPrepaidMonth() + ", isdn: "
                                + bn.getIsdnCustomer() + ", staffCode: " + staffCode);
                    }
                }
                //Make sale trans
                if (totalAmount > 0) {
                    logger.info("Total amount: " + totalAmount + " greater than 0, need to make revenue, isdn: " + bn.getIsdnCustomer());
                    Long saleTransId = db.getSequence("SALE_TRANS_SEQ", "dbsm");
                    String strTempId = db.getShopIdStaffIdByStaffCode(staffCode);
                    String[] arrTempId = strTempId.split("\\|");
                    int rsMakeSaleTrans = db.insertSaleTrans(saleTransId, Long.valueOf(arrTempId[0]), Long.valueOf(arrTempId[1]),
                            0L, 0L, totalAmount.doubleValue(), Long.valueOf(bn.getPkId()),
                            bn.getIsdnCustomer(), reasonId, orgRequestId, bn.getPayMethod(), bn.getBankTranCode(), 0.0, 0, bn.getActionProfileId());
                    if (rsMakeSaleTrans != 1) {
                        //Insert fail, insert log, send sms
                        db.insertLogMakeSaleTransFail(saleTransId, bn.getIsdnCustomer(), bn.getSimSerial(), Long.valueOf(arrTempId[0]),
                                Long.valueOf(arrTempId[1]), 0L, 0L,
                                reasonId, 0L, 0L, "SALE_TRANS");
                        for (String isdn : arrIsdnReceiveError) {
                            db.sendSms(isdn, "Make saleTrans fail, more detail in table: kit_make_sale_trans_fail. saleTransId: "
                                    + saleTransId, "86142");
                        }
                    }
                    //Make sale trans order if payment by bankTransfer...
                    if (bn.getPayMethod() == 0 && bn.getBankName() != null && bn.getBankName().length() > 0
                            && bn.getBankTranAmount() != null && bn.getBankTranAmount().length() > 0
                            && bn.getBankTranCode() != null && bn.getBankTranCode().length() > 0) {
                        int rsMakeSaleTransOrder = db.insertSaleTransOrder(bn.getBankName(), bn.getBankTranAmount(), bn.getBankTranCode(), saleTransId);
                        if (rsMakeSaleTransOrder != 1) {
                            //Insert fail, insert log, send sms
                            db.insertLogMakeSaleTransFail(saleTransId, bn.getIsdnCustomer(), bn.getSimSerial(), Long.valueOf(arrTempId[0]),
                                    Long.valueOf(arrTempId[1]), 0L, 0L,
                                    reasonId, 0L, 0L, "SALE_TRANS_ORDER");
                            for (String isdn : arrIsdnReceiveError) {
                                db.sendSms(isdn, "Make saleTransOrder fail, more detail in table: kit_make_sale_trans_fail. saleTransId: "
                                        + saleTransId + ", kitBatchId: " + bn.getKitBatchId(), "86142");
                            }
                        }
                    }
                    //Make sale trans detail
                    //1. Detail for SaleServices
                    Long saleTransDetailSaleService = db.getSequence("SALE_TRANS_DETAIL_SEQ", "dbsm");
//                int rsSaleTransDetailSaleServices = 0;
//                if (isPrepaidMonth) {
//                    rsSaleTransDetailSaleServices = db.insertSaleTransDetail(saleTransDetailSaleService, saleTransId, "208309", "", "",//stockModelId = 208309 for emola scratch card connect
//                            "", "", "", "", "", "", "","", "", "17", "",
//                            "", String.valueOf(priceOfServices), priceOfServices.doubleValue(), 0.0);
//                } else {
//                    rsSaleTransDetailSaleServices = db.insertSaleTransDetail(saleTransDetailSaleService, saleTransId, "208309", "", "",
//                            "","", "", "", "", "", "", "", "", "17", "",
//                            "", String.valueOf(priceOfServices), priceOfServices.doubleValue(), 0.0);
//                }
                    int rsSaleTransDetailSaleServices = db.insertSaleTransDetail(saleTransDetailSaleService, saleTransId, "208309", "520784", "",//stockModelId = 208309 for emola scratch card connect, priceId = 520784 --> For price of emola scratch card
                            "", "", "", "", "", "", "", "", "", "", "17",
                            "1", "", (totalAmount.doubleValue() - priceIsdn.doubleValue()), 0.0, (totalAmount - priceIsdn.longValue()));
//                int rsSaleTransDetailSaleServices = db.insertSaleTransDetail(saleTransDetailSaleService, saleTransId, "", "", saleService.getSaleServicesId(),
//                        String.valueOf(saleServicesPrice.getSaleServicesPriceId()),
//                        "", "", "", "", saleServiceCode, saleService.getName(), saleService.getAccountModelCode(), saleService.getAccountModelName(), "17", "",
//                        "", String.valueOf(saleServicesPrice.getPrice()), saleServicesPrice.getPrice(), 0.0);
                    if (rsSaleTransDetailSaleServices != 1) {
                        //Insert fail, insert log, send sms
                        db.insertLogMakeSaleTransFail(saleTransId, bn.getIsdnCustomer(), bn.getSimSerial(), Long.valueOf(arrTempId[0]),
                                Long.valueOf(arrTempId[1]), 0L, 0L,
                                reasonId, saleTransDetailSaleService, 0L, "SALE_TRANS_DETAIL");
                        for (String isdn : arrIsdnReceiveError) {
                            db.sendSms(isdn, "Make saleTransDetail for SaleServices fail, more detail in table: kit_make_sale_trans_fail. saleTransDetailId: " + saleTransDetailSaleService, "86142");
                        }
                    }
                    //2. Detail for Isdn
                    Long stockModelId = db.getStockModelIdByIsdn(bn.getIsdnCustomer());
                    StockModel stockModel = db.findStockModelById(stockModelId);
                    Long saleTransDetailIsdn = db.getSequence("SALE_TRANS_DETAIL_SEQ", "dbsm");
                    int rsSaleTransDetailIsdn = db.insertSaleTransDetail(saleTransDetailIsdn, saleTransId, String.valueOf(stockModelId),
                            String.valueOf(priceOfIsdn.getPriceId()), "", "",
                            "1", "Mobile Number", stockModel.getStockModelCode(), stockModel.getName(), "", "", stockModel.getAccountingModelCode(), stockModel.getAccountingModelName(), "", "17",
                            String.valueOf(priceOfIsdn.getPrice()), "", priceOfIsdn.getPrice(), discountAmount, 1L);
                    if (rsSaleTransDetailIsdn != 1) {
                        //Insert fail, insert log, send sms
                        db.insertLogMakeSaleTransFail(saleTransId, bn.getIsdnCustomer(), bn.getSimSerial(), Long.valueOf(arrTempId[0]),
                                Long.valueOf(arrTempId[1]), 0L, 0L,
                                reasonId, saleTransDetailIsdn, 0L, "SALE_TRANS_DETAIL");
                        for (String isdn : arrIsdnReceiveError) {
                            db.sendSms(isdn, "Make saleTransDetail for Isdn fail, more detail in table: kit_make_sale_trans_fail. saleTransDetailId: " + saleTransDetailIsdn, "86142");
                        }
                    }
                    //Make saleTransSerial
                    Long saleTransSerialId = db.getSequence("SALE_TRANS_SERIAL_SEQ", "dbsm");
                    int rsSaleTransSerial = db.insertSaleTransSerial(saleTransSerialId, saleTransDetailIsdn, stockModelId, bn.getIsdnCustomer());
                    if (rsSaleTransSerial != 1) {
                        //Insert fail, insert log, send sms
                        db.insertLogMakeSaleTransFail(0L, bn.getIsdnCustomer(), bn.getSimSerial(), Long.valueOf(arrTempId[0]),
                                Long.valueOf(arrTempId[1]), 0L, 0L,
                                reasonId, saleTransDetailIsdn, saleTransSerialId, "SALE_TRANS_SERIAL");
                        for (String isdn : arrIsdnReceiveError) {
                            db.sendSms(isdn, "Make saleTransSerial fail, more detail in table: kit_make_sale_trans_fail. saleTransSerialId: " + saleTransSerialId, "86142");
                        }
                    }
                } else {
                    logger.info("Total amount is 0 MT, no need to make revenue for transaction..., isdn: " + bn.getIsdnCustomer());
                }

                String tel = db.getTelByStaffCode(staffCode);
                cal = Calendar.getInstance();
                cal.setTime(new Date());
                cal.add(Calendar.DATE, 7);
                String tempMsg = connectSimSucessfully.replace("%PCKG%", tmpProduct).replace("%SERIAL%", bn.getSimSerial())
                        .replace("%DAY%", sdf2.format(cal.getTime()));
                db.sendSms(tel, tempMsg, "86142");
                logger.info("isdn " + bn.getIsdnCustomer() + " Message send to channel: " + tempMsg);
                if (!isdnOwnerOfChannel.isEmpty() && !smsSaleHandsetForManager.isEmpty()) {
                    db.sendSms(isdnOwnerOfChannel, smsSaleHandsetForManager, "86142");
                }
                //Calculate bonus.
                Comission comission = db.getComissionStaff(staffCode, productCode, bn.getIsdnCustomer());
                if (comission != null) {
                    //                20190104 change not pay bonus now, insert to pay_bonus_connect_kit to wait sub active
                    db.insertBonusConnectKit(staffCode, productCode, bn.getIsdnCustomer(), bn.getActionAuditId(), bn.getChannelTypeId(),
                            bn.getActionCode(), bn.getSimSerial(), bn.getIssueDateTime(), bn.getPrepaidMonth(),
                            bonusForCustomer, totalCommissionForPrepaidMonth, bn.getActionProfileId());
                } else {
                    if (bn.getVasCode() != null) {
                        //Product not VIP, not receive bonus
                        logger.info("Don't pay comission because product not VIP, start check commission of vas_code: " + bn.getVasCode() + ", isdn: " + bn.getIsdnCustomer());
                        comission = db.getComissionStaff(staffCode, bn.getVasCode(), bn.getIsdnCustomer());
                        if (comission != null) {
                            logger.info("start insertBonusConnectKit, because vasCode: " + bn.getVasCode() + " have config comission, isdn: " + bn.getIsdnCustomer());
//                20190104 change not pay bonus now, insert to pay_bonus_connect_kit to wait sub active
                            db.insertBonusConnectKit(staffCode, productCode, bn.getIsdnCustomer(), bn.getActionAuditId(), bn.getChannelTypeId(),
                                    bn.getActionCode(), bn.getSimSerial(), bn.getIssueDateTime(), bn.getPrepaidMonth(),
                                    bonusForCustomer, totalCommissionForPrepaidMonth, bn.getActionProfileId());
                        } else {
                            bn.seteWalletDescription("Don't pay comission because product not VIP.");
                            bn.setDuration(System.currentTimeMillis() - timeSt);
                        }
                    }
                }
            } else {
                logger.warn("After validate respone code is fail actionId " + bn.getActionAuditId()
                        + " id " + bn.getId() + " isdn: " + bn.getIsdnCustomer()
                        + " so continue with other transaction");
                bn.setDescription("After validate respone code is fail");
                bn.setDuration(System.currentTimeMillis() - timeSt);
                continue;
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
                append("|\tID|").
                append("|\tACTION_AUDIT_ID|").
                append("|\tPK_ID|").
                append("|\tCREATE_STAFF\t|").
                append("|\tCREATE_SHOP\t|").
                append("|\tCREATE_TIME\t|").
                append("|\tCHECK_STAFF\t|").
                append("|\tCHECK_INFO\t|").
                append("|\tCHECK_TIME\t|").
                append("|\tISDN\t|").
                append("|\tSIM_SERIAL\t|").
                append("|\tCONNECT_KIT_STATUS\t|").
                append("|\tVAS_CODE\t|").
                append("|\tKIT_BATCH_ID\t|").
                append("|\tPREPAID_MONTH\t|").
                append("|\tPAY_METHOD\t|").
                append("|\tBONUS_STATUS\t|");
        for (Record record : listRecord) {
            Bonus bn = (Bonus) record;
            br.append("\r\n").
                    append("|\t").
                    append(bn.getId()).
                    append("||\t").
                    append(bn.getActionAuditId()).
                    append("||\t").
                    append(bn.getPkId()).
                    append("||\t").
                    append(bn.getUserName()).
                    append("||\t").
                    append(bn.getShopCode()).
                    append("||\t").
                    append((bn.getIssueDateTime() != null ? sdf.format(bn.getIssueDateTime()) : null)).
                    append("||\t").
                    append(bn.getStaffCheck()).
                    append("||\t").
                    append(bn.getCheckInfo()).
                    append("||\t").
                    append((bn.getTimeCheck() != null ? sdf.format(bn.getTimeCheck()) : null)).
                    append("||\t").
                    append(bn.getIsdnCustomer()).
                    append("||\t").
                    append(bn.getSimSerial()).
                    append("||\t").
                    append(bn.getConnectKitStatus()).
                    append("||\t").
                    append(bn.getVasCode()).
                    append("||\t").
                    append(bn.getKitBatchId()).
                    append("||\t").
                    append(bn.getPrepaidMonth()).
                    append("||\t").
                    append(bn.getPayMethod()).
                    append("||\t").
                    append(bn.getBonusStatus());
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

    public void blockOneWay(String msisdn, boolean isSim4G) {
//        Flow Step...
//        HLR_HW_MOD_BAOC
//        HLR_HW_MOD_BAIC
//        HLR_HW_MOD_PLMNSS
//        4G: HLR_HW_MODI_SUB_LOCK_4G / 3G: HLR_HW_MODI_SUB_GPRS
//        HLR_HW_MOD_ODBROAM
//      Close BAOC flag on HLR for customer -- HLR_HW_MOD_BAOC
        String closeFlagBAOCResult = pro.inActiveFlagBAOC("258" + msisdn);
        if ("0".equals(closeFlagBAOCResult)) {
            logger.info("Close flag BAOC successfully for sub when register info " + msisdn);
        } else {
            logger.warn("Fail to open flag BAOC for sub when register info " + msisdn);
        }
//      Commit BAOC flag on HLR for customer -- HLR_HW_ACT_BAOC
        String commitFlagBAOCResult = pro.commitFlagBAOC("258" + msisdn);
        if ("0".equals(commitFlagBAOCResult)) {
            logger.info("Close flag BAOC successfully for sub when register info " + msisdn);
        } else {
            logger.warn("Fail to open flag BAOC for sub when register info " + msisdn);
        }
        String modFlagBAIC = pro.modFlagBAIC("258" + msisdn);
        if ("0".equals(modFlagBAIC)) {
            logger.info("Close flag BAIC successfully for sub when register info " + msisdn + ", command: HLR_HW_MOD_BAIC");
        } else {
            logger.warn("Fail to open flag BAIC for sub when register info " + msisdn);
        }

        String actFlagBAIC = pro.commitFlagBAIC("258" + msisdn);
        if ("0".equals(actFlagBAIC)) {
            logger.info("Close flag BAIC successfully for sub when register info " + msisdn + ", command: HLR_HW_ACT_BAIC");
        } else {
            logger.warn("Fail to open flag BAIC for sub when register info " + msisdn);
        }
//      Open USSD flag on HLR for customer -- HLR_HW_MOD_PLMNSS
        String inActiveFlagPLMNSS = pro.inActiveFlagPLMNSS("258" + msisdn);
        if ("0".equals(inActiveFlagPLMNSS)) {
            logger.info("Open flag HLR_HW_MOD_PLMNSS successfully for sub when register info " + msisdn);
        } else {
            logger.warn("Fail to close flag HLR_HW_MOD_PLMNSS for sub when register info " + msisdn);
        }
//      Close GPRSLCK flag on HLR for customer -- HLR_HW_MODI_SUB_GPRS (3G)
        if (!isSim4G) {
            String closeFlagGPRSResult = pro.inActiveFlagGPRSLCK("258" + msisdn);
            if ("0".equals(closeFlagGPRSResult)) {
                logger.info("Close flag GPRS successfully for sub when register info " + msisdn);
            } else {
                logger.warn("Fail to open flag GPRS for sub when register info " + msisdn);
            }
        } else {
            String flagData = pro.modDataFlag("258" + msisdn, "TRUE", "TRUE");
            if ("0".equals(flagData)) {
                logger.info("Barring DATA 3G/4G successfully for sub when connect kit  " + msisdn);
            } else {
                logger.warn("Fail to open flag MODI_GPRS for sub when connect kit  " + msisdn);
            }
        }
        String flagRoaming = pro.modRoamingFlag("258" + msisdn, "BROHPLMNC");
        if ("0".equals(flagRoaming)) {
            logger.info("Barring ROAMING successfully for sub when connect kit  " + msisdn);
        } else {
            logger.warn("Fail to open flag MODI_GPRS for sub when connect kit  " + msisdn);
        }
    }
}
