/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbKitBatchConnectProcessor;
import com.viettel.paybonus.obj.AssignedUser;
import com.viettel.paybonus.obj.Comission;
import com.viettel.paybonus.obj.Customer;
import com.viettel.paybonus.obj.KitBatch;
import com.viettel.paybonus.obj.KitBatchInfo;
import com.viettel.paybonus.obj.Offer;
import com.viettel.paybonus.obj.Price;
import com.viettel.paybonus.obj.ProductConnectKit;
import com.viettel.paybonus.obj.StockModel;
import com.viettel.paybonus.service.BankTransferUtils;
import com.viettel.paybonus.service.Exchange;
import com.viettel.paybonus.service.Service;
import com.viettel.threadfw.manager.AppManager;
import java.util.List;
import org.apache.log4j.Logger;
import com.viettel.threadfw.process.ProcessRecordAbstract;
import com.viettel.vas.util.ExchangeClientChannel;
import java.math.BigDecimal;
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
public class KitBatchConnect extends ProcessRecordAbstract {

    Exchange pro;
    Service services;
    DbKitBatchConnectProcessor db;
    String urlPaymentVoucher;
    String keyPaymentVoucher;
    String lstIsdnReceiveError;
    String[] arrIsdnReceiveError;
    String specialProductFreeIsdn;
    SimpleDateFormat sdfAddPrice = new SimpleDateFormat("yyyyMMddHHmmss");
    SimpleDateFormat sdf2 = new SimpleDateFormat("dd/MM/yyyy");
    String eliteMsgProductMap;
    String[] arrEliteMsgProductMap;
    ArrayList<HashMap> lstEliteMsgProductMap;
    String agentEmola;
    String[] arrAgentEmola;
    ArrayList<HashMap> lstMapAgentEmola;
    String kitBatchDevUnitCommission;
    String[] arrKitBatchDevUnitCommission;
    ArrayList<HashMap> lstMapKitBatchDevUnitCommission;
    String minSubInBatch;
    String minMoneyInBatch;
    String debitEnterpriseConfig;
    String[] arrDebitEnterpriseConfig;
    ArrayList<HashMap> lstMapDebitEnterpiseConfig;
    String eliteKitBatchGroupFreeCallMsg;
    String eliteKitBatchProductBranchPromotion;
    String[] arrProductBranchPromotion;
    ArrayList<HashMap> lstMapProductBranchPromotion;
    String vasCodeAmountMoney;
    String[] arrVasCodeAmountMoney;
    String vasCodeAmountRate;
    ArrayList<HashMap> lstMapVasCodeAmount;
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
    String kitConnectBonusMoneyPrepaidMonth;
    String[] arrBonusMoneyPrepaidMonth;
    String kitConnectUpdateTransCodeFail;
    String bonusRateForPrepaidMonth;
    String kitBatchEnterpriseOwnerBonus;
    String kitBatchEnterpriseSpecStaffBonus;
    String[] arrSpecStaffBonus;
    String kitBatchEnterprisePck;
    String[] arrKitBatchEnterprisePck;
    double limitMoneyAddonAddCUG;

    public KitBatchConnect() {
        super();
        logger = Logger.getLogger(KitBatchConnect.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
        services = new Service(ExchangeClientChannel.getInstance("../etc/service_client.cfg").getInstanceChannel(), logger);
        db = new DbKitBatchConnectProcessor();

        urlPaymentVoucher = ResourceBundle.getBundle("configPayBonus").getString("urlPaymentVoucher");
        keyPaymentVoucher = ResourceBundle.getBundle("configPayBonus").getString("keyPaymentVoucher");
        lstIsdnReceiveError = ResourceBundle.getBundle("configPayBonus").getString("lstIsdnReceiveError");
        arrIsdnReceiveError = lstIsdnReceiveError.split("\\|");
        specialProductFreeIsdn = ResourceBundle.getBundle("configPayBonus").getString("specialProductFreeIsdn");
        agentEmola = ResourceBundle.getBundle("configPayBonus").getString("KitConnectSpecialAgentEmola");
        kitBatchEnterpriseOwnerBonus = ResourceBundle.getBundle("configPayBonus").getString("KitBatchEnterpriseOwnerBonus");
        kitBatchEnterpriseSpecStaffBonus = ResourceBundle.getBundle("configPayBonus").getString("KitBatchEnterpriseSpecStaffBonus");
        arrAgentEmola = agentEmola.split("\\|");

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

        kitBatchDevUnitCommission = ResourceBundle.getBundle("configPayBonus").getString("KitBatchDevelopmentUnit");
        arrKitBatchDevUnitCommission = kitBatchDevUnitCommission.split("\\|");
        lstMapKitBatchDevUnitCommission = new ArrayList<HashMap>();
        for (String tmp : arrKitBatchDevUnitCommission) {
            String[] arrTmp = tmp.split("\\:");
            HashMap map = new HashMap();
            map.put(arrTmp[0].trim(), arrTmp[1].trim());
            lstMapKitBatchDevUnitCommission.add(map);
        }
        minMoneyInBatch = ResourceBundle.getBundle("configPayBonus").getString("minMoneyInBatch");

        debitEnterpriseConfig = ResourceBundle.getBundle("configPayBonus").getString("debitEnterpriseConfig");
        arrDebitEnterpriseConfig = debitEnterpriseConfig.split("\\|");
        lstMapDebitEnterpiseConfig = new ArrayList<HashMap>();
        for (String tmp : arrDebitEnterpriseConfig) {
            String[] arrTmp = tmp.split("\\:");
            HashMap map = new HashMap();
            map.put(arrTmp[0].trim(), arrTmp[1].trim());
            lstMapDebitEnterpiseConfig.add(map);
        }
        eliteKitBatchGroupFreeCallMsg = ResourceBundle.getBundle("configPayBonus").getString("eliteKitBatchGroupFreeCallMsg");
        eliteKitBatchProductBranchPromotion = ResourceBundle.getBundle("configPayBonus").getString("eliteKitBatchProductBranchPromotion");
        arrProductBranchPromotion = eliteKitBatchProductBranchPromotion.split("\\|");
        lstMapProductBranchPromotion = new ArrayList<HashMap>();
        for (String tmp : arrProductBranchPromotion) {
            String[] arrTmp = tmp.split("\\:");
            HashMap map = new HashMap();
            map.put(arrTmp[0].trim(), arrTmp[1].trim());
            lstMapProductBranchPromotion.add(map);
        }
        vasCodeAmountMoney = ResourceBundle.getBundle("configPayBonus").getString("vasCodeAmountMoney");
        arrVasCodeAmountMoney = vasCodeAmountMoney.split("\\|");
        lstMapVasCodeAmount = new ArrayList<HashMap>();
        for (String vasAmount : arrVasCodeAmountMoney) {
            String[] tmp = vasAmount.split("\\:");
            HashMap map = new HashMap();
            map.put(tmp[0].trim().toUpperCase(), tmp[1].trim());
            lstMapVasCodeAmount.add(map);
        }
        vasCodeAmountRate = ResourceBundle.getBundle("configPayBonus").getString("vasCodeAmountRate");
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
        if (kitBatchEnterpriseSpecStaffBonus != null || !kitBatchEnterpriseSpecStaffBonus.isEmpty()) {
            arrSpecStaffBonus = kitBatchEnterpriseSpecStaffBonus.split("\\|");
        }
        kitConnectBonusMoneyPrepaidMonth = db.getParamValue("CONNECT_BONUS_PAIDMONTH");
        arrBonusMoneyPrepaidMonth = kitConnectBonusMoneyPrepaidMonth.split("\\|");
        kitConnectUpdateTransCodeFail = ResourceBundle.getBundle("configPayBonus").getString("kitConnectUpdateTransCodeFail");
        bonusRateForPrepaidMonth = ResourceBundle.getBundle("configPayBonus").getString("bonusRateForPrepaidMonth");
        kitBatchEnterprisePck = db.getParamValue("ENTERPRISE_PRODUCT_CODE");
        if (kitBatchEnterprisePck != null && !kitBatchEnterprisePck.isEmpty()) {
            arrKitBatchEnterprisePck = kitBatchEnterprisePck.split("\\|");
        }
        minSubInBatch = db.getParamValue("LIMIT_FREE_CALL_IN_BATCH");
        if (minSubInBatch == null || minSubInBatch.trim().isEmpty()) {
            minSubInBatch = "20";
        }
        String srtlimitMoneyAddonAddCUG = db.getParamValue("LIMIT_MONEY_ADDON_CUG");
        if (srtlimitMoneyAddonAddCUG != null && !srtlimitMoneyAddonAddCUG.trim().isEmpty()) {
            limitMoneyAddonAddCUG = new BigDecimal(srtlimitMoneyAddonAddCUG).doubleValue();
        } else {
            limitMoneyAddonAddCUG = 200;
        }

    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        for (Record record : listRecord) {
            KitBatchInfo moRecord = (KitBatchInfo) record;
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
        String openFlagMODIGPRSResult;
        String orgRequestId;
        SimpleDateFormat sdf = new SimpleDateFormat("ddMMyyyyHHmmssSSS");
        boolean isSim4G;
        Double totalMoney;
        Double totalDiscountForPrepaidMonth;
        long totalSuccess;
        Long totalCommission;
//        long totalCommissionForPrepaidMonth;
        Long totalFail;
//        Long actionAuditId;
        boolean isAddMonth;
        String expireTimeGroup;
        Double totalAmountVas;
        double totalPriceOfHandset;
        boolean isValidate;
        String isdnEnterprise;
        boolean isExistEnterprisePackage;
        String BCCS_CM = "BCCS_CM";
        Double totalAmountAddon;
        double tmpDiscountForPrepaidMonthOldSub = 0;
        String prepaidMonthInput = "0";

        for (Record record : listRecord) {
            timeSt = System.currentTimeMillis();
            staffCode = "";
            orgRequestId = "";
            openFlagMODIGPRSResult = "";
            isSim4G = false;
            isAddMonth = false;
            totalMoney = 0.0;
            totalFail = 0L;
            totalSuccess = 0L;
            totalCommission = 0L;
//            actionAuditId = 0L;
            expireTimeGroup = "";
            totalAmountVas = 0.0;
            totalDiscountForPrepaidMonth = 0.0;
//            totalCommissionForPrepaidMonth = 0;
            totalPriceOfHandset = 0;
            KitBatchInfo bn = (KitBatchInfo) record;
            isValidate = false;
            isExistEnterprisePackage = false;
            isdnEnterprise = "";
            totalAmountAddon = 0.0;
            listResult.add(bn);

            if ("0".equals(bn.getResultCode())) {
                bn.setTotalSuccess(totalSuccess);
                bn.setTotalFail(totalFail);
                bn.setTotalMoney(totalMoney.longValue());
                bn.setGroupStatus("0");
                bn.setExpireTimeGroup(expireTimeGroup);
//                Step 0: Update status = 9 in table KIT_BATCH_INFO to avoid duplicating processing when this thread timeout
                db.changeBatchProcessing(bn.getKitBatchId());
//                Step 1: Get List KitBatchDetail...
                List<KitBatch> lstKitBatchDetail = db.getListKitBatchDetail(bn.getKitBatchId());
                if (lstKitBatchDetail.isEmpty()) {
                    logger.warn("List KitBatchDetail is null or empty, id " + bn.getKitBatchId());
                    bn.setResultCode("02");
                    bn.setDescription("List KitBatchDetail is null or empty");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    if ("0".equals(bn.getPayType())) {
                        logger.info("Paymethod is BankTransfer, now rollback trans: bankCode: " + bn.getBankTransCode()
                                + ", bankName: " + bn.getBankName() + "id " + bn.getKitBatchId() + ", staff: " + staffCode);
                        String rollBackTrans = BankTransferUtils.callWSSOAProllbackTrans(bn.getBankTransCode(), bn.getBankName(),
                                bn.getBankTransAmount(), staffCode, staffCode);
                        if (!"00".equalsIgnoreCase(rollBackTrans)) {
                            logger.info("Rollback of BankDocument fail error_code: " + rollBackTrans + ", id: " + bn.getKitBatchId() + ", staffCode: " + staffCode);
                            bn.setDescription(bn.getDescription() + "--Rollback BankDocument fail");
                        } else {
                            logger.info("Rollback of BankDocument BankDocument successfully, error_code: " + rollBackTrans
                                    + "id: " + bn.getKitBatchId() + ", staffCode: " + staffCode);
                        }
                    }
                    continue;
                }
                if ("0".equals(bn.getPayType())) {
                    double totalMoneyBatch = db.getTotalKitBatchMount(bn.getKitBatchId());
                    if (totalMoneyBatch > Double.parseDouble(bn.getBankTransAmount())) {
                        logger.info("The money of bank doc cument is invalid, error_code: "
                                + "id: " + bn.getKitBatchId() + ", staffCode: " + staffCode);
                        logger.warn("create_user in kit_batch_info is null or empty, id " + bn.getKitBatchId());
                        bn.setResultCode("01");
                        bn.setDescription("The money of bank doc cument is invalid");
                        bn.setDuration(System.currentTimeMillis() - timeSt);
                        for (KitBatch kitBatch : lstKitBatchDetail) {
                            int rollbackIsdn = db.updateStockIsdn(1L, "SYS_ROLLBACK", kitBatch.getIsdn());
                            int rollbackSim = db.updateStockSim(0L, 1L, kitBatch.getSerial(), kitBatch.getIsdn());
                            logger.warn("create_user in kit_batch_info is null, Result rollback stockIsdnMobile: " + rollbackIsdn + " stockSim: "
                                    + rollbackSim + " for serial: " + kitBatch.getSerial()
                                    + " isdn " + kitBatch.getIsdn());
                        }
                        continue;
                    }
                }

//                Step 1.1: Check account ewallet on SM to get isdn ewallet, staff_code
                staffCode = bn.getCreateUser();
                isdnEnterprise = bn.getEnterpriseWallet();
                if (staffCode == null || "".equals(staffCode)) {
                    logger.warn("create_user in kit_batch_info is null or empty, id " + bn.getKitBatchId());
                    bn.setResultCode("01");
                    bn.setDescription("CreateStaff in Sub_Profile_Info is null or empty");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    for (KitBatch kitBatch : lstKitBatchDetail) {
                        int rollbackIsdn = db.updateStockIsdn(1L, "SYS_ROLLBACK", kitBatch.getIsdn());
                        int rollbackSim = db.updateStockSim(0L, 1L, kitBatch.getSerial(), kitBatch.getIsdn());
                        logger.warn("create_user in kit_batch_info is null, Result rollback stockIsdnMobile: " + rollbackIsdn + " stockSim: "
                                + rollbackSim + " for serial: " + kitBatch.getSerial()
                                + " isdn " + kitBatch.getIsdn());
                    }
                    if ("0".equals(bn.getPayType())) {
                        logger.info("Paymethod is BankTransfer, now rollback trans: bankCode: " + bn.getBankTransCode()
                                + ", bankName: " + bn.getBankName() + "id " + bn.getKitBatchId() + ", staff: " + staffCode);
                        String rollBackTrans = BankTransferUtils.callWSSOAProllbackTrans(bn.getBankTransCode(), bn.getBankName(),
                                bn.getBankTransAmount(), staffCode, staffCode);
                        if (!"00".equalsIgnoreCase(rollBackTrans)) {
                            logger.info("Rollback of BankDocument fail error_code: " + rollBackTrans + ", id: " + bn.getKitBatchId() + ", staffCode: " + staffCode);
                            bn.setDescription(bn.getDescription() + "--Rollback BankDocument fail");
                        } else {
                            logger.info("Rollback of BankDocument BankDocument successfully, error_code: " + rollBackTrans
                                    + "id: " + bn.getKitBatchId() + ", staffCode: " + staffCode);
                        }
                    }
                    continue;
                }
                boolean isStaff = db.checkMovitelStaff(staffCode);
                String tel = db.getTelByStaffCode(staffCode);
                if (BCCS_CM.equals(bn.getChannelType())) {
                    logger.info("start check front of BI, backside of BI, custId: " + bn.getCustId());
                    boolean checkValidProfile = false;
                    if (BCCS_CM.equals(bn.getChannelType())) {
                        checkValidProfile = db.checkValidProfileCorporeate(bn.getCustId());
                    } else {
                        checkValidProfile = db.checkBIOfDocument(bn.getCustId());
                    }
                    if (!checkValidProfile) {
                        logger.warn("Invalid profile corporate, custId: " + bn.getCustId() + ", id " + bn.getKitBatchId());
                        bn.setResultCode("07");
                        bn.setDescription("Invalid profile corporate");
                        bn.setDuration(Long.valueOf(System.currentTimeMillis() - timeSt));
                        for (KitBatch kitBatch : lstKitBatchDetail) {
                            int rollbackIsdn = db.updateStockIsdn(1L, "SYS_ROLLBACK", kitBatch.getIsdn());
                            int rollbackSim = db.updateStockSim(0L, 1L, kitBatch.getSerial(), kitBatch.getIsdn());
                            logger.warn("create_user in kit_batch_info is null, Result rollback stockIsdnMobile: " + rollbackIsdn + " stockSim: "
                                    + rollbackSim + " for serial: " + kitBatch.getSerial()
                                    + " isdn " + kitBatch.getIsdn());
                        }
                        if ("0".equals(bn.getPayType())) {
                            logger.info("Paymethod is BankTransfer, now rollback trans: bankCode: " + bn.getBankTransCode()
                                    + ", bankName: " + bn.getBankName() + "id " + bn.getKitBatchId() + ", staff: " + staffCode);
                            String rollBackTrans = BankTransferUtils.callWSSOAProllbackTrans(bn.getBankTransCode(), bn.getBankName(),
                                    bn.getBankTransAmount(), staffCode, staffCode);
                            if (!"00".equalsIgnoreCase(rollBackTrans)) {
                                logger.info("Rollback of BankDocument fail error_code: " + rollBackTrans + ", id: " + bn.getKitBatchId() + ", staffCode: " + staffCode);
                                bn.setDescription(bn.getDescription() + "--Rollback BankDocument fail");
                            } else {
                                logger.info("Rollback of BankDocument BankDocument successfully, error_code: " + rollBackTrans
                                        + "id: " + bn.getKitBatchId() + ", staffCode: " + staffCode);
                            }
                        }
                        String msg = "Fail to connect kit batch, because the corporate profile is invalid, id " + bn.getKitBatchId();
                        db.sendSms(tel, msg, "86904");
                        continue;
                    }
                }
                if (bn.getAddMonth() == null || (bn.getAddMonth() != null && Integer.parseInt(bn.getAddMonth()) == 0)) {
                    //that mean 
                    logger.warn("ConnectKitBatch from: " + bn.getChannelType()
                            + ", addMonth is null or zero, must increase to default is 1 months, id " + bn.getKitBatchId());
                    bn.setAddMonth("1");
                    prepaidMonthInput = "1";
                }
                if (bn.getAddMonth() != null && Integer.parseInt(bn.getAddMonth()) >= 1) {
                    prepaidMonthInput = bn.getAddMonth();
                }
                if (bn.getAddMonth() != null && Integer.parseInt(bn.getAddMonth()) > 1) {
                    isAddMonth = true;
                    bn.setAddMonth(String.valueOf(Integer.parseInt(bn.getAddMonth()) - 1));//So thang dong truoc = Total - thang hien tai
                }

                //<editor-fold defaultstate="collapsed" desc="Not used">
//                Step 2: Connect KIT
                for (KitBatch kitBatch : lstKitBatchDetail) {
////                        Step 2.1: Check ProductCode mapping
                    if (kitBatch.isIsConnectNew()) {
                        ProductConnectKit productCon = null;
                        boolean isRegisterVas = false;
                        if ("PARA5".equals(kitBatch.getProductCode()) && kitBatch.getProductAddOn() != null && kitBatch.isIsConnectNew()) {
                            productCon = db.getProductConnectKit(kitBatch.getProductAddOn());
                            isRegisterVas = true;
                        } else {
                            productCon = db.getProductConnectKit(kitBatch.getProductCode());
                        }

                        if (productCon == null) {
                            logger.info("ProductConnectKit is null, cannot connect kit for sim: " + kitBatch.getSerial()
                                    + ", isdn: " + kitBatch.getIsdn() + ", staff: " + staffCode);
                            db.updateKitBatchDetail(bn.getKitBatchId(), kitBatch.getSerial(), kitBatch.getIsdn(), "ERR_01",
                                    "ProductConnectKit is empty", bn.getNodeName(), null, null);
                            int rollbackIsdn = db.updateStockIsdn(1L, "SYS_ROLLBACK", kitBatch.getIsdn());
                            int rollbackSim = db.updateStockSim(0L, 1L, kitBatch.getSerial(), kitBatch.getIsdn());
                            logger.warn("Can not find ProductConnectKit, Result rollback stockIsdnMobile: " + rollbackIsdn + " stockSim: "
                                    + rollbackSim + " for serial: " + kitBatch.getSerial()
                                    + " isdn " + kitBatch.getIsdn());
                            isValidate = true;
                            break;
                        }

                        String productCode = kitBatch.getProductCode().toUpperCase();
                        if (productCode == null || productCode.trim().length() <= 0) {
                            logger.info("productCode is empty, cannot connect kit for sim: " + kitBatch.getSerial()
                                    + ", isdn: " + kitBatch.getIsdn() + ", staff: " + staffCode);
                            db.updateKitBatchDetail(bn.getKitBatchId(), kitBatch.getSerial(), kitBatch.getIsdn(), "ERR_01",
                                    "productCode is empty", bn.getNodeName(), null, null);
                            int rollbackIsdn = db.updateStockIsdn(1L, "SYS_ROLLBACK", kitBatch.getIsdn());
                            int rollbackSim = db.updateStockSim(0L, 1L, kitBatch.getSerial(), kitBatch.getIsdn());
                            logger.warn("Can not find productCode, Result rollback stockIsdnMobile: " + rollbackIsdn + " stockSim: "
                                    + rollbackSim + " for serial: " + kitBatch.getSerial()
                                    + " isdn " + kitBatch.getIsdn());
                            isValidate = true;
                            break;
                        }

//                        Step 2.2: Calculate saleService Fee + Fee of Isdn
                        Long reasonId = db.getReasonIdByProductCode(productCode, kitBatch.getIsdn());
                        if (reasonId == null) {
                            logger.info("Can not find reasonId, cannot connect kit for sim: " + kitBatch.getSerial()
                                    + ", isdn: " + kitBatch.getIsdn() + ", staff: " + staffCode);
                            db.updateKitBatchDetail(bn.getKitBatchId(), kitBatch.getSerial(), kitBatch.getIsdn(), "ERR_02",
                                    "Reason is empty, not yet define for this product", bn.getNodeName(), null, null);
                            int rollbackIsdn = db.updateStockIsdn(1L, "SYS_ROLLBACK", kitBatch.getIsdn());
                            int rollbackSim = db.updateStockSim(0L, 1L, kitBatch.getSerial(), kitBatch.getIsdn());
                            logger.warn("Can not find reasonId, Result rollback stockIsdnMobile: " + rollbackIsdn + " stockSim: "
                                    + rollbackSim + " for serial: " + kitBatch.getSerial()
                                    + " isdn " + kitBatch.getIsdn());
                            isValidate = true;
                            break;
                        }
                        kitBatch.setReasonId(reasonId);
                        String saleServiceCode = db.getSaleServiceCode(reasonId, productCode, kitBatch.getIsdn());
                        if (saleServiceCode == null) {
                            logger.info("Can not find saleServiceCode, cannot connect kit for sim: " + kitBatch.getSerial()
                                    + ", isdn: " + kitBatch.getIsdn() + ", staff: " + staffCode);
                            db.updateKitBatchDetail(bn.getKitBatchId(), kitBatch.getSerial(), kitBatch.getIsdn(), "ERR_03",
                                    "saleServiceCode not yet define for this product", bn.getNodeName(), null, null);
                            int rollbackIsdn = db.updateStockIsdn(1L, "SYS_ROLLBACK", kitBatch.getIsdn());
                            int rollbackSim = db.updateStockSim(0L, 1L, kitBatch.getSerial(), kitBatch.getIsdn());
                            logger.warn("Can not find saleServiceCode, Result rollback stockIsdnMobile: " + rollbackIsdn + " stockSim: "
                                    + rollbackSim + " for serial: " + kitBatch.getSerial()
                                    + " isdn " + kitBatch.getIsdn());
                            isValidate = true;
                            break;
                        }
                        kitBatch.setSaleServiceCode(saleServiceCode);
//						SaleServices saleService = db.getSaleService(saleServiceCode, kitBatch.getIsdn());
//						if (saleService == null) {
//							logger.info("Can not find saleService, cannot connect kit for sim: " + kitBatch.getSerial()
//									+ ", isdn: " + kitBatch.getIsdn() + ", staff: " + staffCode);
//							db.updateKitBatchDetail(bn.getKitBatchId(), kitBatch.getSerial(), kitBatch.getIsdn(), "ERR_04",
//									"Can not find saleService", bn.getNodeName(), null, null);
//							int rollbackIsdn = db.updateStockIsdn(1L, "SYS_ROLLBACK", kitBatch.getIsdn());
//							int rollbackSim = db.updateStockSim(0L, 1L, kitBatch.getSerial(), kitBatch.getIsdn());
//							logger.warn("Can not find saleService, Result rollback stockIsdnMobile: " + rollbackIsdn + " stockSim: "
//									+ rollbackSim + " for serial: " + kitBatch.getSerial()
//									+ " isdn " + kitBatch.getIsdn());
//							isValidate = true;
//							break;
//						}
//						kitBatch.setSaleServices(saleService);
//						SaleServicesPrice saleServicesPrice = db.getSaleServicesPrice(saleService.getSaleServicesId(), kitBatch.getIsdn());
//						if (saleServicesPrice == null) {
//							logger.info("Can not find saleServicesPrice, cannot connect kit for sim: " + kitBatch.getSerial()
//									+ ", isdn: " + kitBatch.getIsdn() + ", staff: " + staffCode);
//							db.updateKitBatchDetail(bn.getKitBatchId(), kitBatch.getSerial(), kitBatch.getIsdn(), "ERR_05",
//									"Can not find saleServicesPrice", bn.getNodeName(), null, null);
//							int rollbackIsdn = db.updateStockIsdn(1L, "SYS_ROLLBACK", kitBatch.getIsdn());
//							int rollbackSim = db.updateStockSim(0L, 1L, kitBatch.getSerial(), kitBatch.getIsdn());
//							logger.warn("Can not find saleServicesPrice, Result rollback stockIsdnMobile: " + rollbackIsdn + " stockSim: "
//									+ rollbackSim + " for serial: " + kitBatch.getSerial()
//									+ " isdn " + kitBatch.getIsdn());
//							isValidate = true;
//							break;
//						}

                        //kitBatch.setSaleServicesPrice(saleServicesPrice);
//						Price priceOfIsdn = db.getPrice(kitBatch.getIsdn(), saleService.getSaleServicesId());
//						if (priceOfIsdn == null) {
//							logger.info("Can not find priceOfIsdn, cannot connect kit for sim: " + kitBatch.getSerial()
//									+ ", isdn: " + kitBatch.getIsdn() + ", staff: " + staffCode);
//							db.updateKitBatchDetail(bn.getKitBatchId(), kitBatch.getSerial(), kitBatch.getIsdn(), "ERR_06",
//									"Can not find priceOfIsdn", bn.getNodeName(), null, null);
//							int rollbackIsdn = db.updateStockIsdn(1L, "SYS_ROLLBACK", kitBatch.getIsdn());
//							int rollbackSim = db.updateStockSim(0L, 1L, kitBatch.getSerial(), kitBatch.getIsdn());
//							logger.warn("Can not find priceOfIsdn, Result rollback stockIsdnMobile: " + rollbackIsdn + " stockSim: "
//									+ rollbackSim + " for serial: " + kitBatch.getSerial()
//									+ " isdn " + kitBatch.getIsdn());
//							isValidate = true;
//							break;
//						}
                        //kitBatch.setPriceOfIsdn(priceOfIsdn);
                        Double priceIsdn = 0.0;
                        Double discountAmount = 0.0;
//                    logger.info("Config product: " + specialProductFreeIsdn + "---Product code: " + productCode);
//                    if (specialProductFreeIsdn.toUpperCase().contains(productCode.toUpperCase())) {
//						if (priceOfIsdn.getPrice() > productCon.getPrice()) {
//							priceIsdn = priceOfIsdn.getPrice();
//						} else {
//							priceIsdn = 0.0;
//							discountAmount = priceOfIsdn.getPrice();
//						}
//                    } else {
//                        priceIsdn = priceOfIsdn.getPrice();
//                    }
                        logger.info("Price of isdn: " + kitBatch.getIsdn() + " is: " + priceIsdn + " for product: " + productCode);
                        kitBatch.setMoneyIsdn(priceIsdn + "");
                        kitBatch.setDiscountAmount(discountAmount);

                        long tmpDiscountForPrepaidMonth = 0;
                        if (isAddMonth) {
                            double rateBonusPrepaidMonth = 0.0;
                            try {
                                rateBonusPrepaidMonth = Double.parseDouble(bonusRateForPrepaidMonth);
                            } catch (NumberFormatException ex) {
                                ex.printStackTrace();
                                rateBonusPrepaidMonth = 0.0;
                            }
//                        totalCommissionForPrepaidMonth += Math.round(rateBonusPrepaidMonth * saleServicesPrice.getPrice() * Integer.parseInt(bn.getAddMonth()));
                            kitBatch.setBonusForPrepaid(Math.round(rateBonusPrepaidMonth * productCon.getPrice() * Integer.parseInt(bn.getAddMonth())));
//                Step 2.5: Calculate discount for prepaidMonths, if not sale handset...

//							if (kitBatch.getHandsetSerial() == null || kitBatch.getHandsetSerial().length() <= 0) {
//								float percentBonus = calculateBonus(kitBatch, bn.getAddMonth());
//								//Ignore enterprise product
//								boolean ignoreBonus = false;
//								if (arrKitBatchEnterprisePck != null && arrKitBatchEnterprisePck.length > 0) {
//									for (String pck : arrKitBatchEnterprisePck) {
//										if (kitBatch.getProductCode().equalsIgnoreCase(pck)) {
//											ignoreBonus = true;
//											if (!isExistEnterprisePackage) {
//												isExistEnterprisePackage = true;
//												logger.info("The package is enterprise so ignore bonus prepaid month set isExistEnterprisePackage is " + isExistEnterprisePackage);
//											}
//											break;
//										}
//									}
//								}
//								if (!ignoreBonus) {
//									tmpDiscountForPrepaidMonth = Math.round(productCon.getPrice() * (Integer.parseInt(bn.getAddMonth())) * percentBonus);
//								} else {
//									tmpDiscountForPrepaidMonth = 0;
//								}
//
//								logger.info("Batch have prepaidMonth value: " + bn.getAddMonth() + " package " + kitBatch.getProductCode() + ", percentBonus: "
//										+ percentBonus + ", tmpBonusForPrepaidMonth: " + tmpDiscountForPrepaidMonth + ", isdn in batch:  " + kitBatch.getIsdn());
//								totalDiscountForPrepaidMonth += tmpDiscountForPrepaidMonth;
//							} else {
//								//<editor-fold defaultstate="collapsed" desc="for HS">
//								logger.info("start calculate price of handset with discount, no need to pay bonus for customer: "
//										+ kitBatch.getHandsetSerial() + ", sub:  " + kitBatch.getIsdn());
////                        Step 1: Get information of stockModel by stockModelId
//								Long stockModelId = db.getStockModelIdHandset(kitBatch.getHandsetSerial());
//								StockModel stockModel = db.findStockModelById(stockModelId);
//								if (stockModel != null) {
////                            Step 2: find price for sale (saleRetail)
//									Long pricePolicy = db.getPricePolicyByStaffCode(staffCode);
//									Price priceRetail = db.getPriceForSaleRetail(stockModelId, pricePolicy);
//									if (priceRetail != null) {
//										double discountAmountHandset = db.getDiscountForHandset(productCode);
//										double priceOfHandset = priceRetail.getPrice() - discountAmountHandset;
//										totalPriceOfHandset = totalPriceOfHandset + priceOfHandset;
//										logger.info("Price of Handset:" + priceOfHandset + ", total price of handset: " + totalPriceOfHandset
//												+ " for saleRetail by serial: "
//												+ kitBatch.getHandsetSerial() + ", sub:  " + kitBatch.getIsdn());
//									} else {
//										logger.info("cannot find price for saleRetail by serial: "
//												+ kitBatch.getHandsetSerial() + ", sub:  " + kitBatch.getIsdn());
//										for (String isdn : arrIsdnReceiveError) {
//											db.sendSms(isdn, "cannot find price for saleRetail, serialHanset: " + kitBatch.getHandsetSerial()
//													+ ", isdnCustomer: " + kitBatch.getIsdn(), "86142");
//										}
//									}
//								} else {
//									logger.info("cannot find stockModel by serial: "
//											+ kitBatch.getHandsetSerial() + ", sub:  " + kitBatch.getIsdn());
//									for (String isdn : arrIsdnReceiveError) {
//										db.sendSms(isdn, "cannot find stockModel by serial, serialHanset: " + kitBatch.getHandsetSerial()
//												+ ", isdnCustomer: " + kitBatch.getIsdn(), "86142");
//									}
//								}
//
////</editor-fold>
                            //}
                            //Set vas price
//							double amountVas = 0;
//							double totalAmount = 0;
//							if (isRegisterVas) {
//								amountVas = (productCon.getPrice() * (Integer.parseInt(bn.getAddMonth()) + 1));
//								kitBatch.setAmountVas(productCon.getPrice());
//							} else {
//								totalAmount = (productCon.getPrice() * (Integer.parseInt(bn.getAddMonth()) + 1)) + priceIsdn;
//							}
//							totalAmountVas += amountVas;
//							totalMoney += totalAmount;
//							logger.info("Kit Batch have value addMonth: " + bn.getAddMonth() + ", sum totalMoney with addMonth, "
//									+ "id: " + bn.getKitBatchId() + ", staffCode: " + staffCode);
//							kitBatch.setAmountTax((totalAmount + totalAmountVas) - tmpDiscountForPrepaidMonth);
//							kitBatch.getSaleServicesPrice().setPrice((totalAmount + totalAmountVas) - priceIsdn);
//							kitBatch.setDiscountForPrepaidMonth(tmpDiscountForPrepaidMonth);
                            //kitBatch.setMoneyProduct(productCon.getPrice() + "");
//						} else {
//							double amountVas = 0;
//							double totalAmount = 0;
//							if (isRegisterVas) {
//								amountVas = productCon.getPrice();
//								kitBatch.setAmountVas(productCon.getPrice());
//								//kitBatch.setMoneyProduct(productCon.getPrice() + "");
//							} else {
//								totalAmount = productCon.getPrice() + priceIsdn;
//							}
//							totalAmountVas += amountVas;
//							totalMoney += totalAmount;
//							kitBatch.setAmountTax((totalAmount + totalAmountVas) - tmpDiscountForPrepaidMonth);
//							kitBatch.getSaleServicesPrice().setPrice(totalAmount - priceIsdn);
                        }
                    }
//					else {
//						if (isAddMonth) {
//							float percentBonus = calculateBonus(kitBatch, bn.getAddMonth());
//							ProductConnectKit producCon = db.getProductConnectKit(kitBatch.getProductAddOn());
//							if (producCon != null && producCon.getPrice() > 0) {
//								double tmpDiscountForPrepaidMonthOld = Math.round(producCon.getPrice() * (Integer.parseInt(bn.getAddMonth())) * percentBonus);
//								long amountVas = (producCon.getPrice() * (Integer.parseInt(bn.getAddMonth()) + 1));
//								totalAmountVas += amountVas;
//								kitBatch.setAmountTax(totalAmountVas - tmpDiscountForPrepaidMonthOld);
//								kitBatch.setDiscountForPrepaidMonth(tmpDiscountForPrepaidMonthOld);
//								tmpDiscountForPrepaidMonthOldSub += tmpDiscountForPrepaidMonthOld;
//								//kitBatch.setMoneyProduct(producCon.getPrice() + "");
//							}
//						} else {
//							ProductConnectKit producCon = db.getProductConnectKit(kitBatch.getProductAddOn());
//							if (producCon != null && producCon.getPrice() > 0) {
//								totalMoney += producCon.getPrice();
//							}
//							//kitBatch.setMoneyProduct(producCon.getPrice() + "");
//						}

//					}
                }

//				if (isValidate) {
//					logger.warn("Validate sim, isdn before connect batch is fail, id " + bn.getKitBatchId());
//					bn.setResultCode("06");
//					bn.setDescription("Validate before connect kit by batch is fail.");
//					bn.setDuration(System.currentTimeMillis() - timeSt);
//					for (KitBatch kitBatch : lstKitBatchDetail) {
//						int rollbackIsdn = db.updateStockIsdn(1L, "SYS_ROLLBACK", kitBatch.getIsdn());
//						int rollbackSim = db.updateStockSim(0L, 1L, kitBatch.getSerial(), kitBatch.getIsdn());
//						logger.warn("create_user in kit_batch_info is null, Result rollback stockIsdnMobile: " + rollbackIsdn + " stockSim: "
//								+ rollbackSim + " for serial: " + kitBatch.getSerial()
//								+ " isdn " + kitBatch.getIsdn());
//					}
//					if ("0".equals(bn.getPayType())) {
//						logger.info("Paymethod is BankTransfer, now rollback trans: bankCode: " + bn.getBankTransCode()
//								+ ", bankName: " + bn.getBankName() + "id " + bn.getKitBatchId() + ", staff: " + staffCode);
//						String rollBackTrans = BankTransferUtils.callWSSOAProllbackTrans(bn.getBankTransCode(), bn.getBankName(),
//								bn.getBankTransAmount(), staffCode, staffCode);
//						if (!"00".equalsIgnoreCase(rollBackTrans)) {
//							logger.info("Rollback of BankDocument fail error_code: " + rollBackTrans + ", id: " + bn.getKitBatchId() + ", staffCode: " + staffCode);
//							bn.setDescription(bn.getDescription() + "--Rollback BankDocument fail");
//						} else {
//							logger.info("Rollback of BankDocument BankDocument successfully, error_code: " + rollBackTrans
//									+ "id: " + bn.getKitBatchId() + ", staffCode: " + staffCode);
//						}
//					}
//					String msg = "Fail to connect batch, because validate information before connect is fail, id " + bn.getKitBatchId();
//					db.sendSms(tel, msg, "86904");
//					continue;
//				}
//				logger.info("Money of product and isdn: " + totalMoney + "money of vas: " + totalMoney
//						+ ", money discount for prepaid months not sale handset: " + totalDiscountForPrepaidMonth
//						+ ", id: " + bn.getKitBatchId() + ", staffCode: " + staffCode);
//				long totalAmount = Math.round(totalMoney) + Math.round(totalAmountVas) + Math.round(totalPriceOfHandset) - Math.round(totalDiscountForPrepaidMonth) - Math.round(tmpDiscountForPrepaidMonthOldSub);
//				bn.setTotalMoney(totalAmount);
                //</editor-fold>
//                Step 3: Charge money one time...
                //<editor-fold defaultstate="collapsed" desc="Charge money">
//				logger.info("Total money of list KIT: " + totalMoney + ", id: " + bn.getKitBatchId() + ", staffCode: " + staffCode);
//				if (bn.getTotalMoney() > 0 && !BCCS_CM.equals(bn.getChannelType())) {
//					if ("0".equals(bn.getPayType())) {
//						logger.info("payType is BankAccountTransfer: bankName" + bn.getBankName() + ", bankDocument: " + bn.getBankTransCode()
//								+ "bankAmount: " + bn.getBankTransAmount() + ", id: " + bn.getKitBatchId() + ", staffCode: " + staffCode);
//						if (Double.valueOf(bn.getBankTransAmount()) >= bn.getTotalMoney()) {
//							logger.info("Amount of bankDocument is VALID, skip next step, id: " + bn.getKitBatchId() + ", staffCode: " + staffCode);
//							//Call API update status of bankDocument... >> USED
//							String checkTrans = BankTransferUtils.callWSSOAPupdateTrans(bn.getBankTransCode(), bn.getBankName(),
//									bn.getBankTransAmount(), staffCode, staffCode);
//							if (!"00".equalsIgnoreCase(checkTrans)) {
//								logger.info("Update status of BankDocument fail error_code: " + checkTrans + ", id: " + bn.getKitBatchId() + ", staffCode: " + staffCode);
//								bn.setResultCode("05");
//								bn.setDescription("Update status of BankDocument fail");
//								bn.setDuration(System.currentTimeMillis() - timeSt);
//								for (KitBatch kitBatch : lstKitBatchDetail) {
//									int rollbackIsdn = db.updateStockIsdn(1L, "SYS_ROLLBACK", kitBatch.getIsdn());
//									int rollbackSim = db.updateStockSim(0L, 1L, kitBatch.getSerial(), kitBatch.getIsdn());
//									logger.warn("Call WS Update status of BankDocument fail " + checkTrans + ", Result rollback stockIsdnMobile: " + rollbackIsdn + " stockSim: "
//											+ rollbackSim + " for serial: " + kitBatch.getSerial()
//											+ " isdn " + kitBatch.getIsdn());
//								}
//								if ("0".equals(bn.getPayType())) {
//									logger.info("Paymethod is BankTransfer, now rollback trans: bankCode: " + bn.getBankTransCode()
//											+ ", bankName: " + bn.getBankName() + "id " + bn.getKitBatchId() + ", staff: " + staffCode);
//									String rollBackTrans = BankTransferUtils.callWSSOAProllbackTrans(bn.getBankTransCode(), bn.getBankName(),
//											bn.getBankTransAmount(), staffCode, staffCode);
//									if (!"00".equalsIgnoreCase(rollBackTrans)) {
//										logger.info("Rollback of BankDocument fail error_code: " + rollBackTrans + ", id: " + bn.getKitBatchId() + ", staffCode: " + staffCode);
//										bn.setDescription(bn.getDescription() + "--Rollback BankDocument fail");
//									} else {
//										logger.info("Rollback of BankDocument BankDocument successfully, error_code: " + rollBackTrans
//												+ "id: " + bn.getKitBatchId() + ", staffCode: " + staffCode);
//									}
//								}
//								continue;
//							} else {
//								logger.info("Update status of BankDocument successfully, id: " + bn.getKitBatchId() + ", staffCode: " + staffCode);
//							}
//
//						} else {
////                            Rollback Trans....
//							logger.info("Amount of bankDocument is invalid, less than totalMoney, id: " + bn.getKitBatchId() + ", staffCode: " + staffCode);
//							bn.setResultCode("03");
//							bn.setDescription("Amount of bankDocument is invalid, less than totalMoney");
//							bn.setDuration(System.currentTimeMillis() - timeSt);
//							for (KitBatch kitBatch : lstKitBatchDetail) {
//								int rollbackIsdn = db.updateStockIsdn(1L, "SYS_ROLLBACK", kitBatch.getIsdn());
//								int rollbackSim = db.updateStockSim(0L, 1L, kitBatch.getSerial(), kitBatch.getIsdn());
//								logger.warn("Amount of bankDocument is invalid, Result rollback stockIsdnMobile: " + rollbackIsdn + " stockSim: "
//										+ rollbackSim + " for serial: " + kitBatch.getSerial()
//										+ " isdn " + kitBatch.getIsdn());
//							}
//							if ("0".equals(bn.getPayType())) {
//								logger.info("Paymethod is BankTransfer, now rollback trans: bankCode: " + bn.getBankTransCode()
//										+ ", bankName: " + bn.getBankName() + "id " + bn.getKitBatchId() + ", staff: " + staffCode);
//								String rollBackTrans = BankTransferUtils.callWSSOAProllbackTrans(bn.getBankTransCode(), bn.getBankName(),
//										bn.getBankTransAmount(), staffCode, staffCode);
//								if (!"00".equalsIgnoreCase(rollBackTrans)) {
//									logger.info("Rollback of BankDocument fail error_code: " + rollBackTrans + ", id: " + bn.getKitBatchId() + ", staffCode: " + staffCode);
//									bn.setDescription(bn.getDescription() + "--Rollback BankDocument fail");
//								} else {
//									logger.info("Rollback of BankDocument BankDocument successfully, error_code: " + rollBackTrans
//											+ "id: " + bn.getKitBatchId() + ", staffCode: " + staffCode);
//								}
//							}
//							String msg = "Amount of bankDocument is invalid, less than totalMoney:" + bn.getTotalMoney() + " for batch id " + bn.getKitBatchId()
//									+ " please check again.";
//							db.sendSms(tel, msg, "86904");
//							continue;
//						}
//					} else if ("2".equals(bn.getPayType()) && BCCS_CM.equals(bn.getChannelType()) || "1".equals(bn.getPayType())) {
//						ResponseWallet responseWallet = new ResponseWallet();
//						//20200226 Update payemnt by enterprise account via BCCS_CM
//						if ("2".equals(bn.getPayType()) && BCCS_CM.equals(bn.getChannelType())) {
//							logger.info("start charge money by enterprise method, id: " + bn.getKitBatchId() + ", staffCode: " + staffCode);
//							//isdnEnterprise = db.getEnterpriseWallet(bn.getKitBatchId());
//							if (isdnEnterprise == null || isdnEnterprise.isEmpty()) {
//								logger.info("Cannot get enteprise isdn " + bn.getKitBatchId());
//								responseWallet.setResponseCode("00");
//								responseWallet.setResponseMessage("Cannot get enteprise isdn " + bn.getKitBatchId());
//							} else {
//								QueryInforResponse accountInfo = EWalletUtil.queryEnterpriseAccountInfo(isdnEnterprise);
//								if (accountInfo == null || !"1".equals(accountInfo.getStatus()) || !"CO".equals(accountInfo.getChannelCode())
//										|| accountInfo.getCustomerCode() == null || accountInfo.getProviderCode() == null
//										|| accountInfo.getCustomerCode().isEmpty() || accountInfo.getProviderCode().isEmpty()) {
//									logger.info("Invalid account enterprise " + bn.getKitBatchId() + " isdnEnterprise " + isdnEnterprise);
//									responseWallet.setResponseCode("00");
//									responseWallet.setResponseMessage("Invalid account enterprise " + isdnEnterprise);
//								} else {
//									responseWallet = EWalletUtil.chargeEmolaEnterpriseV2(sdf.format(new Date()) + bn.getKitBatchId(), db,
//											isdnEnterprise, bn.getTotalMoney() * 1.00, logger, isdnEnterprise, staffCode, accountInfo.getProviderCode(), accountInfo.getCustomerCode());
//								}
//							}
//						} else if ("1".equals(bn.getPayType())) {
//							logger.info("start charge money by emola personal method, id: " + bn.getKitBatchId() + ", staffCode: " + staffCode);
//							responseWallet = EWalletUtil.paymentVoucher(db, bn, bn.getTotalMoney() * 1.00, logger, bn.getEmolaAccount());
//						}
//						if (responseWallet.getResponseCode() != null && !"01".equals(responseWallet.getResponseCode())) {
//							logger.info("Charge eMola fail, responseCode: " + responseWallet.getResponseCode() + "responseMessage: " + responseWallet.getResponseMessage()
//									+ ", staffCode: " + staffCode);
//							bn.setResultCode("04");
//							bn.setDescription("Charge totalMoney on eMola system fail." + responseWallet != null ? responseWallet.getResponseMessage() : "");
//							bn.setDuration(System.currentTimeMillis() - timeSt);
//							for (KitBatch kitBatch : lstKitBatchDetail) {
//								int rollbackIsdn = db.updateStockIsdn(1L, "SYS_ROLLBACK", kitBatch.getIsdn());
//								int rollbackSim = db.updateStockSim(0L, 1L, kitBatch.getSerial(), kitBatch.getIsdn());
//								logger.warn("Charge eMola fail, Result rollback stockIsdnMobile: " + rollbackIsdn + " stockSim: "
//										+ rollbackSim + " for serial: " + kitBatch.getSerial()
//										+ " isdn " + kitBatch.getIsdn());
//							}
//							String msg = "Fail to charge Emola for batch id " + bn.getKitBatchId()
//									+ " please check your Emola account or voucher code, detail: " + responseWallet.getResponseMessage();
//							db.sendSms(tel, msg, "86904");
//							continue;
//						}
//						orgRequestId = responseWallet.getRequestId();//RequestId using when revertTransaction
//					} else if (("2".equals(bn.getPayType()) && "mBCCS".equals(bn.getChannelType()))
//							|| "3".equals(bn.getPayType())) { //POS: 2 >> mBCCS ; 3 >> BCCS_CM
//						logger.info("staffCode: " + staffCode + " using POS to connect charge money, so no need charge money and clear debit (later), "
//								+ "referenceId: " + bn.getReferenceId() + ", id " + bn.getKitBatchId());
//					}
//				} else {
//					logger.info("Total money is 0 so no need to charge eMola id: " + bn.getKitBatchId() + ", staffCode: " + staffCode);
//				}
                //</editor-fold>
//                Step 4: Connect KIT by batch....
                //<editor-fold defaultstate="collapsed" desc="Conect kit batch">
                String shopCode = db.getShopCodeByStaffCode(staffCode);
                logger.info("Start connect KIT by batch,  kit_batch_id: " + bn.getKitBatchId() + ", staffCode: " + staffCode);
                String assignUser = "";
                List<AssignedUser> lstAssign = db.getListAssignedUser();
                if (lstAssign != null && !lstAssign.isEmpty()) {
                    Long min = lstAssign.get(0).getCount();
                    assignUser = lstAssign.get(0).getAssingedUser();
                    for (AssignedUser assign : lstAssign) {
                        if (assign.getCount() < min) {
                            min = assign.getCount();
                            assignUser = assign.getAssingedUser();
                        }
                    }
                }

                for (KitBatch kitBatch : lstKitBatchDetail) {
                    if (kitBatch.isIsConnectNew()) {
                        Long actionAuditId = db.getSequence("seq_action_audit", "cm_pre");
                        logger.info("start connect kit for isdn: " + kitBatch.getIsdn() + ", serial: " + kitBatch.getSerial()
                                + ", kit_batch_id: " + bn.getKitBatchId() + ", staffCode: " + staffCode);
//                    Step 4.1: Insert sub_mb
                        Long subId = db.getSequence("SEQ_SUB_ID", "cm_pre");
                        Customer customer = null;
                        if (BCCS_CM.equals(bn.getChannelType())) {
                            customer = db.getCorpCustomerByCustId(kitBatch.getCustId());
                        } else {
                            customer = db.getCustomerByCustIdCmPre(kitBatch.getCustId(), bn.getChannelType());
                        }

                        if (customer == null) {
                            logger.info("Can not get Customer Info, productCode: " + kitBatch.getProductCode() + "serial: " + kitBatch.getSerial()
                                    + ", isdn: " + kitBatch.getIsdn() + ", staff: " + staffCode);
                            db.updateKitBatchDetail(bn.getKitBatchId(), kitBatch.getSerial(), kitBatch.getIsdn(), "ERR_16",
                                    "Can not get Customer Info", bn.getNodeName(), kitBatch.getMoneyProduct(), kitBatch.getMoneyIsdn());
                            int rollbackIsdn = db.updateStockIsdn(1L, "SYS_ROLLBACK", kitBatch.getIsdn());
                            int rollbackSim = db.updateStockSim(0L, 1L, kitBatch.getSerial(), kitBatch.getIsdn());
                            logger.warn("Can not get Customer Info, Result rollback stockIsdnMobile: " + rollbackIsdn + " stockSim: "
                                    + rollbackSim + " for serial: " + kitBatch.getSerial()
                                    + " isdn " + kitBatch.getIsdn());
                            bn.setDuration(System.currentTimeMillis() - timeSt);

                            continue;
                        }
                        ProductConnectKit product = db.getProductConnectKit(kitBatch.getProductCode());
                        if (product == null) {
                            logger.info("Can not get ProductConnectKit, productCode: " + kitBatch.getProductCode() + "serial: " + kitBatch.getSerial()
                                    + ", isdn: " + kitBatch.getIsdn() + ", staff: " + staffCode);
                            db.updateKitBatchDetail(bn.getKitBatchId(), kitBatch.getSerial(), kitBatch.getIsdn(), "ERR_07",
                                    "Can not get ProductConnectKit", bn.getNodeName(), kitBatch.getMoneyProduct(), kitBatch.getMoneyIsdn());
                            int rollbackIsdn = db.updateStockIsdn(1L, "SYS_ROLLBACK", kitBatch.getIsdn());
                            int rollbackSim = db.updateStockSim(0L, 1L, kitBatch.getSerial(), kitBatch.getIsdn());
                            logger.warn("Can not get ProductConnectKit, Result rollback stockIsdnMobile: " + rollbackIsdn + " stockSim: "
                                    + rollbackSim + " for serial: " + kitBatch.getSerial()
                                    + " isdn " + kitBatch.getIsdn());
                            bn.setDuration(System.currentTimeMillis() - timeSt);

                            continue;
                        }
                        String simInfo = db.getImsiEkiSimBySerial(kitBatch.getSerial(), kitBatch.getIsdn());
                        String imsiOfSim = "", ekiValue = "";
                        if (simInfo != null && simInfo.length() > 0) {
                            String[] info = simInfo.split("\\|");
                            imsiOfSim = info[0];
                            ekiValue = info[1];
                        } else {
                            logger.info("IMSI|EKI is empty, productCode: " + kitBatch.getProductCode() + "serial: " + kitBatch.getSerial()
                                    + ", isdn: " + kitBatch.getIsdn() + ", staff: " + staffCode);
                            db.updateKitBatchDetail(bn.getKitBatchId(), kitBatch.getSerial(), kitBatch.getIsdn(), "ERR_08",
                                    "Can not get information of sim (IMSI|EKI)", bn.getNodeName(), kitBatch.getMoneyProduct(), kitBatch.getMoneyIsdn());
                            int rollbackIsdn = db.updateStockIsdn(1L, "SYS_ROLLBACK", kitBatch.getIsdn());
                            int rollbackSim = db.updateStockSim(0L, 1L, kitBatch.getSerial(), kitBatch.getIsdn());
                            logger.warn("Can not get information of sim (IMSI|EKI), Result rollback stockIsdnMobile: " + rollbackIsdn + " stockSim: "
                                    + rollbackSim + " for serial: " + kitBatch.getSerial()
                                    + " isdn " + kitBatch.getIsdn());
                            bn.setDuration(System.currentTimeMillis() - timeSt);

                            continue;
                        }
                        boolean isActive = false;
                        if (kitBatch.getProductAddOn() != null && !kitBatch.getProductAddOn().trim().isEmpty()) {
                            isActive = true;
                        }
                        int rsSubMb = db.insertSubMbCmPre(subId, kitBatch.getCustId(), kitBatch.getIsdn(), customer.getSubName(), imsiOfSim, kitBatch.getSerial(),
                                product.getOfferId(), kitBatch.getProductCode(), staffCode, shopCode, isActive);
                        if (rsSubMb != 1) {
                            logger.info("Cannot create sub_mb record, productCode: " + kitBatch.getProductCode() + "serial: " + kitBatch.getSerial()
                                    + ", isdn: " + kitBatch.getIsdn() + ", staff: " + staffCode);
                            db.updateKitBatchDetail(bn.getKitBatchId(), kitBatch.getSerial(), kitBatch.getIsdn(), "ERR_09",
                                    "Cannot create sub_mb record", bn.getNodeName(), kitBatch.getMoneyProduct(), kitBatch.getMoneyIsdn());
                            int rollbackIsdn = db.updateStockIsdn(1L, "SYS_ROLLBACK", kitBatch.getIsdn());
                            int rollbackSim = db.updateStockSim(0L, 1L, kitBatch.getSerial(), kitBatch.getIsdn());
                            logger.warn("Cannot create sub_mb record, Result rollback stockIsdnMobile: " + rollbackIsdn + " stockSim: "
                                    + rollbackSim + " for serial: " + kitBatch.getSerial()
                                    + " isdn " + kitBatch.getIsdn());
                            bn.setDuration(System.currentTimeMillis() - timeSt);

                            continue;
                        }
//                    Step 4.2: Insert sub_profile_info
                        Long subProfileId = db.getSequence("sub_profile_info_seq", "cm_pre");
                        kitBatch.setSubProfileId(subProfileId);
                        int rsSubProfile = 0;
                        if (BCCS_CM.equals(bn.getChannelType())) {
                            rsSubProfile = db.insertSubProfileInfo(subProfileId, subId, kitBatch.getCustId(), actionAuditId, kitBatch.getIsdn(),
                                    staffCode, shopCode, customer.getFrontImageUrl(), customer.getBackImageUrl(), customer.getFormImageUrl(), "SYSTEM_AUTO",
                                    "615", 2926L, kitBatch.getSerial(), customer.getSubName(), customer.getIdNo(), bn.getKitBatchId(), "0", kitBatch.getProductAddOn(), true);
                        } else {
                            if ("CAMP".equals(customer.getBusType())) {
                                rsSubProfile = db.insertSubProfileInfo(subProfileId, subId, kitBatch.getCustId(), actionAuditId, kitBatch.getIsdn(),
                                        staffCode, shopCode, customer.getFrontImageUrl(), customer.getBackImageUrl(), customer.getFormImageUrl(), assignUser,
                                        "615", 2926L, kitBatch.getSerial(), customer.getSubName(), customer.getIdNo(), bn.getKitBatchId(), "0", kitBatch.getProductAddOn(), false);
                            } else {
                                rsSubProfile = db.insertSubProfileInfo(subProfileId, subId, kitBatch.getCustId(), actionAuditId, kitBatch.getIsdn(),
                                        staffCode, shopCode, customer.getFrontImageUrl(), customer.getBackImageUrl(), customer.getFormImageUrl(), assignUser,
                                        "615", 2926L, kitBatch.getSerial(), customer.getSubName(), customer.getIdNo(), bn.getKitBatchId(), "", kitBatch.getProductAddOn(), false);
                            }
                        }

                        if (rsSubProfile != 1) {
                            int rollbackSubMb = db.destroySubMbRecord(kitBatch.getSerial(), kitBatch.getIsdn());
                            int rollbackIsdn = db.updateStockIsdn(1L, "SYS_ROLLBACK", kitBatch.getIsdn());
                            int rollbackSim = db.updateStockSim(0L, 1L, kitBatch.getSerial(), kitBatch.getIsdn());
                            int rollBackSubProfile = db.deleteSubprofileInfo(subProfileId);
                            logger.info("Fail to insert sub_profile_info, Result rollback SubMb" + rollbackSubMb
                                    + " stockIsdnMobile: " + rollbackIsdn + " stockSim: "
                                    + rollbackSim + " rollBackSubProfile " + rollBackSubProfile + " for serial: " + kitBatch.getSerial()
                                    + " isdn " + kitBatch.getIsdn());
                            db.updateKitBatchDetail(bn.getKitBatchId(), kitBatch.getSerial(), kitBatch.getIsdn(), "ERR_10",
                                    "Cannot create sub_profile_info record", bn.getNodeName(), kitBatch.getMoneyProduct(), kitBatch.getMoneyIsdn());
                            bn.setDuration(System.currentTimeMillis() - timeSt);

                            continue;
                        } else {
                            logger.info("Create sub_profile_info for sub successfully, serial: " + kitBatch.getSerial()
                                    + ", isdn: " + kitBatch.getIsdn() + ", staff: " + staffCode + ", kitBatchId: " + bn.getKitBatchId());
                        }
                        kitBatch.setActionAuditId(actionAuditId);
//                    db.insertActionAudit(actionAuditId, kitBatch.getIsdn(), kitBatch.getSerial(),
//                            "Create Sub_mb record, sub_profile_record successfully.", subId, shopCode, staffCode);
                        String mainProduct = db.getMainProduct(kitBatch.getProductCode(), kitBatch.getIsdn());
                        if (mainProduct == null) {
                            int rollbackSubMb = db.destroySubMbRecord(kitBatch.getSerial(), kitBatch.getIsdn());
                            int rollbackIsdn = db.updateStockIsdn(1L, "SYS_ROLLBACK", kitBatch.getIsdn());
                            int rollbackSim = db.updateStockSim(0L, 1L, kitBatch.getSerial(), kitBatch.getIsdn());
                            int rollBackSubProfile = db.deleteSubprofileInfo(subProfileId);
                            logger.info("Fail to get getMainProduct of mainProduct: " + kitBatch.getProductCode() + ""
                                    + " Result rollback SubMb" + rollbackSubMb
                                    + " stockIsdnMobile: " + rollbackIsdn + " stockSim: "
                                    + rollbackSim + " rollBackSubProfile " + rollBackSubProfile + " for serial: " + kitBatch.getSerial()
                                    + " isdn " + kitBatch.getIsdn());
                            db.updateKitBatchDetail(bn.getKitBatchId(), kitBatch.getSerial(), kitBatch.getIsdn(), "ERR_11",
                                    "Cannot get price_plan_code of product", bn.getNodeName(), kitBatch.getMoneyProduct(), kitBatch.getMoneyIsdn());
                            bn.setDuration(System.currentTimeMillis() - timeSt);
                            bn.setDuration(System.currentTimeMillis() - timeSt);

                            continue;
                        }
                        Long k4sNO = db.getK4SNO(imsiOfSim);
                        if (k4sNO == null) {
                            logger.info("k4sNO is null so set default k4sNO = 2 " + kitBatch.getIsdn());
                            k4sNO = 2L;
                        }
                        isSim4G = db.isSim4G(kitBatch.getSerial(), 207607L);
                        String checkKI = pro.checkKiSim("258" + kitBatch.getIsdn(), imsiOfSim);
                        //Error when query KI >>> KI not load...etc....
                        if (!"0".equals(checkKI)) {
                            if ("ERR3048".equals(checkKI)) {
                                //KI not load >>> Add KI
                                String addKI = pro.addKiSim("258" + kitBatch.getIsdn(), ekiValue, imsiOfSim, k4sNO + "", isSim4G);
                                if (!"0".equals(addKI)) {
                                    logger.error("Fail to connectKit --- add KI fail" + kitBatch.getIsdn());
                                    int rollbackSubMb = db.destroySubMbRecord(kitBatch.getSerial(), kitBatch.getIsdn());
                                    int rollbackIsdn = db.updateStockIsdn(1L, "SYS_ROLLBACK", kitBatch.getIsdn());
                                    int rollbackSim = db.updateStockSim(0L, 1L, kitBatch.getSerial(), kitBatch.getIsdn());
                                    int rollBackSubProfile = db.deleteSubprofileInfo(subProfileId);
                                    //Update status sub_id_no ve 0
                                    logger.warn("Fail to connectKit --- add KI fail, Result rollback: SubMb " + rollbackSubMb + " stockIsdnMobile: "
                                            + rollbackIsdn + " stockSim: " + rollbackSim + " for serial: "
                                            + kitBatch.getSerial() + " isdn " + kitBatch.getIsdn() + " rollBackSubProfile " + rollBackSubProfile);
                                    db.updateKitBatchDetail(bn.getKitBatchId(), kitBatch.getSerial(), kitBatch.getIsdn(), "ERR_12",
                                            "Add KI for sim not successfully.", bn.getNodeName(), kitBatch.getMoneyProduct(), kitBatch.getMoneyIsdn());
                                    bn.setDuration(System.currentTimeMillis() - timeSt);
                                    continue;
                                }
                            } else {
                                logger.error("Fail to connectKit ---- Error isn't KI not load" + kitBatch.getIsdn());
                                int rollbackSubMb = db.destroySubMbRecord(kitBatch.getSerial(), kitBatch.getIsdn());
                                int rollbackIsdn = db.updateStockIsdn(1L, "SYS_ROLLBACK", kitBatch.getIsdn());
                                int rollbackSim = db.updateStockSim(0L, 1L, kitBatch.getSerial(), kitBatch.getIsdn());
                                int rollBackSubProfile = db.deleteSubprofileInfo(subProfileId);
                                //Update status sub_id_no ve 0
                                logger.warn("Fail to connectKit ---- Error when check KI of sim, errCode: " + checkKI + "sub_mb: " + rollbackSubMb + " stockIsdnMobile: "
                                        + rollbackIsdn + " stockSim: " + rollbackSim + " for serial: "
                                        + kitBatch.getSerial() + " isdn " + kitBatch.getIsdn() + "rollBackSubProfile " + rollBackSubProfile);
                                db.updateKitBatchDetail(bn.getKitBatchId(), kitBatch.getSerial(), kitBatch.getIsdn(), "ERR_13",
                                        "Error when check KI of sim, errCode: " + checkKI, bn.getNodeName(), kitBatch.getMoneyProduct(), kitBatch.getMoneyIsdn());
                                bn.setDuration(System.currentTimeMillis() - timeSt);

                                continue;
                            }
                        }
                        String tplId = db.getTPLID(kitBatch.getIsdn(), isSim4G);
                        if (tplId == null) {
                            logger.error("Fail to connectKit ---- Error when get TPLID" + kitBatch.getIsdn());
                            int rollbackSubMb = db.destroySubMbRecord(kitBatch.getSerial(), kitBatch.getIsdn());
                            int rollbackIsdn = db.updateStockIsdn(1L, "SYS_ROLLBACK", kitBatch.getIsdn());
                            int rollbackSim = db.updateStockSim(0L, 1L, kitBatch.getSerial(), kitBatch.getIsdn());
                            int rollBackSubProfile = db.deleteSubprofileInfo(subProfileId);
                            //Update status sub_id_no ve 0
                            logger.warn("Fail to connectKit ---- Error when get TPLID, sub_mb: " + rollbackSubMb + " stockIsdnMobile: "
                                    + rollbackIsdn + " stockSim: " + rollbackSim + " for serial: "
                                    + kitBatch.getSerial() + " isdn " + kitBatch.getIsdn() + " rollBackSubProfile " + rollBackSubProfile);
                            db.updateKitBatchDetail(bn.getKitBatchId(), kitBatch.getSerial(), kitBatch.getIsdn(), "ERR_14",
                                    "Error when get TPLID.", bn.getNodeName(), null, null);
                            bn.setDuration(System.currentTimeMillis() - timeSt);

                            continue;
                        }
//
                        String resultActive = services.connectKit(mainProduct, kitBatch.getProductCode(), "258" + kitBatch.getIsdn(),
                                kitBatch.getSerial(), imsiOfSim, ekiValue, db, shopCode, staffCode,
                                String.valueOf(k4sNO), tplId);
                        //Bacnx
//						String resultActive = "0";
                        if (!"0".equals(resultActive)) {
                            int rollbackSubMb = db.destroySubMbRecord(kitBatch.getSerial(), kitBatch.getIsdn());
                            int rollbackIsdn = db.updateStockIsdn(1L, "SYS_ROLLBACK", kitBatch.getIsdn());
                            int rollbackSim = db.updateStockSim(0L, 1L, kitBatch.getSerial(), kitBatch.getIsdn());
                            int rollBackSubProfile = db.deleteSubprofileInfo(subProfileId);
                            //Update status sub_id_no ve 0
                            logger.warn("Fail to connectKit, Result rollback: SubMb " + rollbackSubMb + " stockIsdnMobile: "
                                    + rollbackIsdn + " stockSim: " + rollbackSim + " for serial: "
                                    + kitBatch.getSerial() + " isdn " + kitBatch.getIsdn() + "rollBackSubProfile " + rollBackSubProfile);
                            db.updateKitBatchDetail(bn.getKitBatchId(), kitBatch.getSerial(), kitBatch.getIsdn(), "ERR_15",
                                    "Fail to connectKit.", bn.getNodeName(), kitBatch.getMoneyProduct(), kitBatch.getMoneyIsdn());
                            bn.setDuration(System.currentTimeMillis() - timeSt);

                            continue;
                        } else {
                            totalSuccess++;
                            logger.info("Connect Kit successfully " + kitBatch.getIsdn()
                                    + " now save log, make sale trans, register Vas and paybonus");
                            db.updateKitBatchDetail(bn.getKitBatchId(), kitBatch.getSerial(), kitBatch.getIsdn(), "0",
                                    "Connect Kit successfully.", bn.getNodeName(), kitBatch.getMoneyProduct(), kitBatch.getMoneyIsdn());
                            kitBatch.setResultCode("0");
                        }
                        if (db.checkVipProductConnectKit(kitBatch.getProductCode())) {
                            String strTplId3G, strTmpId4G;
                            if (Long.valueOf(kitBatch.getMoneyProduct()) >= 1200) {
                                strTmpId4G = "1";
                                strTplId3G = "36";
                            } else if (Long.valueOf(kitBatch.getMoneyProduct()) >= 500) {
                                strTmpId4G = "2";
                                strTplId3G = "37";
                            } else {
                                strTmpId4G = "3";
                                strTplId3G = "38";
                            }
//                        LinhNBV 20190617: Only call MODI_GPRS for VIP sim
                            if (!"38".equals(strTplId3G)) {
                                openFlagMODIGPRSResult = pro.activeFlagMODIGPRS("258" + kitBatch.getIsdn(), strTplId3G);
                                if ("0".equals(openFlagMODIGPRSResult)) {
                                    logger.info("Open flag MODI_GPRS successfully for sub when connect kit  " + kitBatch.getIsdn());
                                } else {
                                    logger.warn("Fail to open flag MODI_GPRS for sub when connect kit  " + kitBatch.getIsdn());
                                }
                            }

                            if (isSim4G && !"3".equals(strTmpId4G)) {
                                logger.info("productCode is Vip and is Sim 4G, start add command MOD_TPLOPTGPRS with tmpId = 1, isdn: " + kitBatch.getIsdn());
                                String modTPLOPTGPRSResult = pro.modTPLOPTGPRS("258" + kitBatch.getIsdn(), "TRUE", strTmpId4G, "TRUE");
                                if ("0".equals(modTPLOPTGPRSResult)) {
                                    logger.info("Open flag MOD_TPLOPTGPRS successfully for sub when connect kit  " + kitBatch.getIsdn());
                                } else {
                                    logger.warn("Fail to open flag MOD_TPLOPTGPRS for sub when connect kit  " + kitBatch.getIsdn());
                                }
                            }
                            if (!"DATA_SIM".equals(kitBatch.getProductCode())) {
                                db.insertDataKitVas(kitBatch.getIsdn(), kitBatch.getSerial(), staffCode.toUpperCase(), kitBatch.getProductCode());
                            } else {
                                logger.warn("Product code:  " + kitBatch.getProductCode() + " is not VIP. "
                                        + "No need to insert kit_vas table. " + kitBatch.getIsdn() + ", id: " + bn.getKitBatchId() + ", staffCode: " + staffCode);
                            }
                        }
//                    Step 3: Chan BAOC, BAIC
                        blockOneWay(kitBatch.getIsdn(), isSim4G);
                        //}
                        //Update status SIM (SOLD,HLR), ISDN --> (USING))
                        db.updateStockIsdn(2L, "SYSTEM_AUTO", kitBatch.getIsdn());
                        db.updateStockSim(2L, 2L, kitBatch.getSerial(), kitBatch.getIsdn());
                        Comission comission = null;
                        if (kitBatch.getProductAddOn() != null && !kitBatch.getProductAddOn().trim().isEmpty()) {
                            comission = db.getComissionStaff(staffCode, kitBatch.getProductAddOn(), kitBatch.getIsdn());
                        } else {
                            comission = db.getComissionStaff(staffCode, kitBatch.getProductCode(), kitBatch.getIsdn());
                        }
                        if (comission != null) {//LinhNBV 20180903: DATA_SIM is not vip product, not need insert table kit_vas.
                            long tmpComission = 0;
                            if (isStaff) {
                                if (isAddMonth) {
                                    tmpComission = comission.getBonusCtv() + Math.round(0.05 * Double.valueOf(kitBatch.getMoneyProduct()) * (Integer.parseInt(bn.getAddMonth())));
                                } else {
                                    tmpComission = comission.getBonusCtv();
                                }

                            } else {
                                if (isAddMonth) {
                                    tmpComission = comission.getBonusChannel() + Math.round(0.9 * (0.05 * Double.valueOf(kitBatch.getMoneyProduct()) * (Integer.parseInt(bn.getAddMonth()))));
                                } else {
                                    tmpComission = comission.getBonusChannel();
                                }
                            }
                            totalCommission += tmpComission;
//                        totalCommission += comission.getBonusCenter();
                            logger.info("Commssion of product: " + kitBatch.getProductCode() + " is: " + comission.getBonusCenter()
                                    + ", totalCommission: " + totalCommission + ", id: " + bn.getKitBatchId() + ", staffCode: " + staffCode);
                        } else {
                            logger.warn("Product code:  " + kitBatch.getProductCode() + " is not VIP. "
                                    + "Don't have commission value. " + kitBatch.getIsdn() + ", id: " + bn.getKitBatchId() + ", staffCode: " + staffCode);
                        }
                        //update connect_kit_status = 1 >> already connect
                        db.updateConnectKitStatus(subProfileId);
                        //Make sale trans
                        if (!BCCS_CM.equals(bn.getChannelType())) {
                            Long saleTransId = db.getSequence("SALE_TRANS_SEQ", "dbsm");
                            String strTempId = db.getShopIdStaffIdByStaffCode(staffCode);
                            String[] arrTempId = strTempId.split("\\|");
                            int rsMakeSaleTrans = db.insertSaleTrans(saleTransId, Long.valueOf(arrTempId[0]), Long.valueOf(arrTempId[1]),
                                    0L, 0L, kitBatch.getAmountTax(), subId,
                                    kitBatch.getIsdn(), kitBatch.getReasonId(), orgRequestId, Integer.parseInt(bn.getPayType()),
                                    bn.getBankTransAmount(), kitBatch.getDiscountForPrepaidMonth(), kitBatch.getAmountVas(), kitBatch.getKitBatchId());
                            if (rsMakeSaleTrans != 1) {
                                //Insert fail, insert log, send sms
                                db.insertLogMakeSaleTransFail(saleTransId, kitBatch.getIsdn(), kitBatch.getSerial(), Long.valueOf(arrTempId[0]),
                                        Long.valueOf(arrTempId[1]), kitBatch.getSaleServices().getSaleServicesId(), kitBatch.getSaleServicesPrice().getSaleServicesPriceId(),
                                        kitBatch.getReasonId(), 0L, 0L, "SALE_TRANS");
                                for (String isdn : arrIsdnReceiveError) {
                                    db.sendSms(isdn, "Make saleTrans fail, more detail in table: kit_make_sale_trans_fail. saleTransId: "
                                            + saleTransId, "86142");
                                }
                            }
                            //Make sale trans order if payment by bankTransfer...
                            if ("0".equals(bn.getPayType()) && bn.getBankName() != null && bn.getBankName().length() > 0
                                    && bn.getBankTransAmount() != null && bn.getBankTransAmount().length() > 0
                                    && bn.getBankTransCode() != null && bn.getBankTransCode().length() > 0) {
                                int rsMakeSaleTransOrder = db.insertSaleTransOrder(bn.getBankName(), bn.getBankTransAmount(), bn.getBankTransCode(), saleTransId);
                                if (rsMakeSaleTransOrder != 1) {
                                    //Insert fail, insert log, send sms
                                    db.insertLogMakeSaleTransFail(saleTransId, kitBatch.getIsdn(), kitBatch.getSerial(), Long.valueOf(arrTempId[0]),
                                            Long.valueOf(arrTempId[1]), kitBatch.getSaleServices().getSaleServicesId(), kitBatch.getSaleServicesPrice().getSaleServicesPriceId(),
                                            kitBatch.getReasonId(), 0L, 0L, "SALE_TRANS_ORDER");
                                    for (String isdn : arrIsdnReceiveError) {
                                        db.sendSms(isdn, "Make saleTransOrder fail, more detail in table: kit_make_sale_trans_fail. saleTransId: "
                                                + saleTransId + ", kitBatchId: " + bn.getKitBatchId(), "86142");
                                    }
                                }
                            }
                            //Make sale trans detail
                            //1. Detail for SaleServices
                            Long saleTransDetailSaleService = db.getSequence("SALE_TRANS_DETAIL_SEQ", "dbsm");
                            int rsSaleTransDetailSaleServices = 0;
//                    if (isAddMonth) {
                            rsSaleTransDetailSaleServices = db.insertSaleTransDetail(saleTransDetailSaleService, saleTransId, "208309", "520784", "",//stockModelId = 208309 for emola scratch card connect, priceId = 520784 --> For price of emola scratch card
                                    "", "", "", "", "", "", "", "", "", "", "17",
                                    "1", "", (kitBatch.getAmountTax() + kitBatch.getDiscountForPrepaidMonth() - kitBatch.getPriceOfIsdn().getPrice()), kitBatch.getDiscountForPrepaidMonth(),
                                    (kitBatch.getAmountTax().longValue() + Math.round(kitBatch.getDiscountForPrepaidMonth()) - kitBatch.getPriceOfIsdn().getPrice().longValue()));
//                    } else {
//
//                        rsSaleTransDetailSaleServices = db.insertSaleTransDetail(saleTransDetailSaleService, saleTransId, "208309", "520784", "",//stockModelId = 208309 for emola scratch card connect, priceId = 520784 --> For price of emola scratch card
//                                "", "", "", "", "", "", "", "", "", "", "17",
//                                "1", "", (kitBatch.getAmountTax() - kitBatch.getPriceOfIsdn().getPrice()), kitBatch.getDiscountForPrepaidMonth(), (kitBatch.getAmountTax().longValue() - kitBatch.getPriceOfIsdn().getPrice().longValue()));
//
//                    }

                            if (rsSaleTransDetailSaleServices != 1) {
                                //Insert fail, insert log, send sms
                                db.insertLogMakeSaleTransFail(saleTransId, kitBatch.getIsdn(), kitBatch.getSerial(), Long.valueOf(arrTempId[0]),
                                        Long.valueOf(arrTempId[1]), kitBatch.getSaleServices().getSaleServicesId(), kitBatch.getSaleServicesPrice().getSaleServicesPriceId(),
                                        kitBatch.getReasonId(), saleTransDetailSaleService, 0L, "SALE_TRANS_DETAIL");
                                for (String isdn : arrIsdnReceiveError) {
                                    db.sendSms(isdn, "Make saleTransDetail for SaleServices fail, more detail in table: kit_make_sale_trans_fail. saleTransDetailId: " + saleTransDetailSaleService, "86142");
                                }
                            }
                            //2. Detail for Isdn
                            Long stockModelId = db.getStockModelIdByIsdn(kitBatch.getIsdn());
                            StockModel stockModel = db.findStockModelById(stockModelId);
                            Long saleTransDetailIsdn = db.getSequence("SALE_TRANS_DETAIL_SEQ", "dbsm");
//                    int rsSaleTransDetailIsdn = db.insertSaleTransDetail(saleTransDetailIsdn, saleTransId, String.valueOf(stockModelId),
//                            String.valueOf(kitBatch.getPriceOfIsdn().getPriceId()), String.valueOf(kitBatch.getSaleServices().getSaleServicesId()), "",
//                            "1", "Mobile Number", stockModel.getStockModelCode(), stockModel.getName(), "", "", stockModel.getAccountingModelCode(), stockModel.getAccountingModelName(), "", "17",
//                            String.valueOf(kitBatch.getPriceOfIsdn().getPrice()), "", kitBatch.getPriceOfIsdn().getPrice(), kitBatch.getDiscountAmount());
                            int rsSaleTransDetailIsdn = db.insertSaleTransDetail(saleTransDetailIsdn, saleTransId, String.valueOf(stockModelId),
                                    String.valueOf(kitBatch.getPriceOfIsdn().getPriceId()), "", "",
                                    "1", "Mobile Number", stockModel.getStockModelCode(), stockModel.getName(), "", "", stockModel.getAccountingModelCode(), stockModel.getAccountingModelName(), "", "17",
                                    String.valueOf(kitBatch.getPriceOfIsdn().getPriceId()), "", kitBatch.getPriceOfIsdn().getPrice(), kitBatch.getDiscountAmount(), 1L);
                            if (rsSaleTransDetailIsdn != 1) {
                                //Insert fail, insert log, send sms
                                db.insertLogMakeSaleTransFail(saleTransId, kitBatch.getIsdn(), kitBatch.getSerial(), Long.valueOf(arrTempId[0]),
                                        Long.valueOf(arrTempId[1]), kitBatch.getSaleServices().getSaleServicesId(), kitBatch.getSaleServicesPrice().getSaleServicesPriceId(),
                                        kitBatch.getReasonId(), saleTransDetailIsdn, 0L, "SALE_TRANS_DETAIL");
                                for (String isdn : arrIsdnReceiveError) {
                                    db.sendSms(isdn, "Make saleTransDetail for Isdn fail, more detail in table: kit_make_sale_trans_fail. saleTransDetailId: " + saleTransDetailIsdn, "86142");
                                }
                            }
                            //Make saleTransSerial
                            Long saleTransSerialId = db.getSequence("SALE_TRANS_SERIAL_SEQ", "dbsm");
                            int rsSaleTransSerial = db.insertSaleTransSerial(saleTransSerialId, saleTransDetailIsdn, stockModelId, kitBatch.getIsdn());
                            if (rsSaleTransSerial != 1) {
                                //Insert fail, insert log, send sms
                                db.insertLogMakeSaleTransFail(0L, kitBatch.getIsdn(), kitBatch.getSerial(), Long.valueOf(arrTempId[0]),
                                        Long.valueOf(arrTempId[1]), kitBatch.getSaleServices().getSaleServicesId(), kitBatch.getSaleServicesPrice().getSaleServicesPriceId(),
                                        kitBatch.getReasonId(), saleTransDetailIsdn, saleTransSerialId, "SALE_TRANS_SERIAL");
                                for (String isdn : arrIsdnReceiveError) {
                                    db.sendSms(isdn, "Make saleTransSerial fail, more detail in table: kit_make_sale_trans_fail. saleTransSerialId: " + saleTransSerialId, "86142");
                                }
                            }
                            //Make saleTrans for sale handset...
                            if (kitBatch.getHandsetSerial() != null && kitBatch.getHandsetSerial().length() > 0
                                    && Integer.parseInt(bn.getAddMonth()) >= 11) {
                                logger.info("start sale handset with discount, no need to calculate discount for prepaid: "
                                        + kitBatch.getHandsetSerial() + ", isdn in batch:  " + kitBatch.getIsdn());
                                //                        Step 1: Get information of stockModel by stockModelId
                                Long stockModelIdHandset = db.getStockModelIdHandset(kitBatch.getHandsetSerial());
                                StockModel stockModelHandset = db.findStockModelById(stockModelIdHandset);
                                if (stockModelHandset != null) {
//                            Step 2: find price for sale (saleRetail)
                                    Long pricePolicy = db.getPricePolicyByStaffCode(staffCode);
                                    Price priceRetail = db.getPriceForSaleRetail(stockModelIdHandset, pricePolicy);
                                    if (priceRetail != null) {
//                                Step 3: getPriceDiscount
                                        double discountAmountHandset = db.getDiscountForHandset(kitBatch.getProductCode());
//                                Step 3.1: Make SaleTrans, SaleTransDetail, SaleTransSerial...
                                        Long saleTransIdHandset = db.getSequence("SALE_TRANS_SEQ", "dbsm");
                                        String strTempIdHandset = db.getShopIdStaffIdByStaffCode(staffCode);//SHOP_ID|STAFF_ID
                                        String[] arrTempIdHandset = strTempIdHandset.split("\\|");
                                        int rsMakeSaleTransHandset = db.insertSaleTrans(saleTransIdHandset, Long.valueOf(arrTempIdHandset[0]), Long.valueOf(arrTempIdHandset[1]),
                                                0L, 0L, (priceRetail.getPrice() - discountAmountHandset), 0L,
                                                "", 200351L, "", 2, "", discountAmountHandset, kitBatch.getAmountVas(), kitBatch.getKitBatchId());
                                        if (rsMakeSaleTransHandset != 1) {
                                            //Insert fail, insert log, send sms
                                            db.insertLogMakeSaleTransFail(saleTransIdHandset, kitBatch.getIsdn(), kitBatch.getSerial(), Long.valueOf(arrTempIdHandset[0]),
                                                    Long.valueOf(arrTempIdHandset[1]), 0L, 0L,
                                                    200351L, 0L, 0L, "SALE_TRANS");
                                            for (String isdn : arrIsdnReceiveError) {
                                                db.sendSms(isdn, "Make saleTrans for sale handset in kit batch fail, more detail in table: kit_make_sale_trans_fail. saleTransId: "
                                                        + saleTransIdHandset, "86142");
                                            }
                                        }
                                        //Make sale trans order if payment by bankTransfer...
                                        if ("0".equals(bn.getPayType()) && bn.getBankName() != null && bn.getBankName().length() > 0
                                                && bn.getBankTransAmount() != null && bn.getBankTransAmount().length() > 0
                                                && bn.getBankTransCode() != null && bn.getBankTransCode().length() > 0) {
                                            int rsMakeSaleTransOrder = db.insertSaleTransOrder(bn.getBankName(), bn.getBankTransAmount(), bn.getBankTransCode(), saleTransIdHandset);
                                            if (rsMakeSaleTransOrder != 1) {
                                                //Insert fail, insert log, send sms
                                                db.insertLogMakeSaleTransFail(saleTransIdHandset, kitBatch.getIsdn(), kitBatch.getSerial(), Long.valueOf(arrTempIdHandset[0]),
                                                        Long.valueOf(arrTempIdHandset[1]), 0L, 0L,
                                                        200351L, 0L, 0L, "SALE_TRANS_ORDER");
                                                for (String isdn : arrIsdnReceiveError) {
                                                    db.sendSms(isdn, "Make saleTransOrder fail, more detail in table: kit_make_sale_trans_fail. saleTransId: "
                                                            + saleTransId + ", kitBatchId: " + bn.getKitBatchId(), "86142");
                                                }
                                            }
                                        }
                                        //Make sale trans detail
                                        //1. Detail for SaleServices
                                        Long saleTransDetailSaleServiceHandset = db.getSequence("SALE_TRANS_DETAIL_SEQ", "dbsm");
//                                int rsSaleTransDetailSaleServicesHandset = db.insertSaleTransDetail(saleTransDetailSaleServiceHandset, saleTransIdHandset, String.valueOf(stockModelIdHandset), String.valueOf(priceRetail.getPriceId()), "",
//                                        "", "7", "Handset", stockModelHandset.getAccountingModelCode(), stockModelHandset.getAccountingModelName(), "", "",
//                                        stockModelHandset.getAccountingModelCode(), stockModelHandset.getAccountingModelName(), "17", "",
//                                        "", String.valueOf(priceRetail.getPrice()), priceRetail.getPrice(), discountAmountHandset);
                                        int rsSaleTransDetailSaleServicesHandset = db.insertSaleTransDetail(saleTransDetailSaleServiceHandset, saleTransIdHandset, String.valueOf(stockModelIdHandset), String.valueOf(priceRetail.getPriceId()), "",
                                                "", "7", "Handset", stockModelHandset.getAccountingModelCode(), stockModelHandset.getAccountingModelName(), "", "",
                                                stockModelHandset.getAccountingModelCode(), stockModelHandset.getAccountingModelName(), "", "17",
                                                String.valueOf(priceRetail.getPrice()), "", priceRetail.getPrice(), 0.0, 1L);
                                        if (rsSaleTransDetailSaleServicesHandset != 1) {
                                            //Insert fail, insert log, send sms
                                            db.insertLogMakeSaleTransFail(saleTransIdHandset, kitBatch.getIsdn(), kitBatch.getSerial(), Long.valueOf(arrTempIdHandset[0]),
                                                    Long.valueOf(arrTempIdHandset[1]), 0L, 0L,
                                                    200351L, saleTransDetailSaleServiceHandset, 0L, "SALE_TRANS_DETAIL");
                                            for (String isdn : arrIsdnReceiveError) {
                                                db.sendSms(isdn, "Make saleTransDetail for sale handset in kit batch fail, more detail in table: kit_make_sale_trans_fail. saleTransDetailId: " + saleTransDetailSaleServiceHandset, "86142");
                                            }
                                        }
                                        //Make saleTransSerial
                                        Long saleTransSerialIdHandset = db.getSequence("SALE_TRANS_SERIAL_SEQ", "dbsm");
                                        int rsSaleTransSerialHandset = db.insertSaleTransSerial(saleTransSerialIdHandset, saleTransDetailSaleServiceHandset, stockModelIdHandset, kitBatch.getHandsetSerial());
                                        if (rsSaleTransSerialHandset != 1) {
                                            //Insert fail, insert log, send sms
                                            db.insertLogMakeSaleTransFail(0L, kitBatch.getIsdn(), kitBatch.getSerial(), Long.valueOf(arrTempIdHandset[0]),
                                                    Long.valueOf(arrTempIdHandset[1]), 0L, 0L,
                                                    200351L, saleTransDetailSaleServiceHandset, saleTransSerialIdHandset, "SALE_TRANS_SERIAL");
                                            for (String isdn : arrIsdnReceiveError) {
                                                db.sendSms(isdn, "Make saleTransSerial for sale handset in kit batch fail, more detail in table: kit_make_sale_trans_fail. saleTransSerialId: " + saleTransSerialIdHandset, "86142");
                                            }
                                        }
//                                Step 4: Minus stockTotal
                                        int expStockTotal = db.expStockTotal(Long.valueOf(arrTempIdHandset[1]), stockModelIdHandset, 1L);
                                        if (expStockTotal != 1) {
                                            //Insert fail, insert log, send sms
                                            for (String isdn : arrIsdnReceiveError) {
                                                db.sendSms(isdn, "expStockTotal fail, serialHanset: " + kitBatch.getHandsetSerial()
                                                        + ", isdnCustomer: " + kitBatch.getIsdn(), "86142");
                                            }
                                        }
//                                Step 5: update stock_hand >> status = 0 >> already sale...
                                        int updateSeialExp = db.updateSeialExp(Long.valueOf(arrTempIdHandset[1]), stockModelIdHandset, kitBatch.getHandsetSerial());
                                        if (updateSeialExp != 1) {
                                            //Insert fail, insert log, send sms
                                            for (String isdn : arrIsdnReceiveError) {
                                                db.sendSms(isdn, "updateSeialExp fail, serialHanset: " + kitBatch.getHandsetSerial()
                                                        + ", isdnCustomer: " + kitBatch.getIsdn(), "86142");
                                            }
                                        }

                                    } else {
                                        logger.info("cannot find price for saleRetail by serial: "
                                                + kitBatch.getHandsetSerial() + ", sub:  " + kitBatch.getIsdn());
                                        for (String isdn : arrIsdnReceiveError) {
                                            db.sendSms(isdn, "cannot find price for saleRetail, serialHanset: " + kitBatch.getHandsetSerial()
                                                    + ", isdnCustomer: " + kitBatch.getIsdn(), "86142");
                                        }
                                    }
                                } else {
                                    logger.info("cannot find stockModel by serial: "
                                            + kitBatch.getHandsetSerial() + ", sub:  " + kitBatch.getIsdn());
                                    for (String isdn : arrIsdnReceiveError) {
                                        db.sendSms(isdn, "cannot find stockModel by serial, serialHanset: " + kitBatch.getHandsetSerial()
                                                + ", isdnCustomer: " + kitBatch.getIsdn(), "86142");
                                    }
                                }
                            }
                        }

                        //Insert Mo
                        if (kitBatch.getProductAddOn() != null && !kitBatch.getProductAddOn().trim().isEmpty()) {
                            logger.info("Customer register vas with package: " + kitBatch.getProductAddOn() + " isdn " + kitBatch.getIsdn() + "batchId: " + bn.getKitBatchId());
                            String amountTopup = "";
                            ProductConnectKit productAddon = db.getProductConnectKit(kitBatch.getProductAddOn());
                            if (productAddon != null && productAddon.getPrice() != null) {
                                amountTopup = productAddon.getPrice() + "";
                            }
                            if (!amountTopup.isEmpty() && amountTopup.length() > 0) {
//                        Active subscriber first, after that topup
                                String activeOnOCS = pro.activeOCSACTIVEFIRST("258" + kitBatch.getIsdn());
                                if ("0".equals(activeOnOCS)) {
                                    logger.info("Open flag OCSHW_ACTIVEFIRST successfully for sub: " + kitBatch.getIsdn());
                                } else {
                                    logger.warn("Open flag OCSHW_ACTIVEFIRST unsuccessfully for sub: " + kitBatch.getIsdn() + ", result: " + activeOnOCS);
                                }
//                    Calling webservice
                                SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH.mm.ss");
                                Date now = new Date();
                                String strDate = sdfDate.format(now).replace("-", "").replace(".", "").replace(" ", "");
                                String requestIdTopup = "KIT_" + strDate + kitBatch.getIsdn();
                                String rsTopup = "";
                                if (BCCS_CM.equals(bn.getChannelType())) {
                                    rsTopup = "0";//Already charge on CM
                                } else {
                                    rsTopup = pro.topupWS(requestIdTopup, kitBatch.getIsdn(), amountTopup);
                                }

////                            For testing...
//                            String rsTopup = "0";
                                if ("0".equals(rsTopup)) {
                                    logger.info("Topup successfully for sub: " + kitBatch.getIsdn() + ", vas_code: " + kitBatch.getProductAddOn() + ", amount: " + amountTopup);
//                            Insert MO
                                    ProductConnectKit productCn = db.getProductConnectKit(kitBatch.getProductAddOn());
                                    String param = productCn.getVasParam();
                                    if (param != null && "?".equals(param)) {
                                        param = staffCode + "|" + bn.getAddMonth();
                                        if (BCCS_CM.equals(bn.getChannelType())) {
                                            param = param + "|4";//staff|prepaidMOnth|4(paymenthod)
                                        } else {
                                            param = staffCode;
                                        }
                                    } else {
                                        param = "pt|1|1|4";
                                    }
                                    long moID = db.getSequence("mo_seq", productCn.getVasConnection());
                                    int rsInsertMo = 0;
                                    boolean insertMoid = false;
                                    if (moID > 0) {
                                        insertMoid = true;
                                        Thread.sleep(1000);
                                        rsInsertMo = db.insertMOWithMoId(kitBatch.getIsdn(), kitBatch.getProductAddOn(), productCn.getVasConnection(), param, productCn.getVasActionType(), productCn.getVasChannel(), moID);
                                    } else {
                                        Thread.sleep(1000);
                                        rsInsertMo = db.insertMO(kitBatch.getIsdn(), kitBatch.getProductAddOn(), productCn.getVasConnection(), param, productCn.getVasActionType(), productCn.getVasChannel());
                                    }
                                    if (rsInsertMo == 1 && insertMoid) {
                                        db.updateKitBatchDetailWithVas(bn.getKitBatchId(), kitBatch.getSerial(), kitBatch.getIsdn(), moID);
                                        logger.info("Insert MO successfully for sub: " + kitBatch.getIsdn() + ", vas_code: " + kitBatch.getProductAddOn() + ", amount: " + amountTopup);
                                    } else {
                                        logger.info("Insert MO unsuccessfully for sub: " + kitBatch.getIsdn() + ", vas_code: " + kitBatch.getProductAddOn() + ", amount: " + amountTopup);
                                        for (String isdn : arrIsdnReceiveError) {
                                            db.sendSms(isdn, "Insert MO unsuccessfully for sub: " + kitBatch.getIsdn() + ", vas_code: " + kitBatch.getProductAddOn() + ". Please check and insert by hand now.", "86142");
                                        }
                                    }
                                } else {
                                    logger.info("Topup unsuccessfully for sub: " + kitBatch.getIsdn() + ", vas_code: " + kitBatch.getProductAddOn() + ", amount: " + amountTopup);
                                }
                            } else {
                                logger.info("Cannot get amount topup from VAS_CODE: " + kitBatch.getProductAddOn() + ", isdn: " + kitBatch.getIsdn());
                            }
                        }
                    } else {//Old sub Regiter vas only
                        totalSuccess++;
                        logger.info("Old sub register Addon " + kitBatch.getIsdn());
                        ProductConnectKit productCon = db.getProductConnectKit(kitBatch.getProductAddOn());
                        String addOnFee = "0";
                        if (productCon != null) {
                            addOnFee = productCon.getPrice() + "";
                        }
                        db.updateKitBatchDetail(bn.getKitBatchId(), kitBatch.getSerial(), kitBatch.getIsdn(), "0",
                                "Old sub register Addon.", bn.getNodeName(), addOnFee, kitBatch.getMoneyIsdn());
                        kitBatch.setResultCode("0");
                        //Insert Mo
//						if (kitBatch.getProductAddOn() != null && !kitBatch.getProductAddOn().trim().isEmpty()) {
//							ProductConnectKit productCn = db.getProductConnectKit(kitBatch.getProductAddOn());
//							String param = "pt|1|1|4";
//							int rsInsertMo = db.insertMO(kitBatch.getIsdn(), kitBatch.getProductAddOn(), productCn.getVasConnection(), param, productCn.getVasActionType(), productCn.getVasChannel());
//							if (rsInsertMo == 1) {
//								totalSuccess++;
//								logger.info("Insert MO successfully for sub: " + kitBatch.getIsdn() + ", vas_code: " + kitBatch.getProductAddOn());
//							} else {
//								logger.info("Topup successfully but insert MO unsuccessfully for sub: " + kitBatch.getIsdn() + ", vas_code: " + kitBatch.getProductAddOn());
//								for (String isdn : arrIsdnReceiveError) {
//									db.sendSms(isdn, "Topup successfully but insert MO unsuccessfully for sub: " + kitBatch.getIsdn() + ", vas_code: " + kitBatch.getProductAddOn() + ". Please check and insert by hand now.", "86142");
//								}
//							}
//						}
                    }
                }

//</editor-fold>
                //Save Log actionAudit
                Long actionAuditId = db.getSequence("seq_action_audit", "cm_pre");
                db.insertActionAudit(actionAuditId, "Connect KIT by Batch success for batchId: " + bn.getKitBatchId() + ".",
                        bn.getCustId(), shopCode, staffCode);
                //Check conditional and make free call in group eLite

                //<editor-fold defaultstate="collapsed" desc="Create CUG">
                if (bn.getExtendFromKitBatchId() != null && bn.getExtendFromKitBatchId() > 0) {
                    logger.info("Begin extend for call group free, oldKitBatchId: " + bn.getExtendFromKitBatchId()
                            + "id: " + bn.getKitBatchId() + ", staffCode: " + staffCode);
                    //Step 1: Check group already have CUG_ID or not
                    String cugName = db.getCUGInformation(bn.getExtendFromKitBatchId());
                    String expireTime = db.getExpireTimeCUG(bn.getExtendFromKitBatchId());
                    if (cugName != null && !cugName.isEmpty() && !"N/A".equals(cugName)
                            && expireTime != null && !expireTime.isEmpty()
                            && (new SimpleDateFormat("yyyyMMddHHmmss").parse(expireTime.trim()).compareTo(new Date())) > 0) {
                        //<editor-fold defaultstate="collapsed" desc="Exist Valid CUG">
                        //Step 2: Add to group if exist
                        String cugId = cugName.split("\\_")[1];
                        logger.info("Add sub for call group free, oldKitBatchId: " + bn.getExtendFromKitBatchId()
                                + "id: " + bn.getKitBatchId() + ", staffCode: " + staffCode);
                        List<KitBatch> lstKitEliteCUG = db.getListKitBatchDetailAddCUG(bn.getKitBatchId(), limitMoneyAddonAddCUG);
                        List<KitBatch> addCugList = new ArrayList<KitBatch>();
                        addUniqueKitBatch(lstKitEliteCUG, addCugList);

                        List<KitBatch> listChecOCS = db.getListKitBatchDetailCheckOCS(bn.getKitBatchId(), limitMoneyAddonAddCUG);
                        addUniqueKitBatch(checkValidToAddCUG(listChecOCS, limitMoneyAddonAddCUG, bn.getKitBatchId()), addCugList);

                        for (KitBatch kitBatch : addCugList) {
                            //Step 3: Add member to group on OCS
                            String[] resultAddMember = pro.addMemberGroupCUG(String.valueOf(cugId), kitBatch.getIsdn());
                            if (resultAddMember != null && !"0".equals(resultAddMember[0]) && !"WS_VPN_MEMBER_EXIST".equals(resultAddMember[1])) {
                                logger.warn("Fail to add member " + kitBatch.getIsdn() + " to OCS groupid " + cugId
                                        + " errorcode " + resultAddMember + " batchId " + bn.getKitBatchId());
                                db.updateGroupEliteKitBatchDetail(bn.getKitBatchId(), kitBatch.getIsdn(), resultAddMember[0], String.valueOf(cugId), cugName, "");
                            } else {
                                logger.warn("Add member " + kitBatch.getIsdn() + " to OCS groupid " + cugId
                                        + " successfully errorcode " + resultAddMember + " batchId " + bn.getKitBatchId());
                                //Add priceplan for member on OCS
                                String resultAddPriceMember = pro.addPriceV4(kitBatch.getIsdn(), "11205034", "", expireTime);
                                if (!"0".equals(resultAddPriceMember) && !"102010228".equals(resultAddPriceMember)) {
                                    logger.warn("Fail to add price 11205034 for owner sub on OCS " + cugId
                                            + " error_code " + resultAddPriceMember + " batchId " + bn.getKitBatchId());
                                } else {
                                    logger.warn("Add price 11205034 for owner sub on OCS " + cugId
                                            + "successfully error_code " + resultAddPriceMember + " batchId " + bn.getKitBatchId());
                                    db.sendSms(kitBatch.getIsdn(), eliteKitBatchGroupFreeCallMsg.
                                            replace("%XX%", (addCugList.size()) + "").
                                            replace("%NAME_GROUP%", bn.getGroupName()), "86904");
                                }
                                db.updateGroupEliteKitBatchDetail(bn.getKitBatchId(), kitBatch.getIsdn(), resultAddPriceMember, String.valueOf(cugId), cugName, "");
                                db.updateExpireTimeKitBatchInfo(kitBatch.getKitBatchId(), expireTimeGroup);
                            }
                            bn.setExpireTimeGroup(expireTime);
                            bn.setGroupStatus("1");
                            bn.setDuration(System.currentTimeMillis() - timeSt);

                        }
//</editor-fold>
                    } else {
                        //<editor-fold defaultstate="collapsed" desc="Create new CUG">
                        List<KitBatch> listAddCUG = new ArrayList<KitBatch>();
                        List<KitBatch> allKitBatchDetail = db.getAllKitBatchDetailBeLongCustomer(bn.getKitBatchId());
                        List<KitBatch> allKitBatchExt = db.getAllKitBatchExtendBeLongCustomer(bn.getKitBatchId());
                        addUniqueKitBatch(checkValidToAddCUG(allKitBatchDetail, limitMoneyAddonAddCUG, bn.getKitBatchId()), listAddCUG);
                        addUniqueKitBatch(checkValidToAddCUG(allKitBatchExt, limitMoneyAddonAddCUG, bn.getKitBatchId()), listAddCUG);
                        logger.info("Total subElite valid CUG: " + listAddCUG.size() + ", extendFromKitBatchId: " + bn.getExtendFromKitBatchId());
                        if (listAddCUG.size() >= Integer.parseInt(minSubInBatch)) {
                            logger.info("start make group elite for call free..., id: " + bn.getKitBatchId()
                                    + ", staffCode: " + staffCode);
                            KitBatch ownerKitBatch = listAddCUG.get(0);
                            String ownerOfGroup = ownerKitBatch.getIsdn();
                            Long cugId = db.getSequence("CUG_ID_FTTH_MOBILE_SEQ", "cm_pos");
                            cugName = ownerOfGroup + "_" + cugId;
                            //Step 3: Make group on OCS
                            String[] resultCreateGroup = pro.createGroupCUG(String.valueOf(cugId));
                            if (resultCreateGroup != null && !"0".equals(resultCreateGroup[0]) && !"WS_VPN_CLUSTER_EXIST".equals(resultCreateGroup[1])) {
                                logger.warn("Fail to create group on OCS "
                                        + " error_code " + resultCreateGroup + " batchId " + bn.getKitBatchId());
                            } else {
                                //Step 4: Add owner to group on OCS
                                String[] resultAddOwner = pro.addMemberGroupCUG(String.valueOf(cugId), ownerOfGroup);
                                if (resultAddOwner != null && !"0".equals(resultAddOwner[0]) && !"WS_VPN_MEMBER_EXIST".equals(resultAddOwner[1])) {
                                    logger.warn(ownerOfGroup + " fail to add owner to group on OCS GroupId: " + cugId
                                            + " error_code " + resultAddOwner[0] + " batchId " + bn.getExtendFromKitBatchId());
                                } else {
                                    //Step 5: Add priceplan for owner on OCS
                                    int addDay = 0;
                                    if (isAddMonth) {
                                        addDay = Integer.parseInt(bn.getAddMonth()) * 30;
                                    } else {
                                        addDay = 30;
                                    }
                                    String resultAddPriceOwner = pro.addPrice(ownerOfGroup, "11205034", "", addDay + "");
                                    if (!"0".equals(resultAddPriceOwner) && !"102010228".equals(resultAddPriceOwner)) {
                                        logger.warn(ownerOfGroup + " fail to add price 11205034 for owner sub on OCS " + cugId
                                                + " error_code " + resultAddPriceOwner + " batchId " + bn.getExtendFromKitBatchId());
                                    } else {
                                        //Step 5.1: Set ExpireTime Call free of group
                                        Calendar cal = Calendar.getInstance();
                                        cal.setTime(new Date());
                                        cal.add(Calendar.DATE, addDay + 1); //20180807 add one more day because OCS trunc to begining of day
                                        expireTimeGroup = sdfAddPrice.format(cal.getTime());
                                        bn.setExpireTimeGroup(expireTimeGroup);
                                        bn.setGroupStatus("1");
                                        db.updateGroupEliteKitBatchDetail(ownerKitBatch.getKitBatchId(), ownerKitBatch.getIsdn(), resultAddPriceOwner, String.valueOf(cugId), cugName, ownerOfGroup);
                                        //Step 6: Add member...
                                        //<editor-fold defaultstate="collapsed" desc="Add menber">
                                        for (KitBatch kitBatch : listAddCUG) {
                                            if (!ownerOfGroup.equals(kitBatch.getIsdn())) {
                                                //Step 3: Add member to group on OCS
                                                String[] resultAddMember = pro.addMemberGroupCUG(String.valueOf(cugId), kitBatch.getIsdn());
                                                if (resultAddMember != null && !"0".equals(resultAddMember[0]) && !"WS_VPN_MEMBER_EXIST".equals(resultAddMember[1])) {
                                                    logger.warn("Fail to add member " + kitBatch.getIsdn() + " to OCS groupid " + cugId
                                                            + " errorcode " + resultAddMember[0] + " batchId " + bn.getKitBatchId());
                                                    db.updateGroupEliteKitBatchDetail(kitBatch.getKitBatchId(), kitBatch.getIsdn(), resultAddMember[0], String.valueOf(cugId), cugName, "");
                                                } else {
                                                    logger.warn("Add member " + kitBatch.getIsdn() + " to OCS groupid " + cugId
                                                            + " successfully errorcode " + resultAddMember + " batchId " + bn.getKitBatchId());
                                                    addDay = 0;
                                                    if (isAddMonth) {
                                                        addDay = Integer.parseInt(bn.getAddMonth()) * 30;
                                                    } else {
                                                        addDay = 30;
                                                    }
                                                    //Add priceplan for member on OCS
                                                    String resultAddPriceMember = pro.addPrice(kitBatch.getIsdn(), "11205034", "", addDay + "");
                                                    if (!"0".equals(resultAddPriceMember) && !"102010228".equals(resultAddPriceMember)) {
                                                        logger.warn(ownerOfGroup + " fail to add price 11205034 for owner sub on OCS " + cugId
                                                                + " error_code " + resultAddPriceMember + " batchId " + bn.getKitBatchId());
                                                    } else {
                                                        logger.warn(ownerOfGroup + " add price 11205034 for owner sub on OCS " + cugId
                                                                + "successfully error_code " + resultAddPriceMember + " batchId " + bn.getKitBatchId());
                                                        db.sendSms(kitBatch.getIsdn(), eliteKitBatchGroupFreeCallMsg.
                                                                replace("%XX%", (listAddCUG.size()) + "").
                                                                replace("%NAME_GROUP%", bn.getGroupName()), "86904");
                                                    }
                                                    db.updateGroupEliteKitBatchDetail(kitBatch.getKitBatchId(), kitBatch.getIsdn(), resultAddPriceMember, String.valueOf(cugId), cugName, "");
                                                    db.updateExpireTimeKitBatchInfo(kitBatch.getKitBatchId(), expireTimeGroup);
                                                    //db.updateExpireTimeKitBatchInfo(bn.getExtendFromKitBatchId(), expireTimeGroup);
                                                }
                                            }
                                            db.updateExpireTimeKitBatchInfo(kitBatch.getKitBatchId(), expireTimeGroup);
                                        }
//</editor-fold>
                                    }
                                }
                            }

                        } else {
                            logger.info("Total sub elite CUG: " + listAddCUG.size() + "  not enough to create CUG "
                                    + "id: " + bn.getKitBatchId() + ", staffCode: " + staffCode);
                            bn.setGroupStatus("0");
                            bn.setDuration(System.currentTimeMillis() - timeSt);
                        }
//</editor-fold>

                    }
                } else {//Create new CUG
                    List<KitBatch> listAddCUG = new ArrayList<KitBatch>();
                    List<KitBatch> allKitBatchDetail = db.getAllKitBatchDetailBeLongCustomer(bn.getKitBatchId());
                    addUniqueKitBatch(checkValidToAddCUG(allKitBatchDetail, limitMoneyAddonAddCUG, bn.getKitBatchId()), listAddCUG);

                    if (listAddCUG.size() >= Integer.parseInt(minSubInBatch)) {
                        logger.info("start make group elite for call free..., id: " + bn.getKitBatchId()
                                + ", staffCode: " + staffCode);
                        String ownerOfGroup = listAddCUG.get(0).getIsdn();
                        Long cugId = db.getSequence("CUG_ID_FTTH_MOBILE_SEQ", "cm_pos");
                        String cugName = ownerOfGroup + "_" + cugId;
                        //Step 3: Make group on OCS
                        String[] resultCreateGroup = pro.createGroupCUG(String.valueOf(cugId));
                        if (resultCreateGroup != null && !"0".equals(resultCreateGroup[0]) && !"WS_VPN_CLUSTER_EXIST".equals(resultCreateGroup[1])) {
                            logger.warn("Fail to create group on OCS "
                                    + " error_code " + resultCreateGroup[0] + " batchId " + bn.getKitBatchId());
                        } else {
                            //Step 4: Add owner to group on OCS
                            String[] resultAddOwner = pro.addMemberGroupCUG(String.valueOf(cugId), ownerOfGroup);
                            if (resultAddOwner != null && !"0".equals(resultAddOwner[0]) && !"WS_VPN_MEMBER_EXIST".equals(resultAddOwner[1])) {
                                logger.warn(ownerOfGroup + " fail to add owner to group on OCS GroupId: " + cugId
                                        + " error_code " + resultAddOwner[0] + " batchId " + bn.getKitBatchId());
                            } else {
                                //Step 5: Add priceplan for owner on OCS
                                int addDay = 0;
                                if (isAddMonth) {
                                    addDay = Integer.parseInt(bn.getAddMonth()) * 30;
                                } else {
                                    addDay = 30;
                                }
                                String resultAddPriceOwner = pro.addPrice(ownerOfGroup, "11205034", "", addDay + "");
                                if (!"0".equals(resultAddPriceOwner) && !"102010228".equals(resultAddPriceOwner)) {
                                    logger.warn(ownerOfGroup + " fail to add price 11205034 for owner sub on OCS " + cugId
                                            + " error_code " + resultAddPriceOwner + " batchId " + bn.getKitBatchId());
                                } else {
                                    //Step 5.1: Set ExpireTime Call free of group
                                    Calendar cal = Calendar.getInstance();
                                    cal.setTime(new Date());
                                    cal.add(Calendar.DATE, addDay + 1); //20180807 add one more day because OCS trunc to begining of day
                                    expireTimeGroup = sdfAddPrice.format(cal.getTime());
                                    bn.setExpireTimeGroup(expireTimeGroup);
                                    db.updateGroupEliteKitBatchDetail(bn.getKitBatchId(), ownerOfGroup, resultAddPriceOwner, String.valueOf(cugId),
                                            cugName, ownerOfGroup);
                                    //db.updateExpireTimeKitBatchInfo(bn.getKitBatchId(), expireTimeGroup);

                                    //Step 6: Add member...
                                    for (KitBatch kitBatch : listAddCUG) {
                                        if (!ownerOfGroup.equals(kitBatch.getIsdn())) {
                                            //Step 3: Add member to group on OCS
                                            String[] resultAddMember = pro.addMemberGroupCUG(String.valueOf(cugId), kitBatch.getIsdn());
                                            if (resultAddMember != null && !"0".equals(resultAddMember[0]) && !"WS_VPN_MEMBER_EXIST".equals(resultAddMember[1])) {
                                                logger.warn("Fail to add member " + kitBatch.getIsdn() + " to OCS groupid " + cugId
                                                        + " errorcode " + resultAddMember + " batchId " + bn.getKitBatchId());
                                            } else {
                                                logger.warn("Add member " + kitBatch.getIsdn() + " to OCS groupid " + cugId
                                                        + " successfully errorcode " + resultAddMember + " batchId " + bn.getKitBatchId());
                                                addDay = 0;
                                                if (isAddMonth) {
                                                    addDay = Integer.parseInt(bn.getAddMonth()) * 30;
                                                } else {
                                                    addDay = 30;
                                                }
                                                //Add priceplan for member on OCS
                                                String resultAddPriceMember = pro.addPrice(kitBatch.getIsdn(), "11205034", "", addDay + "");
                                                if (!"0".equals(resultAddPriceMember) && !"102010228".equals(resultAddPriceMember)) {
                                                    logger.warn(ownerOfGroup + " fail to add price 11205034 for owner sub on OCS " + cugId
                                                            + " error_code " + resultAddPriceMember + " batchId " + bn.getKitBatchId());
                                                } else {
                                                    db.updateGroupEliteKitBatchDetail(bn.getKitBatchId(), kitBatch.getIsdn(), resultAddPriceMember, String.valueOf(cugId), cugName, "");
                                                    db.updateExpireTimeKitBatchInfo(kitBatch.getKitBatchId(), expireTimeGroup);
                                                    logger.warn(ownerOfGroup + " add price 11205034 for owner sub on OCS " + cugId
                                                            + "successfully error_code " + resultAddPriceMember + " batchId " + bn.getKitBatchId());
                                                    db.sendSms(kitBatch.getIsdn(), eliteKitBatchGroupFreeCallMsg.
                                                            replace("%XX%", listAddCUG.size() + "").
                                                            replace("%NAME_GROUP%", bn.getGroupName()), "86904");
                                                }
                                            }
                                        } else {
                                            //db.updateExpireTimeKitBatchInfo(kitBatch.getKitBatchId(), expireTimeGroup);
                                            db.sendSms(kitBatch.getIsdn(), eliteKitBatchGroupFreeCallMsg.
                                                    replace("%XX%", listAddCUG.size() + "").
                                                    replace("%NAME_GROUP%", bn.getGroupName()), "86904");
                                        }
                                    }
                                }
                            }
                        }
                        bn.setGroupStatus("1");
                        bn.setDuration(System.currentTimeMillis() - timeSt);

                    } else {
                        logger.warn("Don't need to create group, not enough ELITE sub having " + listAddCUG.size() + " min " + minSubInBatch
                                + " batchId " + bn.getKitBatchId());
                    }
                }
                //</editor-fold>

                totalFail = lstKitBatchDetail.size() - totalSuccess;
                bn.setTotalSuccess(totalSuccess);
                bn.setTotalFail(totalFail);
//                if (totalCommissionForPrepaidMonth > 0) {
//                    totalCommission = totalCommission + totalCommissionForPrepaidMonth;
//                    logger.info("Kit Batch have value addMonth: " + bn.getAddMonth() + ", bonus more for prepaid month, " + totalCommissionForPrepaidMonth
//                            + "id: " + bn.getKitBatchId() + ", staffCode: " + staffCode);
//                }
                if (totalSuccess > 0) {
//                    LinhNBV 20190410: insert to kit_elite_prepaid
                    //if (isAddMonth) {
                    int rsPrepaid = db.insertKitElitePrepaid(prepaidMonthInput, bn.getKitBatchId(), staffCode);
                    if (rsPrepaid == 1) {
                        logger.info("insert kit_elite_prepaid successfully, addMonth: " + bn.getAddMonth() + ", id: "
                                + bn.getKitBatchId() + ", staffCode: " + staffCode);
                    } else {
                        logger.info("insert kit_elite_prepaid unsuccessfully, addMonth: " + bn.getAddMonth() + ", id: "
                                + bn.getKitBatchId() + ", staffCode: " + staffCode);
                    }
                    //}
                    //20200226 Update enterprise isdn
                    logger.info("***********Enterprise isdn wallet is " + isdnEnterprise + " ,now try to update in DB");
                    if (isdnEnterprise != null && !isdnEnterprise.isEmpty()) {
                        int rsUpdateEnterprisIsdn = db.updateEnterpriseIsdnForBatch(bn.getKitBatchId(), isdnEnterprise);
                        logger.info("UpdateEnterprisIsdn " + bn.getKitBatchId() + " new isdn enterprise " + isdnEnterprise
                                + " result " + rsUpdateEnterprisIsdn);
                    }
                    logger.info("End connect kit by batch, totalSuccess: " + totalSuccess + ", totalFail: " + totalFail
                            + "now pay commission, totalCommission: " + totalCommission
                            + ", id: " + bn.getKitBatchId() + ", staffCode: " + staffCode);
                    Calendar cal = Calendar.getInstance();
                    cal.setTime(new Date());
                    cal.add(Calendar.DATE, 7);
                    String msg = "Conectaste KIT em massa. Total bem sucedidos: " + totalSuccess + ", total falhados: "
                            + totalFail + ", comissao: " + totalCommission + ", ID do grupo: " + bn.getKitBatchId()
                            + ". Para receber toda comissao incetiva a activacao dos numeros antes de " + sdf2.format(cal.getTime()) + ". Obrigado!";
                    db.sendSms(tel, msg, "86904");
                    bn.setResultCode("0");
                    bn.setDescription("Finish connect Kit Batch, check detail table for each sub.");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                } else {
                    logger.info("End connect kit by batch, totalSuccess: " + totalSuccess + ", totalFail: " + totalFail
                            + " , totalCommission: " + totalCommission
                            + " , id: " + bn.getKitBatchId() + " , staffCode: " + staffCode);
                    bn.setResultCode("05");
                    bn.setDescription("Total success is zero. Check kit_batch_detail table.");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    for (KitBatch kitBatch : lstKitBatchDetail) {
                        int rollbackIsdn = db.updateStockIsdn(1L, "SYS_ROLLBACK", kitBatch.getIsdn());
                        int rollbackSim = db.updateStockSim(0L, 1L, kitBatch.getSerial(), kitBatch.getIsdn());
                        logger.warn("Charge eMola fail, Result rollback stockIsdnMobile: " + rollbackIsdn + " stockSim: "
                                + rollbackSim + " for serial: " + kitBatch.getSerial()
                                + " isdn " + kitBatch.getIsdn());
                    }
                    if ("0".equals(bn.getPayType())) {
                        logger.info("Paymethod is BankTransfer, now rollback trans: bankCode: " + bn.getBankTransCode()
                                + ", bankName: " + bn.getBankName() + "id " + bn.getKitBatchId() + ", staff: " + staffCode);
                        String rollBackTrans = BankTransferUtils.callWSSOAProllbackTrans(bn.getBankTransCode(), bn.getBankName(),
                                bn.getBankTransAmount(), staffCode, staffCode);
                        if (!"00".equalsIgnoreCase(rollBackTrans)) {
                            logger.info("Rollback of BankDocument fail error_code: " + rollBackTrans + ", id: " + bn.getKitBatchId() + ", staffCode: " + staffCode);
                            bn.setDescription(bn.getDescription() + "--Rollback BankDocument fail");
                        } else {
                            logger.info("Rollback of BankDocument BankDocument successfully, error_code: " + rollBackTrans
                                    + "id: " + bn.getKitBatchId() + ", staffCode: " + staffCode);
                        }
                    }
                    String msg = "Fail to connect batch id " + bn.getKitBatchId();
                    db.sendSms(tel, msg, "86904");
                    continue;
                }
                if (totalCommission > 0 && totalSuccess > 0) {
                    for (KitBatch kitBatch : lstKitBatchDetail) {
                        if (kitBatch.getResultCode() != null && !kitBatch.getResultCode().isEmpty()
                                && "0".equals(kitBatch.getResultCode()) && kitBatch.isIsConnectNew()) {
                            Comission comission = db.getComissionStaff(staffCode, kitBatch.getProductCode(), kitBatch.getIsdn());
                            if (comission != null) {
                                //                20190104 change not pay bonus now, insert to pay_bonus_connect_kit to wait sub active
                                db.insertBonusConnectKit(staffCode, kitBatch.getProductCode(), kitBatch.getIsdn(), kitBatch.getActionAuditId(), 0,
                                        "615", kitBatch.getSerial(), bn.getCreateTime(), Integer.parseInt(bn.getAddMonth()),
                                        0.0, kitBatch.getBonusForPrepaid(), kitBatch.getSubProfileId());
                            }
                        } else {
                            logger.info("No need insert bonus_connect_kit because isdn: " + kitBatch.getIsdn() + ", connect fail, "
                                    + "id: " + bn.getKitBatchId() + ", staffCode: " + staffCode);
                        }

                    }
                } else {
                    logger.info("Value of commission is zero,no need to pay"
                            + ", id: " + bn.getKitBatchId() + ", staffCode: " + staffCode);
                }
            } else {
                logger.warn("After validate respone code is fail "
                        + " id " + bn.getKitBatchId() + " staffCode: " + bn.getCreateUser()
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
                append("|\tKIT_BATCH_ID|").
                append("|\tCREATE_USER|").
                append("|\tCREATE_TIME|").
                append("|\tUNIT_CODE\t|").
                append("|\tCUST_ID\t|").
                append("|\tPAY_TYPE\t|").
                append("|\tBANK_NAME\t|").
                append("|\tBANK_TRAN_CODE\t|").
                append("|\tBANK_TRAN_AMOUNT\t|").
                append("|\tEMOLA_ACCOUNT\t|").
                append("|\tEMOLA_VOUCHER_CODE\t|").
                append("|\tADD_MONTH\t|");
        for (Record record : listRecord) {
            KitBatchInfo bn = (KitBatchInfo) record;
            br.append("\r\n").
                    append("|\t").
                    append(bn.getKitBatchId()).
                    append("||\t").
                    append(bn.getCreateUser()).
                    append("||\t").
                    append((bn.getCreateTime() != null ? sdf.format(bn.getCreateTime()) : null)).
                    append("||\t").
                    append(bn.getUnitCode()).
                    append("||\t").
                    append(bn.getCustId()).
                    append("||\t").
                    append((bn.getPayType())).
                    append("||\t").
                    append(bn.getBankName() != null ? bn.getBankName() : "").
                    append("||\t").
                    append(bn.getBankTransCode() != null ? bn.getBankTransCode() : "").
                    append("||\t").
                    append(bn.getBankTransAmount() != null ? bn.getBankTransAmount() : "").
                    append("||\t").
                    append(bn.getEmolaAccount() != null ? bn.getEmolaAccount() : "").
                    append("||\t").
                    append(bn.getEmolaVoucherCode() != null ? bn.getEmolaVoucherCode() : "").
                    append("||\t").
                    append(bn.getAddMonth() != null ? bn.getAddMonth() : "");
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

    private List<KitBatch> checkValidToAddCUG(List<KitBatch> listKitBatch, double limitFee, long currentKitbatchId) {
        List<KitBatch> addCugList = new ArrayList<KitBatch>();
        try {
            for (KitBatch kitCug : listKitBatch) {
                boolean checkOCS = false;
                if (currentKitbatchId == kitCug.getKitBatchId()) {
                    if (kitCug.isIsConnectNew()) {
                        ProductConnectKit product = db.getProductConnectKit(kitCug.getProductCode().trim());
                        if (product != null && product.getVipProduct() == 1 && !"DATA_SIM".equals(product.getProductCode())) {
                            if (!checkExsiteOnList(addCugList, kitCug)) {
                                addCugList.add(kitCug);
                            }
                        } else if (kitCug.getProductAddOn() != null && !kitCug.getProductAddOn().trim().isEmpty()) {
                            ProductConnectKit productAddon = db.getProductConnectKit(kitCug.getProductAddOn().trim());
                            if (productAddon != null && productAddon.getPrice() >= limitFee) {
                                if (!checkExsiteOnList(addCugList, kitCug)) {
                                    addCugList.add(kitCug);
                                }
                            }
                        }
                    } else {
                        if (kitCug.getProductAddOn() != null && !kitCug.getProductAddOn().trim().isEmpty()) {
                            ProductConnectKit productAddon = db.getProductConnectKit(kitCug.getProductAddOn().trim());
                            if (productAddon != null && productAddon.getPrice() >= limitFee) {
                                if (!checkExsiteOnList(addCugList, kitCug)) {
                                    addCugList.add(kitCug);
                                }
                            }
                        } else {
                            checkOCS = true;
                        }
                    }
                } else {
                    checkOCS = true;
                }
                if (checkOCS) {
                    boolean isValid = false;
                    String originalData = pro.queryIntegration(kitCug.getIsdn());
                    List<Offer> listOffer = null;
                    if (originalData != null && !originalData.isEmpty()) {
                        listOffer = pro.parseListOffer(originalData);
                    }
                    List<String> listPP = db.getValidPricePlan(limitFee);
                    if (listPP != null && !listPP.isEmpty()) {
                        if (listOffer != null && !listOffer.isEmpty()) {
                            for (Offer of : listOffer) {
                                for (String pp : listPP) {
                                    if (pp.equals(of.getId())) {
                                        isValid = true;
                                        break;
                                    }
                                }
                                if (isValid) {
                                    break;
                                }
                            }
                        }
                    }
                    if (!isValid) {
                        if (db.checkValidOla(kitCug.getIsdn())) {
                            isValid = true;
                        }
                    }
                    if (isValid) {
                        if (!checkExsiteOnList(addCugList, kitCug)) {
                            addCugList.add(kitCug);
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Error when check list kit batch detail " + e.getMessage());
            return addCugList;
        }

        return addCugList;
    }

    private List<KitBatch> addUniqueKitBatch(List<KitBatch> fromList, List<KitBatch> toList) {
        for (KitBatch kit : fromList) {
            if (!checkExsiteOnList(toList, kit)) {
                toList.add(kit);
            }
        }
        return toList;
    }

    private boolean checkExsiteOnList(List<KitBatch> list, KitBatch kit) {
        for (KitBatch kitInBatch : list) {
            if (kitInBatch.getKitBatchId().equals(kit.getKitBatchId()) && kitInBatch.getIsdn().equals(kit.getIsdn())) {
                return true;
            }
        }
        return false;
    }

    private float calculateBonus(KitBatch kitBatch, String addMonth) {
        logger.info("start calculate discount with rate ");
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
            if (Integer.parseInt(addMonth) >= configMonth) {
                strPercentBonus = arrTmp[1];
                logger.info("Batch have prepaidMonth value: " + addMonth + ", percentBonus: "
                        + strPercentBonus + ", isdn in batch:  " + kitBatch.getIsdn());
                break;
            }
        }
        float percentBonus;
        try {
            percentBonus = Float.valueOf(strPercentBonus);
        } catch (NumberFormatException ex) {
            ex.printStackTrace();
            logger.info("Have exeption when parse percentBonus so auto set default percentBonus is zero, ex:"
                    + ex.getMessage() + ", isdn:  " + kitBatch.getIsdn());
            percentBonus = 0.0f;
        }
        return percentBonus;
    }
}
