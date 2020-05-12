/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbMakeSaleTranVipSub;
import com.viettel.paybonus.obj.TransactionInfo;
import com.viettel.paybonus.obj.VipSubInfo;
import java.util.List;
import org.apache.log4j.Logger;
import com.viettel.threadfw.process.ProcessRecordAbstract;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.ResourceBundle;

/**
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class VipSubMakeSaleTran extends ProcessRecordAbstract {

    DbMakeSaleTranVipSub db;
    String sMapReason;
    ArrayList<HashMap> lstMapReason;
    String sMapShop;
    ArrayList<HashMap> lstMapShop;
    String sMapStaff;
    ArrayList<HashMap> lstMapStaff;
    String sMapDiscount;
    ArrayList<HashMap> lstMapDiscount;
    String sMapSaleCode;
    ArrayList<HashMap> lstMapSaleCode;
    String isdn;
    String[] listSub;

    public VipSubMakeSaleTran() {
        super();
        logger = Logger.getLogger(VipSubMakeSaleTran.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        sMapReason = ResourceBundle.getBundle("configPayBonus").getString("mapReason");
        sMapShop = ResourceBundle.getBundle("configPayBonus").getString("mapShop");
        sMapStaff = ResourceBundle.getBundle("configPayBonus").getString("mapStaff");
        sMapDiscount = ResourceBundle.getBundle("configPayBonus").getString("mapDiscount");
        sMapSaleCode = ResourceBundle.getBundle("configPayBonus").getString("mapSaleCode");
        String[] tmpMapReson = sMapReason.split("\\|");
        String[] tmpMapShop = sMapShop.split("\\|");
        String[] tmpMapStaff = sMapStaff.split("\\|");
        String[] tmpMapDiscount = sMapDiscount.split("\\|");
        String[] tmpMapSaleCode = sMapSaleCode.split("\\|");
        lstMapReason = new ArrayList<HashMap>();
        lstMapShop = new ArrayList<HashMap>();
        lstMapStaff = new ArrayList<HashMap>();
        lstMapDiscount = new ArrayList<HashMap>();
        lstMapSaleCode = new ArrayList<HashMap>();
        for (String reason : tmpMapReson) {
            String[] tmp = reason.split("\\:");
            HashMap map = new HashMap();
            map.put(tmp[0].trim().toUpperCase(), tmp[1].trim());
            lstMapReason.add(map);
        }
        for (String shop : tmpMapShop) {
            String[] tmp = shop.split("\\:");
            HashMap map = new HashMap();
            map.put(tmp[0].trim().toUpperCase(), tmp[1].trim());
            lstMapShop.add(map);
        }
        for (String staff : tmpMapStaff) {
            String[] tmp = staff.split("\\:");
            HashMap map = new HashMap();
            map.put(tmp[0].trim().toUpperCase(), tmp[1].trim());
            lstMapStaff.add(map);
        }
        for (String discount : tmpMapDiscount) {
            String[] tmp = discount.split("\\:");
            HashMap map = new HashMap();
            map.put(tmp[0].trim().toUpperCase(), tmp[1].trim());
            lstMapDiscount.add(map);
        }
        for (String saleCode : tmpMapSaleCode) {
            String[] tmp = saleCode.split("\\:");
            HashMap map = new HashMap();
            map.put(tmp[0].trim().toUpperCase(), tmp[1].trim());
            lstMapSaleCode.add(map);
        }
        db = new DbMakeSaleTranVipSub();
        isdn = ResourceBundle.getBundle("configPayBonus").getString("vip_sub_isdn");
        listSub = isdn.split("\\|");
    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        return listRecord;
    }

    @Override
    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
        List<Record> listResult = new ArrayList<Record>();
        int resultSaleTran;
        int resultSaleTranDetail;
        int resultUpdateTranLog;
        int resultSaleTranOrder;
        long shopId;
        long staffId;
        String saleCode;
        double discountRate;
        double amountDiscount;
        double amountDiscountBeforeTax;
        double tax;
        double amountPay;
        double amountBeforeTax;
        long reasonId;
        Long[] staffInfo = new Long[2];
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        VipSubInfo vipInfo;
        for (Record record : listRecord) {
            resultSaleTran = 0;
            resultSaleTranDetail = 0;
            resultUpdateTranLog = 0;
            resultSaleTranOrder = 0;
            shopId = 0;
            staffId = 0;
            saleCode = "";
            discountRate = 0;
            amountDiscount = 0;
            amountDiscountBeforeTax = 0;
            amountPay = 0;
            amountBeforeTax = 0;
            tax = 0;
            reasonId = 0;
            vipInfo = new VipSubInfo();
            TransactionInfo bn = (TransactionInfo) record;
            listResult.add(bn);
            if ("0".equals(bn.getResultCode())) {
//                Get value mapped with client
				/*
                 for (int i = 0; i < lstMapShop.size(); i++) {
                 if (lstMapShop.get(i).containsKey(bn.getClient().trim().toUpperCase())) {
                 shopId = Long.valueOf(lstMapShop.get(i).get(bn.getClient().trim().toUpperCase()).toString());
                 break;
                 }
                 }
                 for (int i = 0; i < lstMapStaff.size(); i++) {
                 if (lstMapStaff.get(i).containsKey(bn.getClient().trim().toUpperCase())) {
                 staffId = Long.valueOf(lstMapStaff.get(i).get(bn.getClient().trim().toUpperCase()).toString());
                 break;
                 }
                 }
                 */
                if (bn.getCreateUser() == null || bn.getCreateUser().length() == 0) {
                    logger.warn("Cannot get create user " + bn.getVipSubInfoId() + " create user " + bn.getCreateUser());
                    bn.setResultCode("9");
                    bn.setDescription("Cannot get create user " + bn.getVipSubInfoId());
                    continue;
                }
                staffInfo = db.getStaffInfo(bn.getCreateUser());
                if (staffInfo == null || staffInfo[0] == null || staffInfo[1] == null) {
                    logger.warn("Cannot get staff info from create user " + bn.getVipSubInfoId() + " create user " + bn.getCreateUser());
                    bn.setResultCode("10");
                    bn.setDescription("Cannot get staff info from create user " + bn.getVipSubInfoId());
                    continue;
                }
                staffId = staffInfo[0];
                shopId = staffInfo[1];

                for (int i = 0; i < lstMapReason.size(); i++) {
                    if (lstMapReason.get(i).containsKey(bn.getClient().trim().toUpperCase())) {
                        reasonId = Long.valueOf(lstMapReason.get(i).get(bn.getClient().trim().toUpperCase()).toString());
                        break;
                    }
                }
                for (int i = 0; i < lstMapSaleCode.size(); i++) {
                    if (lstMapSaleCode.get(i).containsKey(bn.getClient().trim().toUpperCase())) {
                        saleCode = lstMapSaleCode.get(i).get(bn.getClient().trim().toUpperCase()).toString();
                        break;
                    }
                }
                for (int i = 0; i < lstMapDiscount.size(); i++) {
                    if (lstMapDiscount.get(i).containsKey(bn.getClient().trim().toUpperCase())) {
                        discountRate = Double.valueOf(lstMapDiscount.get(i).get(bn.getClient().trim().toUpperCase()).toString());
                        break;
                    }
                }
                if (shopId <= 0 || staffId <= 0 || discountRate < 0 || reasonId <= 0 || saleCode == null || saleCode.length() <= 0) {
                    logger.warn("Do not have config for client " + bn.getClient()
                            + " shopId " + shopId + " staffId " + staffId + " discountRate " + discountRate
                            + " reasonId " + reasonId + " saleCode " + saleCode);
                    bn.setResultCode("1");
                    bn.setDescription("Do not have config for client " + bn.getClient());
                    continue;
                }
                saleCode = saleCode + bn.getID() + sdf.format(new Date());
//                get saleTransId
                long saleTransId = db.getSaleTransId(bn.getClient());
                if (saleTransId <= 0) {
                    logger.warn("Fail to get saleTransId for VipSubInfoId " + bn.getVipSubInfoId());
                    for (String sub : listSub) {
                        db.sendSms(sub, "Fail to get saleTransId for VipSubInfoId "
                                + bn.getVipSubInfoId() + " when making SaleTrans", "86904");
                    }
                    bn.setResultCode("2");
                    bn.setDescription("Fail to get saleTransId for VipSubInfoId " + bn.getVipSubInfoId());
                    continue;
                }
//                Calculate tax, amountTax, discount
                if (bn.getMoney() <= 0) {
                    logger.warn("Money is <=0 so can not make sale trans for VipSubInfoId " + bn.getVipSubInfoId());
                    bn.setResultCode("3");
                    bn.setDescription("Money is <=0 so can not make sale trans for VipSubInfoId " + bn.getVipSubInfoId());
                    continue;
                }
////                amountDiscount = bn.getMoney() * discountRate; //Discount after tax
//                amountBeforeTax = bn.getMoney() / (1 + 0.17) - amountDiscount;
//                tax = 0.17 * amountBeforeTax; //fix tax = 17%          
//                amountPay = amountBeforeTax + tax;
//                amountDiscountBeforeTax = amountBeforeTax * discountRate;
                amountBeforeTax = bn.getMoney() / 1.17;
                amountDiscount = amountBeforeTax * discountRate;
                double amountAfterDiscountNotTax = amountBeforeTax - amountDiscount;
                tax = amountAfterDiscountNotTax * 0.17;
                amountPay = amountAfterDiscountNotTax + tax;
                logger.warn("Start make sale_trans for VipSubInfoId " + bn.getVipSubInfoId() + " amountBeforeTax " + bn.getMoney()
                        + " tax " + tax + " amountBeforeTax " + amountBeforeTax
                        + " amountDiscount " + amountDiscount
                        + " amountPay " + amountPay
                        + " shopId " + shopId + " staffId " + staffId + " discountRate " + discountRate
                        + " reasonId " + reasonId + " saleCode " + saleCode);
                vipInfo = db.getVipSubInfo(bn.getVipSubInfoId());
//                insert sale_trans
                //bacnx 20190429
                if ("1".equals(bn.getPaymentMethod())) {//create trans without debit
                    String[] docInfo = db.getBankDocument(bn.getDocId());
                    if (docInfo == null || docInfo[0] == null || docInfo[1] == null || docInfo[2] == null) {
                        logger.warn("The payment method is bank transfer by cannot find band document " + bn.getVipSubInfoId()
                                + "getDocId " + bn.getDocId() + " bank  " + docInfo[0] + " bank doc  " + docInfo[1] + " bank amount " + docInfo[2]);
                        bn.setResultCode("11");
                        bn.setDescription("Cannot find band document info for VipSubInfoId " + bn.getVipSubInfoId());
                        continue;
                    }

                    resultSaleTran = db.insertSaleTransNoDebit(saleTransId, shopId, staffId, amountDiscount, amountPay, amountBeforeTax,
                            tax, reasonId, saleCode, bn.getClient(), vipInfo.getCustName(), vipInfo.getCustTel(), vipInfo.getCustAddress());
                    //insert sale_trans_order
                    resultSaleTranOrder = db.insertSaleTransOrder(docInfo[0], docInfo[2], docInfo[1], saleTransId);
                } else {//create tran for user created vipsub request
                    resultSaleTran = db.insertSaleTrans(saleTransId, shopId, staffId, amountDiscount, amountPay, amountBeforeTax,
                            tax, reasonId, saleCode, bn.getClient(), vipInfo.getCustName(), vipInfo.getCustTel(), vipInfo.getCustAddress());
                }
                if (resultSaleTran <= 0) {
                    logger.warn("Fail to insert SaleTrans VipSubInfoId " + bn.getVipSubInfoId()
                            + " money " + bn.getMoney() + " resultSaleTran " + resultSaleTran);
                    for (String sub : listSub) {
                        db.sendSms(sub, "Fail to insert SaleTrans for VipSubInfoId "
                                + bn.getVipSubInfoId() + " when making SaleTrans", "86904");
                    }
                    bn.setResultCode("4");
                    bn.setDescription("Fail to insert SaleTrans");
                    continue;
                }
                if (resultSaleTranOrder <= 0 && "1".equals(bn.getPaymentMethod())) {
                    logger.warn("Fail to insert SaleTransOrder VipSubInfoId " + bn.getVipSubInfoId()
                            + " money " + bn.getMoney() + " resultSaleTran " + resultSaleTran);
                    for (String sub : listSub) {
                        db.sendSms(sub, "Fail to insert SaleTransOrder for VipSubInfoId "
                                + bn.getVipSubInfoId() + " when making SaleTrans", "86904");
                    }
                    bn.setResultCode("4");
                    bn.setDescription("Fail to insert SaleTrans");
                    continue;
                }
//                insert sale_trans_detail
//                resultSaleTranDetail = db.insertSaleTransDetail(saleTransId, amountPay, amountDiscountBeforeTax,
//                        bn.getMoney(), tax, amountAfterTax, bn.getClient());
                resultSaleTranDetail = db.insertSaleTransDetail(saleTransId, bn.getMoney(), amountDiscount, bn.getClient(), bn.getTableName());
                if (resultSaleTranDetail <= 0) {
                    logger.warn("Fail to insert SaleTranDetail VipSubInfoId " + bn.getVipSubInfoId()
                            + " money " + bn.getMoney() + " resultSaleTranDetail " + resultSaleTranDetail);
                    for (String sub : listSub) {
                        db.sendSms(sub, "Fail to insert SaleTranDetail for VipSubInfoId "
                                + bn.getVipSubInfoId() + " when making SaleTrans", "86904");
                    }
                    bn.setResultCode("4");
                    bn.setDescription("Fail to insert SaleTranDetail");
                    continue;
                }
//                update TranLog
                resultUpdateTranLog = db.updateTransLog(bn.getClient(), bn.getTableName(), saleTransId, saleCode, bn.getVipSubInfoId());
                logger.info("Quantity row need to make sale trans " + bn.getTranInitCount()
                        + " quantity updated " + resultUpdateTranLog + " VipSubInfoId " + bn.getVipSubInfoId());
                bn.setTranUpdateCount(resultUpdateTranLog);
//                Finish
                bn.setResultCode("0");
                bn.setDescription("Finish make sale trans for client " + bn.getClient());
                continue;
            } else {
                logger.warn("After validate respone code is fail id " + bn.getId()
                        + " so continue with other transaction");
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
                append("|\tCLIENT|").
                append("|\tTRAN_INIT_COUNT\t|").
                append("|\tMONEY\t|").
                append("|\tVIP_SUB_INFO_ID\t|").
                append("|\tTRAN_DATE\t|");
        for (Record record : listRecord) {
            TransactionInfo bn = (TransactionInfo) record;
            br.append("\r\n").
                    append("|\t").
                    append(bn.getClient()).
                    append("||\t").
                    append(bn.getTranInitCount()).
                    append("||\t").
                    append(bn.getMoney()).
                    append("||\t").
                    append(bn.getVipSubInfoId()).
                    append("||\t").
                    append((bn.getTransTime() != null ? sdf.format(bn.getTransTime()) : null));
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

    public static void main(String[] args) {
        double money = 100;
        double amountBeforeTax = money / 1.17;
        double amountDiscount = amountBeforeTax * 0.16;
        double amountAfterDiscountNotTax = amountBeforeTax - amountDiscount;
        double tax = amountAfterDiscountNotTax * 0.17;
        double amountPay = amountAfterDiscountNotTax + tax;
        System.out.println("amountBeforeTax " + amountBeforeTax + " amountDiscount " + amountDiscount
                + " amountAfterDiscountNotTax " + amountAfterDiscountNotTax
                + " tax " + tax + " amountPay " + amountPay);
        String test = "pre*0.1#pos*0.2#fbb*0.3";
        double discountRate = 0;
        String[] tmp1 = test.split("\\#");
        for (String discount : tmp1) {
            String[] tmp2 = discount.split("\\*");
            if (tmp2[0] != null && tmp2[1] != null
                    && tmp2[0].trim().toUpperCase().equals("FBB")) {
                discountRate = Double.valueOf(tmp2[1].trim());
                break;
            }
        }
        System.out.println("Hello " + discountRate);
    }
}
