/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.cug.manage;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbMakeSaleTranCug;
import com.viettel.paybonus.obj.StaffInfo;
import com.viettel.paybonus.obj.TransactionInfo;
import com.viettel.paybonus.obj.VipSubInfo;
import com.viettel.paybonus.service.Exchange;
import com.viettel.threadfw.manager.AppManager;
import java.util.List;
import org.apache.log4j.Logger;
import com.viettel.threadfw.process.ProcessRecordAbstract;
import com.viettel.vas.util.ExchangeClientChannel;
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
public class CugSaleTranMaker extends ProcessRecordAbstract {

    DbMakeSaleTranCug db;
    int cugReason;
    double cugBonusRate;
    double cugBonusRateRenew;
    String cugSaleCode;
    String cugMsgToStaff;
    String cugMsgBonus;
    String isdn;
    String[] listSub;
    Exchange pro;

    public CugSaleTranMaker() {
        super();
        logger = Logger.getLogger(CugSaleTranMaker.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        db = new DbMakeSaleTranCug();
        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
        cugReason = Integer.valueOf(ResourceBundle.getBundle("configPayBonus").getString("cugReason"));
        cugBonusRate = Double.valueOf(ResourceBundle.getBundle("configPayBonus").getString("cugBonusRate"));
        cugBonusRateRenew = Double.valueOf(ResourceBundle.getBundle("configPayBonus").getString("cugBonusRateRenew"));
        cugSaleCode = ResourceBundle.getBundle("configPayBonus").getString("cugSaleCode");
        cugMsgToStaff = ResourceBundle.getBundle("configPayBonus").getString("cugMsgToStaff");
        cugMsgBonus = ResourceBundle.getBundle("configPayBonus").getString("cugMsgBonus");
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
        double discountRate;
        double amountDiscount;
        double tax;
        double amountPay;
        double amountBeforeTax;
        long reasonId;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        VipSubInfo vipInfo;
        StaffInfo staffInfo;
        String staffCode;
        String saleCode;
        for (Record record : listRecord) {
            resultSaleTran = 0;
            resultSaleTranDetail = 0;
            resultUpdateTranLog = 0;
            discountRate = 0;
            amountDiscount = 0;
            amountPay = 0;
            amountBeforeTax = 0;
            tax = 0;
            reasonId = 0;
            staffInfo = null;
            staffCode = "";
            saleCode = "";
            vipInfo = new VipSubInfo();
            TransactionInfo bn = (TransactionInfo) record;
            listResult.add(bn);
            if ("0".equals(bn.getResultCode())) {
//                Get staff info
                staffCode = db.getUser(bn.getPolicyId());
                staffInfo = db.getStaffInfo(staffCode);
                if (staffInfo == null || staffInfo.getStaffId() <= 0 || staffInfo.getShopId() <= 0
                        || discountRate < 0 || cugReason <= 0 || cugSaleCode == null || cugSaleCode.length() <= 0) {
                    logger.warn("Do not have info of staff or discountRate or cugReason or saleCode is invalid, policyId " + bn.getPolicyId()
                            + " discountRate " + discountRate + " reasonId " + reasonId + " saleCode " + cugSaleCode);
                    bn.setResultCode("1");
                    bn.setDescription("Do not have info of staff or discountRate or cugReason or saleCode is invalid, policyId " + bn.getPolicyId());
                    continue;
                }
                bn.setClient(staffInfo.getStaffCode());
                saleCode = cugSaleCode + sdf.format(new Date());
//                get saleTransId
                long saleTransId = db.getSaleTransId(bn.getClient());
                if (saleTransId <= 0) {
                    logger.warn("Fail to get saleTransId for policyId " + bn.getPolicyId());
                    for (String sub : listSub) {
                        db.sendSms(sub, "Fail to get saleTransId for policyId "
                                + bn.getPolicyId() + " when making SaleTrans", "86904");
                    }
                    bn.setResultCode("2");
                    bn.setDescription("Fail to get saleTransId for policyId " + bn.getPolicyId());
                    continue;
                }
//                Calculate tax, amountTax, discount
                if (bn.getMoney() <= 0) {
                    logger.warn("Money is <=0 so can not make sale trans for policyId " + bn.getPolicyId());
                    bn.setResultCode("3");
                    bn.setDescription("Money is <=0 so can not make sale trans for policyId " + bn.getPolicyId());
                    continue;
                }
                amountBeforeTax = bn.getMoney() / 1.17;
                amountDiscount = amountBeforeTax * discountRate;
                double amountAfterDiscountNotTax = amountBeforeTax - amountDiscount;
                tax = amountAfterDiscountNotTax * 0.17;
                amountPay = amountAfterDiscountNotTax + tax;
                logger.warn("Start make sale_trans for policyId " + bn.getPolicyId() + " amountBeforeTax " + bn.getMoney()
                        + " tax " + tax + " amountBeforeTax " + amountBeforeTax
                        + " amountDiscount " + amountDiscount
                        + " amountPay " + amountPay
                        + " shopId " + staffInfo.getShopId() + " staffId " + staffInfo.getStaffId() + " discountRate " + discountRate
                        + " reasonId " + reasonId + " saleCode " + saleCode);
                vipInfo = db.getVipSubInfo(bn.getPolicyId());
//                insert sale_trans
                resultSaleTran = db.insertSaleTrans(saleTransId, staffInfo.getShopId(), staffInfo.getStaffId(),
                        amountDiscount, amountPay, amountBeforeTax,
                        tax, reasonId, saleCode, bn.getPolicyId(), vipInfo.getCustName(), vipInfo.getCustTel(), vipInfo.getCustAddress());
                if (resultSaleTran <= 0) {
                    logger.warn("Fail to insert SaleTrans policyId " + bn.getPolicyId()
                            + " money " + bn.getMoney() + " resultSaleTran " + resultSaleTran);
                    for (String sub : listSub) {
                        db.sendSms(sub, "Fail to insert SaleTrans for policyId "
                                + bn.getPolicyId() + " when making SaleTrans", "86904");
                    }
                    bn.setResultCode("4");
                    bn.setDescription("Fail to insert SaleTrans" + " policyId " + bn.getPolicyId());
                    continue;
                }
//                insert sale_trans_detail
                resultSaleTranDetail = db.insertSaleTransDetail(saleTransId, bn.getMoney(), amountDiscount, bn.getPolicyId());
                if (resultSaleTranDetail <= 0) {
                    logger.warn("Fail to insert SaleTranDetail policyId " + bn.getPolicyId()
                            + " money " + bn.getMoney() + " resultSaleTranDetail " + resultSaleTranDetail);
                    for (String sub : listSub) {
                        db.sendSms(sub, "Fail to insert SaleTranDetail for policyId "
                                + bn.getPolicyId() + " when making SaleTrans", "86904");
                    }
                    bn.setResultCode("5");
                    bn.setDescription("Fail to insert SaleTranDetail" + " policyId " + bn.getPolicyId());
                    continue;
                }
//                update TranLog
                resultUpdateTranLog = db.updateTransLog(saleTransId, saleCode, bn.getPolicyId());
                logger.info("Quantity row need to make sale trans " + bn.getTranInitCount()
                        + " quantity updated " + resultUpdateTranLog + " VipSubInfoId " + bn.getVipSubInfoId());
                bn.setTranUpdateCount(resultUpdateTranLog);
//                Finish
                bn.setResultCode("0");
                bn.setDescription("Finish make sale trans for staff " + bn.getClient() + " policyId " + bn.getPolicyId());
//                Send sms to staff
                String msg = cugMsgToStaff.replace("%CUSTNAME%", vipInfo.getCustName());
                msg = msg.replace("%MONEY%", amountPay + "");
                msg = msg.replace("%MEMBER%", bn.getTranInitCount() + "");
                String tel = db.getTelByStaffCode(staffCode);
                db.sendSms(tel, msg, "86904");
//                Pay bonus to staff
                long money = 0;
                if (db.checkRenewPolicy(bn.getPolicyId())) {
                    logger.warn("the policyId " + bn.getPolicyId()
                            + " is renew so calculate bonus by cugBonusRateRenew " + cugBonusRateRenew);
                    money = Math.round(amountPay * cugBonusRateRenew / 100);
                } else {
                    logger.info("the policyId " + bn.getPolicyId()
                            + " is first time so calculate bonus by cugBonusRate " + cugBonusRate);
                    money = Math.round(amountPay * cugBonusRate / 100);
                }
                if (money > 0) {
                    if (staffInfo.getTel() != null && staffInfo.getTel().trim().length() > 0) {
                        String eWalletResponse = pro.callEwallet(vipInfo.getVipSubInfoId(), staffInfo.getChannelTypeId(), staffInfo.getTel(),
                                money, "615", staffCode, sdf.format(new Date()), db);
                        if ("01".equals(eWalletResponse)) {
                            logger.info("Pay Bonus success for policyId " + bn.getPolicyId() + " isdnEmola "
                                    + staffInfo.getTel() + " amount " + money);
                            bn.setDescription("Finish make sale trans and pay bonus for staff " + bn.getClient() + " policyId " + bn.getPolicyId());
                            String msgBonus = cugMsgBonus;
                            msgBonus = msgBonus.replace("%MONEY%", "" + money);
                            msgBonus = msgBonus.replace("%CUSTNAME%", vipInfo.getCustName());
                            db.sendSms(staffInfo.getTel(), msgBonus, "86904");
                        } else {
                            logger.error("Make sale trans success but fail to pay bonus " + bn.getClient()
                                    + " isdnEmola " + staffInfo.getTel() + " amount " + money);
                            bn.setResultCode("6");
                            bn.setDescription("Make sale trans success but fail to pay bonus " + bn.getClient() + " amount " + money);
                        }
                    } else {
                        logger.warn("Do not have isdnEmola to pay bonus " + bn.getClient()
                                + " isdnEmola " + staffInfo.getTel() + " amount " + money + " policyId " + bn.getPolicyId());
                    }
                } else {
                    logger.warn("The money not greater 0 so not pay bonus " + bn.getClient()
                            + " isdnEmola " + staffInfo.getTel() + " amount " + money + " policyId " + bn.getPolicyId());
                }
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
                append("|\tTRAN_INIT_COUNT\t|").
                append("|\tMONEY\t|").
                append("|\tPOLICY_ID\t|").
                append("|\tTRAN_DATE\t|");
        for (Record record : listRecord) {
            TransactionInfo bn = (TransactionInfo) record;
            br.append("\r\n").
                    append("||\t").
                    append(bn.getTranInitCount()).
                    append("||\t").
                    append(bn.getMoney()).
                    append("||\t").
                    append(bn.getPolicyId()).
                    append("||\t").
                    append((bn.getTransTime() != null ? sdf.format(bn.getTransTime()) : null));
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
