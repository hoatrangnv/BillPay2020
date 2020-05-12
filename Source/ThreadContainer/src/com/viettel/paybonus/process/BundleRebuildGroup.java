/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbBundleProcessor;
import com.viettel.paybonus.obj.BundleHis;
import com.viettel.paybonus.obj.ContractInfo;
import com.viettel.paybonus.service.Exchange;
import com.viettel.threadfw.manager.AppManager;
import java.util.List;
import org.apache.log4j.Logger;
import com.viettel.threadfw.process.ProcessRecordAbstract;
import com.viettel.vas.util.ExchangeClientChannel;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.ResourceBundle;

/**
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class BundleRebuildGroup extends ProcessRecordAbstract {

    Exchange pro;
    DbBundleProcessor db;
    String countryCode;

    public BundleRebuildGroup() {
        super();
        logger = Logger.getLogger(BundleRebuildGroup.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        countryCode = ResourceBundle.getBundle("configPayBonus").getString("country_code");
        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
        db = new DbBundleProcessor();
    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        return listRecord;
    }

    @Override
    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
        List<Record> listResult = new ArrayList<Record>();
        ContractInfo contract;
        ArrayList<String> listMember;
        for (Record record : listRecord) {
            contract = null;
            listMember = null;
            BundleHis bn = (BundleHis) record;
            listResult.add(bn);
            if ("0".equals(bn.getResultCode())) {
//            Step 1: get ContractId, TelFax from account
                logger.info("Start get contract info of account " + bn.getFtthAccount());
                contract = db.getContractInfo(bn.getFtthAccount());
                if (contract == null || contract.getContractId() <= 0) {
                    logger.info("Can not find active contract of account " + bn.getFtthAccount()
                            + ", so continue " + bn.getOwnerSub());
                    continue;
                }
//            Step 2: check contract not debit at least 2 month ago, if true let rebuilt group
                if (!db.checkContractDebit(bn.getOwnerSub(), contract.getContractId())) {
                    logger.info("Contract has just payment now not debit over two months more " + contract.getTelFax()
                            + ", so now must rebuilt group, contractid " + contract.getContractId());
                    listMember = db.getListBundleToRecover(bn.getMoHisId());
                    if (listMember == null || listMember.size() <= 0) {
                        logger.warn("Do not find any member so can not recover group when contract payment " + bn.getOwnerSub()
                                + " " + bn.getMoHisId());
                        bn.setResultCode("E01");
                        bn.setDescription("Do not find any member so can not recover group when contract payment");
                        continue;
                    }
                    logger.info("Recover group on OCS groupid " + bn.getMoHisId());
                    pro.bundleAdjustGroup(bn.getMoHisId() + "", bn.getOwnerSub(), "104", "1");
                    for (String member : listMember) {
                        logger.info("Add again member " + member + " owner " + bn.getOwnerSub() + " groupid " + bn.getMoHisId());
                        pro.bundleAdjustMember(member, "1", bn.getMoHisId() + "", "104");
                        logger.info("Add priceplan " + member);
                        pro.addPrice(member, "11205034", "", "-1");
                        logger.info("Recover group on DB groupid " + bn.getMoHisId() + " member " + member);
                        db.recoverBundle(bn.getOwnerSub());
                        db.updateBundleLog(bn.getOwnerSub(), member);
                    }
                    bn.setResultCode("0");
                    bn.setDescription("Recover group success");
                    continue;
                }
            } else {
                logger.warn("After validate respone code is fail id " + bn.getId()
                        + " so continue with other transaction");
                continue;
            }
        }
        listRecord.clear();
        Thread.sleep(90 * 1000);
        return listResult;
    }

    @Override
    public void printListRecord(List<Record> listRecord) throws Exception {
        StringBuilder br = new StringBuilder();
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        br.setLength(0);
        br.append("\r\n").
                append("|\tMO_HIS_ID|").
                append("|\tOWNER\t|").
                append("|\tACCOUNT\t|").
                append("|\tSTART_TIME\t|");
        for (Record record : listRecord) {
            BundleHis bn = (BundleHis) record;
            br.append("\r\n").
                    append("|\t").
                    append(bn.getId()).
                    append("||\t").
                    append(bn.getOwnerSub()).
                    append("||\t").
                    append(bn.getFtthAccount()).
                    append("||\t").
                    append((bn.getStartTime() != null ? sdf.format(bn.getStartTime()) : null));
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
}
