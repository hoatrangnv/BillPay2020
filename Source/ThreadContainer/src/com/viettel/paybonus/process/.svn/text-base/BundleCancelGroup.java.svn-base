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
public class BundleCancelGroup extends ProcessRecordAbstract {

    Exchange pro;
    DbBundleProcessor db;
    String countryCode;

    public BundleCancelGroup() {
        super();
        logger = Logger.getLogger(BundleCancelGroup.class);
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
        ArrayList<BundleHis> listMember;
        ArrayList<BundleHis> listLog;
        long groupId;
        for (Record record : listRecord) {
            contract = null;
            listMember = null;
            groupId = 0;
            BundleHis bn = (BundleHis) record;
            listResult.add(bn);
            if ("0".equals(bn.getResultCode())) {
//            Step 1: get ContractId, TelFax from account
                logger.info("Start get contract info of account " + bn.getFtthAccount());
                contract = db.getContractInfo(bn.getFtthAccount());
                if (contract == null || contract.getContractId() <= 0) {
                    logger.info("Can not find active contract of account " + bn.getFtthAccount()
                            + ", so now destroy group of this account " + bn.getOwnerSub());
                    listMember = db.getListBundle(bn.getOwnerSub());
                    if (listMember == null || listMember.size() <= 0) {
                        bn.setResultCode("E01");
                        bn.setDescription("Do not find any member so can not destroy group when contract canceled");
                        continue;
                    }
//                    Determine groupId
                    for (BundleHis member : listMember) {
                        if (member.getOwnerSub().equals(member.getMemberSub())) {
                            groupId = member.getMoHisId();
                        }
                    }
                    if (groupId <= 0) {
                        bn.setResultCode("E02");
                        bn.setDescription("Do not determine groupid so can not destroy group when contract canceled");
                        continue;
                    }
                    for (BundleHis member : listMember) {
                        logger.info("Remove member " + member.getMemberSub() + " owner " + member.getOwnerSub() + " groupid " + groupId);
                        pro.bundleAdjustMember(member.getMemberSub(), "2", groupId + "", "104");
                        logger.info("Remove priceplan " + member.getMemberSub());
                        pro.removePrice(member.getMemberSub(), "11205034");
                    }
                    logger.info("Cancel group on OCS groupid " + groupId);
                    pro.bundleAdjustGroup(groupId + "", bn.getOwnerSub(), "104", "2");
                    logger.info("Cancel group on DB groupid " + groupId);
                    db.cancelBundle(bn.getOwnerSub(), "4");
                    bn.setResultCode("0");
                    bn.setDescription("Destroy group because can not find active contract of account " + bn.getOwnerSub());
                    logger.info("Make log after destroy group owner " + bn.getOwnerSub() + " groupid " + groupId);
                    listLog = new ArrayList<BundleHis>();
                    for (BundleHis member : listMember) {
                        BundleHis log = new BundleHis();
                        log.setOwnerSub(bn.getOwnerSub());
                        log.setMoHisId(bn.getMoHisId());
                        log.setBatchId(bn.getBatchId());
                        log.setEndReason("4");
                        log.setFtthAccount(bn.getFtthAccount());
                        log.setMemberSub(member.getMemberSub());
                        log.setMessage("Destroy group because can not find active contract of account");
                        log.setStartTime(bn.getStartTime());
                        listLog.add(log);
                    }
                    db.insertBundleLog(listLog);
                    continue;
                }
//            Step 2: check TelFax had changed in one day ago, if true let cancel group
                if (db.checkTelChanged(bn.getOwnerSub(), contract.getContractId())) {
                    logger.info("Tel of contract has changed " + contract.getTelFax()
                            + ", so now destroy group, contract " + contract.getContractId());
                    listMember = db.getListBundle(bn.getOwnerSub());
                    if (listMember == null || listMember.size() <= 0) {
                        bn.setResultCode("E01");
                        bn.setDescription("Do not find any member so can not destroy group when tel changed");
                        continue;
                    }
//                    Determine groupId
                    for (BundleHis member : listMember) {
                        if (member.getOwnerSub().equals(member.getMemberSub())) {
                            groupId = member.getMoHisId();
                        }
                    }
                    if (groupId <= 0) {
                        bn.setResultCode("E02");
                        bn.setDescription("Do not determine groupid so can not destroy group when tel changed");
                        continue;
                    }
                    for (BundleHis member : listMember) {
                        logger.info("Remove member " + member.getMemberSub() + " owner " + member.getOwnerSub() + " groupid " + groupId);
                        pro.bundleAdjustMember(member.getMemberSub(), "2", groupId + "", "104");
                        logger.info("Remove priceplan " + member.getMemberSub());
                        pro.removePrice(member.getMemberSub(), "11205034");
                    }
                    logger.info("Cancel group on OCS groupid " + groupId);
                    pro.bundleAdjustGroup(groupId + "", bn.getOwnerSub(), "104", "2");
                    logger.info("Cancel group on DB groupid " + groupId);
                    db.cancelBundle(bn.getOwnerSub(), "2");
                    bn.setResultCode("0");
                    bn.setDescription("Tel of contract has changed " + contract.getTelFax());
                    logger.info("Make log after destroy group owner " + bn.getOwnerSub() + " groupid " + groupId);
                    listLog = new ArrayList<BundleHis>();
                    for (BundleHis member : listMember) {
                        BundleHis log = new BundleHis();
                        log.setOwnerSub(bn.getOwnerSub());
                        log.setMoHisId(bn.getMoHisId());
                        log.setBatchId(bn.getBatchId());
                        log.setEndReason("2");
                        log.setFtthAccount(bn.getFtthAccount());
                        log.setMemberSub(member.getMemberSub());
                        log.setMessage("Tel of contract has changed");
                        log.setStartTime(bn.getStartTime());
                        listLog.add(log);
                    }
                    db.insertBundleLog(listLog);
                    continue;
                }
//            Step 3: check contract debit at least 2 month ago, if true let cancel group
                if (db.checkContractDebit(bn.getOwnerSub(), contract.getContractId())) {
                    logger.info("Contract has debit over two months " + contract.getTelFax()
                            + ", so now destroy group, contractid " + contract.getContractId());
                    listMember = db.getListBundle(bn.getOwnerSub());
                    if (listMember == null || listMember.size() <= 0) {
                        bn.setResultCode("E01");
                        bn.setDescription("Do not find any member so can not destroy group when contract debit");
                        continue;
                    }
//                    Determine groupId
                    for (BundleHis member : listMember) {
                        if (member.getOwnerSub().equals(member.getMemberSub())) {
                            groupId = member.getMoHisId();
                        }
                    }
                    if (groupId <= 0) {
                        bn.setResultCode("E02");
                        bn.setDescription("Do not determine groupid so can not destroy group when contract debit");
                        continue;
                    }
                    for (BundleHis member : listMember) {
                        logger.info("Remove member " + member.getMemberSub() + " owner " + member.getOwnerSub() + " groupid " + groupId);
                        pro.bundleAdjustMember(member.getMemberSub(), "2", groupId + "", "104");
                        logger.info("Remove priceplan " + member.getMemberSub());
                        pro.removePrice(member.getMemberSub(), "11205034");
                    }
                    logger.info("Cancel group on OCS groupid " + groupId);
                    pro.bundleAdjustGroup(groupId + "", bn.getOwnerSub(), "104", "2");
                    logger.info("Cancel group on DB groupid " + groupId);
                    db.cancelBundle(bn.getOwnerSub(), "3");
                    bn.setResultCode("0");
                    bn.setDescription("Contract has debit over two months, contractid " + contract.getContractId());
                    logger.info("Make log after destroy group owner " + bn.getOwnerSub() + " groupid " + groupId);
                    listLog = new ArrayList<BundleHis>();
                    for (BundleHis member : listMember) {
                        BundleHis log = new BundleHis();
                        log.setOwnerSub(bn.getOwnerSub());
                        log.setMoHisId(bn.getMoHisId());
                        log.setBatchId(bn.getBatchId());
                        log.setEndReason("3");
                        log.setFtthAccount(bn.getFtthAccount());
                        log.setMemberSub(member.getMemberSub());
                        log.setMessage("Contract has debit over two months");
                        log.setStartTime(bn.getStartTime());
                        listLog.add(log);
                    }
                    db.insertBundleLog(listLog);
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
