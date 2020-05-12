/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.pincode.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbAgentOrderRetryExportPincode;
import com.viettel.paybonus.obj.PinCode;
import java.util.List;
import org.apache.log4j.Logger;
import com.viettel.threadfw.process.ProcessRecordAbstract;
import java.util.ArrayList;
import java.util.ResourceBundle;

/**
 * @author LinhNBV
 */
public class AgentOrderRetryExportPinCode extends ProcessRecordAbstract {

    DbAgentOrderRetryExportPincode db;
    String lstIsdnReceiveError;
    String[] arrIsdnRecevieError;

    public AgentOrderRetryExportPinCode() {
        super();
        logger = Logger.getLogger(AgentOrderRetryExportPinCode.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        db = new DbAgentOrderRetryExportPincode("dbsm", logger);
        lstIsdnReceiveError = ResourceBundle.getBundle("configPayBonus").getString("lstIsdnReceiveError");
        arrIsdnRecevieError = lstIsdnReceiveError.split("\\|");
    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        for (Record record : listRecord) {
            PinCode moRecord = (PinCode) record;
            moRecord.setNodeName(holder.getNodeName());
            moRecord.setClusterName(holder.getClusterName());
        }
        return listRecord;
    }

    @Override
    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
        List<Record> listResult = new ArrayList<Record>();

        for (Record record : listRecord) {
            PinCode bn = (PinCode) record;
            listResult.add(bn);
            if ("0".equals(bn.getResultCode())) {
                logger.info("Step 1: Check saleTransOrderId: " + bn.getSaleTransOrderId() + " in table sale_trans_order have saleTransId or not.");
                logger.info("Step 2: Check saleTransOrderId: " + bn.getSaleTransOrderId() + " in table agent_trans_order_his have saleTransId or not.");
                if (db.checkTransactionAutoExported(bn.getSaleTransOrderId())) {
                    logger.info("Export order success >> update status = 1 >> send email, saleTransOrderId: " + bn.getSaleTransOrderId());
                    bn.setResultCode("0");
                    bn.setStatus(1L);
                    bn.setDescription("Retry export order successfully.");
                    continue;
                }
                logger.info("Export order not yet success >> update status = 0 and retry export order, saleTransOrderId: " + bn.getSaleTransOrderId());
                bn.setResultCode("E01");
                bn.setStatus(0L);// or set status = 0 >> auto export again...
                bn.setDescription("Auto export order not yet success. Retry.");
                continue;


            } else {
                logger.warn("After validate respone code is fail saleTransOrderId " + bn.getSaleTransOrderId()
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
        br.setLength(0);
        br.append("\r\n").
                append("|\tSALE_TRANS_ORDER_ID\t|").
                append("|\tRECEIVER_ID\t|").
                append("|\tCREATE_STAFF_ID");
        for (Record record : listRecord) {
            PinCode bn = (PinCode) record;
            br.append("\r\n").
                    append("|\t").
                    append(bn.getSaleTransOrderId()).
                    append("||\t").
                    append(bn.getReceiverId()).
                    append("||\t").
                    append(bn.getCreateStaffId());
        }
        logger.info(br);
    }

    @Override
    public List<Record> processException(List<Record> listRecord, Exception ex) {
        ex.printStackTrace();
        logger.warn("TEMPLATE process exception record: " + ex.toString());
        return listRecord;
    }

    @Override
    public boolean startProcessRecord() {
        return true;
    }
}
