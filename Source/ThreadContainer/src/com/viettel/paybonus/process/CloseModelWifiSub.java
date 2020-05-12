/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbComplainClose;
import com.viettel.paybonus.obj.ComplainInfo;
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
public class CloseModelWifiSub extends ProcessRecordAbstract {
    
    Exchange pro;
    DbComplainClose db;
    String countryCode;
    
    public CloseModelWifiSub() {
        super();
        logger = Logger.getLogger(CloseModelWifiSub.class);
    }
    
    @Override
    public void initBeforeStart() throws Exception {
        countryCode = ResourceBundle.getBundle("configPayBonus").getString("country_code");
        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
        db = new DbComplainClose();
    }
    
    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        return listRecord;
    }
    
    @Override
    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
        List<Record> listResult = new ArrayList<Record>();
        String closeFlagResult;
        for (Record record : listRecord) {
            closeFlagResult = "";
            ComplainInfo bn = (ComplainInfo) record;
            listResult.add(bn);
            if ("0".equals(bn.getResultCode())) {
//                Close GPRS flag on HLR for customer
                closeFlagResult = pro.lockGPRS(bn.getRescueModelIsdn());
                if ("0".equals(closeFlagResult)) {
                    logger.info("Close flag GPRS successfully for sub when complain closed " + bn.getRescueModelIsdn()
                            + " complainid " + bn.getId());
                    bn.setResultCode("0");
                    bn.setDescription("Close flag GPRS successfully");
                    logger.info("Start update SubMB for isdn " + bn.getRescueModelIsdn());
                    db.updateSubMb(bn.getRescueModelIsdn(), bn.getRescueCcUser());
                    db.updateRescueHis(bn.getRescueModelIsdn(), bn.getRescueCcUser());
//                    Reset data fix minus 20GB
                    pro.addSmsDataVoice(bn.getRescueModelIsdn(), "-21474836480", "4500", null);
                    continue;
                } else {
                    logger.warn("Fail to close flag GPRS for sub when complain closed " + bn.getRescueModelIsdn()
                            + " complainid " + bn.getId());
                    bn.setResultCode("E01");
                    bn.setDescription("Fail to close flag GPRS when complain closed");
                    continue;
                }
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
                append("|\tCOMPLAIN_ID|").
                append("|\tRESCUE_CC_USER\t|").
                append("|\tRESCUE_MODEL_ISDN\t|").
                append("|\tRESCUE_TIME\t|");
        for (Record record : listRecord) {
            ComplainInfo bn = (ComplainInfo) record;
            br.append("\r\n").
                    append("|\t").
                    append(bn.getId()).
                    append("||\t").
                    append(bn.getRescueCcUser()).
                    append("||\t").
                    append(bn.getRescueModelIsdn()).
                    append("||\t").
                    append((bn.getRescueTime() != null ? sdf.format(bn.getRescueTime()) : null));
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
