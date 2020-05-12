/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbFixBroadband;
import com.viettel.paybonus.obj.SubAdslLLPrepaid;
import com.viettel.paybonus.service.Service;
import com.viettel.threadfw.manager.AppManager;
import java.util.List;
import org.apache.log4j.Logger;
import com.viettel.threadfw.process.ProcessRecordAbstract;
import com.viettel.vas.util.ExchangeClientChannel;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.ResourceBundle;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class FixBroadbandBlockThread extends ProcessRecordAbstract {
    
    DbFixBroadband db;
    Service pro;
    String msg;
    String isdn;
    String[] listSub;
    SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
    String pckgFtthMobile;
    String[] arrPackageFtthMobile;
    
    public FixBroadbandBlockThread() {
        super();
        logger = Logger.getLogger(FixBroadbandBlockThread.class);
    }
    
    @Override
    public void initBeforeStart() throws Exception {
        msg = ResourceBundle.getBundle("configPayBonus").getString("fbb_msg_block_expire");
        isdn = ResourceBundle.getBundle("configPayBonus").getString("fbb_warn_list_boss");
        listSub = isdn.split("\\|");
        db = new DbFixBroadband("cm_pos", logger);
        pro = new Service(ExchangeClientChannel.getInstance("../etc/service_client.cfg").getInstanceChannel(), logger);
        pckgFtthMobile = ResourceBundle.getBundle("configPayBonus").getString("PCKG_FTTH_MOBILE");
        arrPackageFtthMobile = pckgFtthMobile.split("\\|");
    }
    
    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        return listRecord;
    }
    
    @Override
    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
        List<Record> listResult = new ArrayList<Record>();
        String isdnContract;
        String msgWarn;
        String resultBlock;
        String center;
        for (Record record : listRecord) {
            isdnContract = "";
            resultBlock = "";
            center = "";
            SubAdslLLPrepaid bn = (SubAdslLLPrepaid) record;
            listResult.add(bn);
            msgWarn = msg.replace("%ACCOUNT%", bn.getAccount());
            center = db.getCenter(bn.getAccount());
            if (center == null || center.trim().length() <= 0) {
                logger.warn("Can not get center of account " + bn.getAccount() + " so set default center = 1");
                center = "1";
            }
//            Block
            resultBlock = pro.blockFBB(bn.getAccount(), center);
            if (!"0".equals(resultBlock)) {
                logger.error("Failt to block account " + bn.getAccount() + " when it's expired");
                for (String sub : listSub) {
                    db.sendSms(sub, "Failt to block account " + bn.getAccount() + " when it's expired", "86904", bn.getId());
                }
                Thread.sleep(5 * 60 * 1000); // sleep 5 minutes when have error before continuing process
            } else {
                //            Step 1 get phone of Contract
                isdnContract = db.getPhoneOfContract(bn.getContractId(), bn.getAccount());
//            Step2 send sms using 866123123 to send vodacom, mcell            
                db.sendSms(isdnContract, msgWarn, "866123123", bn.getId());
//            Step 3 update after blocking
                db.updateBlock(bn.getId(), bn.getAccount());
                db.updateSubAdslLl(bn.getId(), bn.getAccount());
                db.insertActionAudit(bn.getId(), bn.getAccount(),
                        "Blocked by System on account " + bn.getAccount() + " due to expire time", bn.getSubId());
//                LinhNBV 20180629: Update blocked_time on table: sub_mb_ftth_mobile if account exist
                for (String pkg : arrPackageFtthMobile) {
                    if (pkg.equals(bn.getNewProductCode())) {
                        int result = db.updateBlockedTimeFtthMobilePckg(bn.getAccount());
                        if (result == 1) {
                            logger.info("Update Blocked time success for account: " + bn.getAccount());
                        } else {
                            logger.info("Error or account not exist on table: sub_mb_ftth for account: " + bn.getAccount());
                        }
                    }
                }
                
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
                append("|\tSUB_ADSL_LL_PREPAID_ID|").
                append("|\tSUB_ID|").
                append("|\tCONTRACT_ID|").
                append("|\tACCOUNT\t|").
                append("|\tNEW_PRODUCT_CODE\t|").
                append("|\tPREPAID_TYPE\t|").
                append("|\tCREATE_SHOP\t|").
                append("|\tCREATE_USER\t|").
                append("|\tCREATE_TIME\t|").
                append("|\tEXPIRE_TIME\t|").
                append("|\tWARNING_COUNT\t|");
        for (Record record : listRecord) {
            SubAdslLLPrepaid bn = (SubAdslLLPrepaid) record;
            br.append("\r\n").
                    append("|\t").
                    append(bn.getSubAdslLlPrepaidId()).
                    append("||\t").
                    append(bn.getSubId()).
                    append("||\t").
                    append(bn.getContractId()).
                    append("||\t").
                    append(bn.getAccount()).
                    append("||\t").
                    append(bn.getNewProductCode()).
                    append("||\t").
                    append(bn.getPrepaidType()).
                    append("||\t").
                    append(bn.getCreateShop()).
                    append("||\t").
                    append(bn.getCreateUser()).
                    append("||\t").
                    append((bn.getCreateTime() != null ? sdf.format(bn.getCreateTime()) : null)).
                    append("||\t").
                    append((bn.getExpireTime() != null ? sdf.format(bn.getExpireTime()) : null)).
                    append("||\t").
                    append(bn.getWarningCount());
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
        System.out.println("rec20180403_101_743542.unl000000227141488OK.txt".substring(32, 41));
        SimpleDateFormat myFormat = new SimpleDateFormat("dd MM yyyy");
        String inputString1 = "05 03 2018";
        String inputString2 = "10 03 2018";
        
        try {
            Date date1 = myFormat.parse(inputString1);
            Date date2 = myFormat.parse(inputString2);
            long diff = date2.getTime() - date1.getTime();
            System.out.println("Days: " + TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS));
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
