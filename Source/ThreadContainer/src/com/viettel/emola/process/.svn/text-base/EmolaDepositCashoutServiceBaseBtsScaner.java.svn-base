/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.emola.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.obj.Config;
import com.viettel.paybonus.obj.EmolaTransactionInfo;
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
public class EmolaDepositCashoutServiceBaseBtsScaner extends ProcessRecordAbstract {

    Exchange pro;
    EmolaTransactionDbProcessor db;
    String countryCode;
    List<Config> lstConfig;
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
    String emolaPromDB = "dbEmolaPromotion";

    public EmolaDepositCashoutServiceBaseBtsScaner() {
        super();
        logger = Logger.getLogger(EmolaDepositCashoutServiceBaseBtsScaner.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        countryCode = ResourceBundle.getBundle("configPayBonus").getString("country_code");
        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
        db = new EmolaTransactionDbProcessor();
        if (Config.listMessage == null || Config.listMessage.isEmpty()) {
            Config.listMessage = db.getEmolaConfig();
        }
    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        for (Record record : listRecord) {
            EmolaTransactionInfo moRecord = (EmolaTransactionInfo) record;
            moRecord.setNodeName(holder.getNodeName());
            moRecord.setClusterName(holder.getClusterName());
        }
        return listRecord;
    }

    @Override
    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
        List<Record> listResult = new ArrayList<Record>();
        for (Record record : listRecord) {
            EmolaTransactionInfo bn = (EmolaTransactionInfo) record;
            listResult.add(bn);
            if ("0".equals(bn.getResultCode())) {
//                Step 1: Check BTS in list support promotion
                String misdn = bn.getMobile();
                if (!misdn.startsWith("258")) misdn = "258" + misdn;
                String mscNumChannel = pro.getMSCInfor(misdn, "");
                if (mscNumChannel.trim().length() <= 0) {
                    logger.warn("Can not get mscNumChannel for channel with ISDN= " + bn.getMobile());
                    bn.setResultCode("E3");
                    bn.setDescription("Can not get mscNumChannel to determine which BTS for support promotion");
                    continue;
                } else {
                    String cellIdChannel = pro.getCellIdRsString(bn.getMobile(), mscNumChannel, "");
                    if (cellIdChannel.trim().length() <= 0) {
                        logger.warn("Can not get cellIdChannel with mscNumChannel " + mscNumChannel + " mobile " + bn.getMobile());
                        bn.setResultCode("E4");
                        bn.setDescription("Can not get cellIdChannel to determine which BTS for support promotion");
                        continue;
                    } else {
                        String[] arrCellId = cellIdChannel.split("\\|");
                        if ((arrCellId != null) && (arrCellId.length == 2)) {
//          
//                            String cellCodeChannel = db.getCell("", arrCellId[0].trim(), arrCellId[1].trim());
                            String btsCodeChannel = db.getBts("", arrCellId[0].trim(), arrCellId[1].trim());
                            bn.setBtsReg(btsCodeChannel);
                            if (btsCodeChannel.trim().length() <= 0) {
                                logger.warn("Can not map cell and lac with BTS " + bn.getMobile());
                                bn.setResultCode("E5");
                                bn.setDescription("Can not map cell and lac with BTS");
                                continue;
                            } else {
                                String listBts = Config.getConfig(Config.listBts, logger);
                                if (listBts != null && !listBts.isEmpty() && !listBts.trim().toUpperCase().contains(btsCodeChannel)) {
                                    logger.warn("Not belong list BTS support promotion " + bn.getMobile() + " current BTS attached " + btsCodeChannel);
                                    bn.setResultCode("E7");
                                    bn.setDescription("Not belong list BTS support promotion" + listBts + ", current BTS attached " + btsCodeChannel);
                                    continue;
                                }
                            }
                        } else {
                            logger.warn("Invalid cellIdChannel " + bn.getMobile());
                            bn.setResultCode("E6");
                            bn.setDescription("Invalid cellIdChannel so can not get BTS");
                            continue;
                        }
                    }
                }
                    //                Step 2: Check already have promotion code for this isdn
                String channelCode = db.getChannelInfo(bn.getAgentWallet());
                if (channelCode != null && !"".equals(channelCode)) {
                    bn.setAgentChannelCode(channelCode);
                }

                int rsInsert = db.insertEmolaTransaction(bn);
                if (rsInsert <= 0) {
                    logger.warn("Fail to insert EmolaTransaction " + bn.getMobile()+ "" + bn.getTransCode()+ " errcode rsInsert " + rsInsert);
                    bn.setResultCode("E10");
                    bn.setDescription("Can not insert EmolaTransaction: " + bn.getMobile() + " invoice " + bn.getTransCode());
                    continue;
                }
             
//                Step 7: Write log and finish
            } else {
                logger.warn("After validate respone code is fail id " + bn.getTransCode()
                        + " so continue with other transaction");
                continue;
            }
        }
        listRecord.clear();
        Thread.sleep(60000);
        return listResult;
    }

    @Override
    public void printListRecord(List<Record> listRecord) throws Exception {
        StringBuilder br = new StringBuilder();
        SimpleDateFormat sdfs = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        br.setLength(0);
        br.append("\r\n").
                append("|\tTRANS_CODE|").
                append("|\tAMOUNT\t|").
                append("|\tMOBILE\t|").
                append("|\tSERVICE_ID\t|").
                append("|\tAGENT_MOBILE\t|").
                append("|\tAGENT_NAME\t|").
                append("|\tTRANS_REF\t|").
                append("|\tCREATE_TIME\t|");
        for (Record record : listRecord) {
            EmolaTransactionInfo bn = (EmolaTransactionInfo) record;
            br.append("\r\n").
                    append("|\t").
                    append(bn.getTransCode()).
                    append("||\t").
                    append(bn.getAmount()).
                    append("||\t").
                    append(bn.getMobile()).
                    append("||\t").
                    append(bn.getServiceId()).
                    append("||\t").
                    append(bn.getAgentWallet()).
                    append("||\t").
                    append(bn.getAgentName()).
                    append("||\t").
                    append((bn.getCreateTime() != null ? sdfs.format(bn.getCreateTime()) : null));
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
