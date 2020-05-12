/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbAddPromotionSubOnairBTS;
import com.viettel.paybonus.obj.BonusSubsBTSOnair;
import com.viettel.paybonus.service.Exchange;
import com.viettel.threadfw.manager.AppManager;
import java.util.List;
import org.apache.log4j.Logger;
import com.viettel.threadfw.process.ProcessRecordAbstract;
import com.viettel.vas.util.ExchangeClientChannel;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.ResourceBundle;

/**
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class AddPromotionSubsRegBTSOnair extends ProcessRecordAbstract {

    Exchange pro;
    DbAddPromotionSubOnairBTS db;
    Calendar cal;
    String balanceType;
    String bts3gDataValueForSubOnair;
    String bts3gDateValidForSubOnair;
    int validDate = 0;
    long dataValues = 0;
    String bts3GsmsSend2Cus;
    SimpleDateFormat sdf = new SimpleDateFormat("ddMMyyyyHHmmssSSS");
    SimpleDateFormat sdfEx = new SimpleDateFormat("yyyyMMddHHmmss");

    public AddPromotionSubsRegBTSOnair() {
        super();
        logger = Logger.getLogger(AddPromotionSubsRegBTSOnair.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
        db = new DbAddPromotionSubOnairBTS();
        bts3gDataValueForSubOnair = ResourceBundle.getBundle("configPayBonus").getString("bts3gDataValueForSubOnair");
        bts3gDateValidForSubOnair = ResourceBundle.getBundle("configPayBonus").getString("bts3gDateValidForSubOnair");
        bts3GsmsSend2Cus = ResourceBundle.getBundle("configPayBonus").getString("bts3GsmsSend2Cus");
        balanceType = "5300";
        validDate = getValidDate(bts3gDateValidForSubOnair);
        dataValues = getBytesData(bts3gDataValueForSubOnair);
    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        for (Record record : listRecord) {
            BonusSubsBTSOnair bn = (BonusSubsBTSOnair) record;
            bn.setNodeName(bn.getNodeName());
            bn.setClusterName(bn.getClusterName());
        }
        return listRecord;
    }

    @Override
    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
        List<Record> listResult = new ArrayList<Record>();
        long timeSt;
        String resultAddData = "";
        for (Record record : listRecord) {
            timeSt = System.currentTimeMillis();
            BonusSubsBTSOnair bn = (BonusSubsBTSOnair) record;
            listResult.add(bn);
            if (dataValues > 0 && validDate > 0) {
                cal = Calendar.getInstance();
                cal.setTime(new Date());
                cal.add(Calendar.DATE, validDate);

                logger.info("Begin add promotion for isdn:" + bn.getIsdn() + ", data:" + dataValues + ", valid date:" + validDate);
                if (db.checkTheFirstTimesReceivePromotion(bn) == Boolean.TRUE) {
                    try {
                        resultAddData = pro.addSmsDataVoice(bn.getIsdn(), String.valueOf(dataValues), balanceType, sdfEx.format(cal.getTime()));
                    } catch (Exception e) {
                        logger.info("Exception :" + bn.getIsdn() + ", " + e.toString());
                        bn.setStatus(1);
                        db.updatePromotionOnair(bn);
                        continue;
                    }

                    if ("0".equals(resultAddData)) {
                        logger.warn("Add data successfully " + bn.getIsdn() + " errcode resultChargeData " + resultAddData);
                        bn.setResultCode("0");
                        bn.setMbDataAdded(dataValues / 1024 / 1024);//conver from Bytes to MB
                        bn.setDescription("Add data successfully. MB data added:" + bn.getMbDataAdded());
                        bn.setCountProcess(bn.getCountProcess());
                        bn.setDuration(System.currentTimeMillis() - timeSt);
                        //insert into his table
                        if (db.insertPromotionOnairBTSHis(bn) < 1) {
                            logger.info(logger.getClass() + "Can not insert to PromotionOnairBTSHis table." + bn.getIsdn());
                        }
                        //delete failed
                        if (db.deletePromotionOnairBT(bn) < 1) {
                            logger.info(logger.getClass() + "Can not delete to PromotionOnairBTS table." + bn.getIsdn());
                            //try to update status
                            if (db.updatePromotionOnair(bn) == 1) {
                                logger.info(logger.getClass() + "Can not update to PromotionOnairBTS table." + bn.getIsdn());
                            }
                        }
                        //send sms to customer
                        db.sendSms(bn.getIsdn(), bts3GsmsSend2Cus, "86142");
                    } else {
                        logger.warn("Add data failed " + bn.getIsdn() + " errcode resultAddData " + resultAddData);
                        bn.setResultCode("E03");
                        bn.setMbDataAdded(0L);
                        bn.setDescription("Add data failed. Error code:" + resultAddData);
                        bn.setCountProcess(bn.getCountProcess());
                        bn.setDuration(System.currentTimeMillis() - timeSt);
                        //insert into his table
                        if (db.insertPromotionOnairBTSHis(bn) < 1) {
                            logger.info(logger.getClass() + "Can not insert to PromotionOnairBTSHis table." + bn.getIsdn());
                        }
                        //delete failed
                        if (db.deletePromotionOnairBT(bn) < 1) {
                            logger.info(logger.getClass() + "Can not delete to PromotionOnairBTS table." + bn.getIsdn());
                            //try to update status
                            if (db.updatePromotionOnair(bn) == 1) {
                                logger.info(logger.getClass() + "Can not update to PromotionOnairBTS table." + bn.getIsdn());
                            }
                        }
                    }
                } else {
                    logger.warn("Don't add data " + bn.getIsdn() + " Not the first time receive promotion ");
                    bn.setResultCode("E02");
                    bn.setMbDataAdded(0L);
                    bn.setDescription("Don't add data. Not the first time receive promotion.");
                    bn.setCountProcess(bn.getCountProcess());
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    //insert into his table
                    if (db.insertPromotionOnairBTSHis(bn) < 1) {
                        logger.info(logger.getClass() + "Can not insert to PromotionOnairBTSHis table." + bn.getIsdn());
                    }
                    //delete failed
                    if (db.deletePromotionOnairBT(bn) < 1) {
                        logger.info(logger.getClass() + "Can not delete to PromotionOnairBTS table." + bn.getIsdn());
                        //try to update status
                        if (db.updatePromotionOnair(bn) == 1) {
                            logger.info(logger.getClass() + "Can not update to PromotionOnairBTS table." + bn.getIsdn());
                        }
                    }
                }

            } else {//Don't have bonus configuration
                logger.info(logger.getClass() + "Don't add data. No configuration of promotion found." + bn.getIsdn());
                bn.setResultCode("E01");
                bn.setMbDataAdded(0L);
                bn.setDescription("Don't add data. No configuration of promotion found.");
                bn.setCountProcess(bn.getCountProcess());
                bn.setDuration(System.currentTimeMillis() - timeSt);
                if (db.insertPromotionOnairBTSHis(bn) < 1) {
                    logger.info(logger.getClass() + "Can not insert to PromotionOnairBTSHis table." + bn.getIsdn());
                }
                //delete failed
                if (db.deletePromotionOnairBT(bn) < 1) {
                    logger.info(logger.getClass() + "Can not delete to PromotionOnairBTS table." + bn.getIsdn());
                    //try to update status
                    if (db.updatePromotionOnair(bn) == 1) {
                        logger.info(logger.getClass() + "Can not update to PromotionOnairBTS table." + bn.getIsdn());
                    }
                }
            }
        }
        listRecord.clear();
        return listResult;
    }

    /**
     * Convert from Megabytes to Bytes
     *
     * @param mbDataStr
     * @return
     */
    public long getBytesData(String mbDataStr) {
        long mbData = 0L;
        try {
            if (mbDataStr != null && mbDataStr.trim().length() > 0) {
                mbData = Long.valueOf(mbDataStr.trim()) * 1024 * 1024;//--Bytes ->> MB
            }
        } catch (Exception ex) {
            logger.error("BonusChangeSim4G --> getMBBonusData --> " + ex.getMessage());
            return 0L;
        }
        return mbData;
    }

    /**
     * Get valid date when add promotion
     *
     * @param mbDateStr
     * @return
     */
    public int getValidDate(String mbDateStr) {
        int valDate = 0;
        try {
            if (mbDateStr != null && mbDateStr.trim().length() > 0) {
                valDate = Integer.valueOf(mbDateStr.trim());
                return valDate;
            }
        } catch (Exception ex) {
            logger.error("BonusChangeSim4G --> getValidDate --> " + ex.getMessage());
            return 0;
        }
        return valDate;
    }

    @Override
    public void printListRecord(List<Record> listRecord) throws Exception {
        StringBuilder br = new StringBuilder();
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        br.setLength(0);
        br.append("\r\n").
                append("|\tID|").
                append("|\tisdn\t|").
                append("|\tcreate_time\t|").
                append("|\tstatus\t|").
                append("|\tbts\t|");
        for (Record record : listRecord) {
            BonusSubsBTSOnair bn = (BonusSubsBTSOnair) record;
            br.append("\r\n").
                    append("|\t").
                    append(bn.getId()).
                    append("||\t").
                    append(bn.getIsdn()).
                    append("||\t").
                    append(bn.getCreateTime()).
                    append("||\t").
                    append(bn.getStatus()).
                    append("||\t").
                    append(bn.getBtsAtt()).
                    append("||\t");
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
}
