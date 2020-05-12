/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbSmasProcessor;
import com.viettel.paybonus.obj.SmasAnnounce;
import java.util.List;
import org.apache.log4j.Logger;
import com.viettel.threadfw.process.ProcessRecordAbstract;
import java.util.ArrayList;

/**
 *
 * @author HuyNQ1
 * @version 1.0
 * @since 24-03-2016
 */
public class SmasProcessAnnounce extends ProcessRecordAbstract {

    DbSmasProcessor db;
    String msg;

    public SmasProcessAnnounce() {
        super();
        logger = Logger.getLogger(SmasProcessAnnounce.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        db = new DbSmasProcessor();
    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        return listRecord;
    }

    @Override
    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
        String phone;
        ArrayList<String> listPhone;
        String shortCode;
        for (Record record : listRecord) {
            SmasAnnounce bn = (SmasAnnounce) record;
            phone = "";
            shortCode = "";
            listPhone = new ArrayList<String>();
            shortCode = db.getShortCodeOfSchool(bn.getSchoolId());
            if (shortCode == null || shortCode.trim().length() <= 0) {
                shortCode = "86904"; //Defaul send by Movitel
            }
//            Check send to person
            if (bn.getSendType() == 1) {
                if (bn.getAnnounceType() == 2) {
//                    Send to parent
                    phone = db.getParentPhoneById(bn.getReceiverId());
                    if (phone != null && phone.trim().length() > 0) {
                        db.sendSms(phone, "[" + bn.getAnnounceName() + "] " + bn.getContent(), shortCode);
                        logger.info("Finish to send " + bn.getAnnounceId());
                        bn.setResultCode("00");
                        bn.setDescription("Finish to send");
                        continue;
                    } else {
                        logger.info("Can not find phone of parent " + bn.getAnnounceId());
                        bn.setResultCode("E03");
                        bn.setDescription("Can not find phone of parent");
                        continue;
                    }
                }
                if (bn.getAnnounceType() == 3) {
//                    Send to student
                    phone = db.getStudentPhoneById(bn.getReceiverId());
                    if (phone != null && phone.trim().length() > 0) {
                        db.sendSms(phone, "[" + bn.getAnnounceName() + "] " + bn.getContent(), shortCode);
                        logger.info("Finish to send " + bn.getAnnounceId());
                        bn.setResultCode("00");
                        bn.setDescription("Finish to send");
                        continue;
                    } else {
                        logger.info("Can not find phone of student " + bn.getAnnounceId());
                        bn.setResultCode("E04");
                        bn.setDescription("Can not find phone of student");
                        continue;
                    }
                }
                if (bn.getAnnounceType() == 4 || bn.getAnnounceType() == 8) {
//                    Send to teacher
                    phone = db.getTeacherPhoneById(bn.getReceiverId());
                    if (phone != null && phone.trim().length() > 0) {
                        db.sendSms(phone, "[" + bn.getAnnounceName() + "] " + bn.getContent(), shortCode);
                        logger.info("Finish to send " + bn.getAnnounceId());
                        bn.setResultCode("00");
                        bn.setDescription("Finish to send");
                        continue;
                    } else {
                        logger.info("Can not find phone of teacher " + bn.getAnnounceId());
                        bn.setResultCode("E02");
                        bn.setDescription("Can not find phone of teacher");
                        continue;
                    }
                }
            } else if (bn.getSendType() == 2) {
//                Send to all student of class
                listPhone = db.getAllStudentByClass(bn.getReceiverId());
                for (String p : listPhone) {
                    if (p != null && p.trim().length() > 0) {
                        db.sendSms(p, "[" + bn.getAnnounceName() + "] " + bn.getContent(), shortCode);
                    }
                }
                logger.info("Finish to send " + bn.getAnnounceId());
                bn.setResultCode("00");
                bn.setDescription("Finish to send");
                continue;
            } else if (bn.getSendType() == 3) {
//                Send to all parent of class
                listPhone = db.getAllParentByClass(bn.getReceiverId());
                for (String p : listPhone) {
                    if (p != null && p.trim().length() > 0) {
                        db.sendSms(p, "[" + bn.getAnnounceName() + "] " + bn.getContent(), shortCode);
                    }
                }
                logger.info("Finish to send " + bn.getAnnounceId());
                bn.setResultCode("00");
                bn.setDescription("Finish to send");
                continue;
            } else if (bn.getSendType() == 4) {
//                Send to all student of school
                listPhone = db.getAllStudentBySchool(bn.getSchoolId());
                for (String p : listPhone) {
                    if (p != null && p.trim().length() > 0) {
                        db.sendSms(p, "[" + bn.getAnnounceName() + "] " + bn.getContent(), shortCode);
                    }
                }
                logger.info("Finish to send " + bn.getAnnounceId());
                bn.setResultCode("00");
                bn.setDescription("Finish to send");
                continue;
            } else if (bn.getSendType() == 5) {
//                Send to all parent of school
                listPhone = db.getAllParentBySchool(bn.getSchoolId());
                for (String p : listPhone) {
                    if (p != null && p.trim().length() > 0) {
                        db.sendSms(p, "[" + bn.getAnnounceName() + "] " + bn.getContent(), shortCode);
                    }
                }
                logger.info("Finish to send " + bn.getAnnounceId());
                bn.setResultCode("00");
                bn.setDescription("Finish to send");
                continue;
            } else if (bn.getSendType() == 6) {
//                Send to all teacher of school
                listPhone = db.getAllTeacherBySchool(bn.getSchoolId());
                for (String p : listPhone) {
                    if (p != null && p.trim().length() > 0) {
                        db.sendSms(p, "[" + bn.getAnnounceName() + "] " + bn.getContent(), shortCode);
                    }
                }
                logger.info("Finish to send " + bn.getAnnounceId());
                bn.setResultCode("00");
                bn.setDescription("Finish to send");
                continue;
            } else if (bn.getSendType() == 7) {
//                Send to all student of Block
                listPhone = db.getAllStudentByGroup(bn.getSchoolId(), bn.getReceiverId());
                for (String p : listPhone) {
                    if (p != null && p.trim().length() > 0) {
                        db.sendSms(p, "[" + bn.getAnnounceName() + "] " + bn.getContent(), shortCode);
                    }
                }
                logger.info("Finish to send " + bn.getAnnounceId());
                bn.setResultCode("00");
                bn.setDescription("Finish to send");
                continue;
            } else if (bn.getSendType() == 8) {
//                Send to all parent of block
                listPhone = db.getAllParentByGroup(bn.getSchoolId(), bn.getReceiverId());
                for (String p : listPhone) {
                    if (p != null && p.trim().length() > 0) {
                        db.sendSms(p, "[" + bn.getAnnounceName() + "] " + bn.getContent(), shortCode);
                    }
                }
                logger.info("Finish to send " + bn.getAnnounceId());
                bn.setResultCode("00");
                bn.setDescription("Finish to send");
                continue;
            } else {
//                Invalid send type
                logger.info("Invalid send type " + bn.getAnnounceId());
                bn.setResultCode("E01");
                bn.setDescription("Invalid send type");
                continue;
            }
        }
        return listRecord;
    }

    @Override
    public void printListRecord(List<Record> listRecord) throws Exception {
        StringBuilder br = new StringBuilder();
        br.setLength(0);
        br.append("\r\n").
                append("|\tANNOUNCE_ID|").
                append("|\tANNOUNCE_NAME\t|").
                append("|\tRECEIVER_ID\t|").
                append("|\tANNOUNCE_TYPE\t|").
                append("|\tSEND_TYPE\t|").
                append("|\tSCHOOL_ID\t|").
                append("|\tSENDER_ID\t|");
        for (Record record : listRecord) {
            SmasAnnounce bn = (SmasAnnounce) record;
            br.append("\r\n").
                    append("|\t").
                    append(bn.getAnnounceId()).
                    append("|\t").
                    append(bn.getAnnounceName()).
                    append("|\t").
                    append(bn.getReceiverId()).
                    append("|\t").
                    append(bn.getAnnounceType()).
                    append("|\t").
                    append(bn.getSendType()).
                    append("|\t").
                    append(bn.getSchoolId()).
                    append("|\t").
                    append(bn.getSenderId());
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
