/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbSmasAbsentProcessor;
import com.viettel.paybonus.obj.SmasAbsent;
import java.util.List;
import org.apache.log4j.Logger;
import com.viettel.threadfw.process.ProcessRecordAbstract;
import java.util.ResourceBundle;

/**
 *
 * @author HuyNQ1
 * @version 1.0
 * @since 24-03-2016
 */
public class SmasProcessAbsent extends ProcessRecordAbstract {

    DbSmasAbsentProcessor db;
    String msg;

    public SmasProcessAbsent() {
        super();
        logger = Logger.getLogger(SmasProcessAbsent.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        db = new DbSmasAbsentProcessor();
    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        return listRecord;
    }

    @Override
    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
        String phoneParent;
        String nameTeacher;
        String nameStudent;
        for (Record record : listRecord) {
            SmasAbsent bn = (SmasAbsent) record;
            phoneParent = "";
            nameTeacher = "";
            nameStudent = "";
            phoneParent = db.getParentPhoneByStudentId(bn.getStudentId());
            nameTeacher = db.getTeacherNameById(bn.getTeacherId());
            nameStudent = db.getStudentNameById(bn.getStudentId());
            msg = ResourceBundle.getBundle("configPayBonus").getString("msgSmasAbsent");
            msg = msg.replace("%NAME%", nameStudent.toUpperCase());
            msg = msg.replace("%TEACHER%", nameTeacher.toUpperCase());
            db.sendSms(phoneParent, msg, "86904");
            logger.info("Finish notify absent id " + bn.getId());
            bn.setResultCode("0");
            bn.setDescription("Finish notify absent");
            continue;
        }
        return listRecord;
    }

    @Override
    public void printListRecord(List<Record> listRecord) throws Exception {
        StringBuilder br = new StringBuilder();
        br.setLength(0);
        br.append("\r\n").
                append("|\tABSENT_ID|").
                append("|\tSTUDENT_ID\t|").
                append("|\tTEACHER_ID\t|").
                append("|\tSUBJECT_D_TYPE\t|").
                append("|\tCLASS_ID\t|");
        for (Record record : listRecord) {
            SmasAbsent bn = (SmasAbsent) record;
            br.append("\r\n").
                    append("|\t").
                    append(bn.getId()).
                    append("|\t").
                    append(bn.getStudentId()).
                    append("|\t").
                    append(bn.getTeacherId()).
                    append("|\t").
                    append(bn.getSubjectId()).
                    append("|\t").
                    append(bn.getClassId());
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
