/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.elite.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbElite;
import com.viettel.paybonus.obj.KitVas;
import com.viettel.paybonus.service.Exchange;
import com.viettel.threadfw.manager.AppManager;
import java.util.List;
import org.apache.log4j.Logger;
import com.viettel.threadfw.process.ProcessRecordAbstract;
import com.viettel.vas.util.ExchangeClientChannel;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.ResourceBundle;

/**
 *
 * @author LamNT
 * @version 1.0
 * @since 05-01-2019
 */
public class MonitorElite extends ProcessRecordAbstract {

    Exchange pro;
    DbElite db;
    Calendar cal = Calendar.getInstance();
    Long dateRuning;
    Long sleepTime = 24 * 60 * 60 * 1000L;

    public MonitorElite() {
        super();
        logger = Logger.getLogger(KitVas.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        db = new DbElite();
        dateRuning = Long.parseLong(ResourceBundle.getBundle("configPayBonus").getString("MonitorEliteDateRuning"));
        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        return listRecord;
    }

    @Override
    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
        List<Record> listResult = new ArrayList<Record>();
        String monitorGrade;
        for (Record record : listRecord) {
            monitorGrade = "";
            KitVas kv = (KitVas) record;
            listResult.add(kv);
            cal.setTime(new Date());
            int dayOfMonth = cal.get(Calendar.DAY_OF_MONTH);
            if (dayOfMonth == dateRuning) {
                logger.info("======= Start day of Month =========");
//              B2: Với mỗi thuê bao kiểm tra trong vòng 1 tháng trở lại đây có gia hạn lần nào không    
                if (db.checkRenew(kv.getIsdn()) || db.checkByMore(kv.getIsdn())) {
//              B4: Thuê bao đã có gia hạn lại trong vòng 1 tháng trở lại đây do vậy thực hiện xóa đánh dấu đi và xóa dữ liệu bảng cm_pre.kit_vas_bonus. Lưu lại lịch sử và kết thúc.  
                    monitorGrade = pro.callOcsChangeCusInfoCommand(kv.getIsdn(), "4050000");
                    if ("0".equals(monitorGrade)) {
                        kv.setDescription("Remove successfully OCSHW_CHANGE_CUS_INFO for isdn=" + kv.getIsdn());
                        logger.info("Remove successfully OCSHW_CHANGE_CUS_INFO for isdn=" + kv.getIsdn());
                        kv.setResultCode("EME01");
                    } else {
                        kv.setDescription("Remove fail OCSHW_CHANGE_CUS_INFO for isdn=" + kv.getIsdn());
                        logger.error("Remove fail OCSHW_CHANGE_CUS_INFO for isdn=" + kv.getIsdn());
                        kv.setResultCode("EME02");
                    }
                    kv.setGradeOcs(4050000L);
                    db.deleteKitVasBonus(kv);
                } else {
//              B3: Thuê bao chưa gia hạn trong vòng 1 tháng trở lại đây do vậy thực hiện kiểm tra thuê bao đã được đánh dấu khuyến mại hay chưa       
                    if (db.checkKitVasBonus(kv.getIsdn())) {
//                  Nếu có thì chứng tỏ đã đánh dấu từ tháng trước đó rồi nên không cần đánh dấu lại, thực hiện ghi log và kết thúc. 
                        logger.info("Number: " + kv.getIsdn() + " Already in kit_vas_bonus");
                        kv.setDescription("Number: " + kv.getIsdn() + " Already in kit_vas_bonus");
                        kv.setResultCode("EME03");
                    } else {
//                  Nếu không có dữ liệu thì thực hiện đánh dấu cho thuê bao, đồng thời lưu lại kết quả vào bảng cm_pre.kit_vas_bonus 
                        monitorGrade = pro.callOcsChangeCusInfoCommand(kv.getIsdn(), "4050002");
                        if ("0".equals(monitorGrade)) {
                            kv.setDescription("Add successfully OCSHW_CHANGE_CUS_INFO for isdn=" + kv.getIsdn());
                            logger.info("Add successfully OCSHW_CHANGE_CUS_INFO for isdn=" + kv.getIsdn());
                            kv.setResultCode("EME04");
                        } else {
                            kv.setDescription("Add fail OCSHW_CHANGE_CUS_INFO for isdn=" + kv.getIsdn());
                            logger.error("Add fail OCSHW_CHANGE_CUS_INFO for isdn=" + kv.getIsdn());
                            kv.setResultCode("EME05");
                        }
                        kv.setGradeOcs(4050002L);
                        db.insertKitVasBonus(kv);
                    }
                }

            } else {
                Thread.sleep(sleepTime);
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
                append("|\tKIT_VAS_ID|").
                append("|\tISDN|").
                append("|\tSERIAL\t|").
                append("|\tCREATE_USER\t|").
                append("|\tPRODUCT_CODE\t|");
        for (Record record : listRecord) {
            KitVas bn = (KitVas) record;
            br.append("\r\n").
                    append("|\t").
                    append(bn.getKitVasId()).
                    append("||\t").
                    append(bn.getIsdn()).
                    append("||\t").
                    append(bn.getSerial()).
                    append("||\t").
                    append(bn.getCreateUser()).
                    append("||\t").
                    append(bn.getProductCode());
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
