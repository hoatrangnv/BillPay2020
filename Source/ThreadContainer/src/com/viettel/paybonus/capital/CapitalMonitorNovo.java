/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.capital;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbTrparu300;
import com.viettel.paybonus.obj.Trparu300;
import com.viettel.paybonus.service.Exchange;
import com.viettel.threadfw.manager.AppManager;
import java.util.List;
import org.apache.log4j.Logger;
import com.viettel.threadfw.process.ProcessRecordAbstract;
import com.viettel.vas.util.ExchangeClientChannel;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.ResourceBundle;

/**
 *
 * @author TANNH
 * @version 1.0
 * @since 02-10-2018
 */
public class CapitalMonitorNovo extends ProcessRecordAbstract {

    Exchange pro;
    DbTrparu300 db;
    private Long sleepTime = 5 * 60 * 1000L;
    String smsBounesNewPhone;
    Calendar cal = Calendar.getInstance();

    public CapitalMonitorNovo() {
        super();
        logger = Logger.getLogger(Trparu300.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        db = new DbTrparu300();
        smsBounesNewPhone = ResourceBundle.getBundle("configPayBonus").getString("smsBounesNewPhone");
        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        for (Record record : listRecord) {
            Trparu300 moRecord = (Trparu300) record;
            moRecord.setNodeName(holder.getNodeName());
            moRecord.setClusterName(holder.getClusterName());
        }
        return listRecord;
    }

    @Override
    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
        List<Record> listResult = new ArrayList<Record>();
        String mscNumCus;
        String cellIdCus;
        String cellCodeCus;
        for (Record record : listRecord) {
            mscNumCus = "";
            cellIdCus = "";
            cellCodeCus = "";
            Trparu300 bn = (Trparu300) record;
            listResult.add(bn);
            try {
//          B1: xoa nhung ban ghi > 90 ngay
                logger.info(bn.getIsdn());
                db.deleteTrparu300(bn);
                db.deleteTrparu300His(bn);

//          B2: lay ra mscNumCus
                mscNumCus = pro.getMSCInfor(bn.getIsdn(), "");
                if (mscNumCus.trim().length() <= 0) {
                    if (db.checkUssd(bn.getIsdn())) {
                        db.inserTrparpu300His(bn.getIsdn(), bn.getCreateTime());
                        db.deleteTrparu300ByIsdn(bn.getIsdn());
                    }
                    continue;
                }
//          B3:   Kiem tra thue bao dang thuoc BTS nao 
                cellIdCus = pro.getCellIdRsString(bn.getIsdn(), mscNumCus, "");
                if (cellIdCus.trim().length() <= 0) {
                    if (db.checkUssd(bn.getIsdn())) {
                        db.inserTrparpu300His(bn.getIsdn(), bn.getCreateTime());
                        db.deleteTrparu300ByIsdn(bn.getIsdn());
                    }
                    continue;
                }
                String[] arrCellId = cellIdCus.split("\\|");
                if ((arrCellId != null) && (arrCellId.length == 2)) {
                    cellCodeCus = db.getCell("", arrCellId[0], arrCellId[1]);
                    if (cellCodeCus.trim().length() <= 0) {
                        if (db.checkUssd(bn.getIsdn())) {
                            db.inserTrparpu300His(bn.getIsdn(), bn.getCreateTime());
                            db.deleteTrparu300ByIsdn(bn.getIsdn());
                        }
                        continue;
                    }
                } else {
                    if (db.checkUssd(bn.getIsdn())) {
                        db.inserTrparpu300His(bn.getIsdn(), bn.getCreateTime());
                        db.deleteTrparu300ByIsdn(bn.getIsdn());
                    }
                    continue;
                }

                if (!cellCodeCus.trim().toUpperCase().contains("MAC") && !cellCodeCus.trim().toUpperCase().contains("MAT")
                        && !cellCodeCus.trim().toUpperCase().contains("INH") && !cellCodeCus.trim().toUpperCase().contains("GAZ")) {
                    logger.warn("Customer moving not in BTS MAC, MAT, INH, GAZ by isdn :  " + bn.getIsdn());
                    if (db.checkUssd(bn.getIsdn())) {
                        db.inserTrparpu300His(bn.getIsdn(), bn.getCreateTime());
                        db.deleteTrparu300ByIsdn(bn.getIsdn());
                    }
                    continue;
                } else {
                    logger.warn("Cus moving in BTS MAC, MAT, INH, GAZ by isdn :  " + bn.getIsdn());
                    if (!db.checkUssd(bn.getIsdn())) {
                        db.inserTrparpu300(bn.getIsdn());
                        db.deleteTrparu300HisByIsdn(bn.getIsdn());
                    }
                }
            } catch (Exception e) {
                db.inserTrparpu300His(bn.getIsdn(), bn.getCreateTime());
                db.deleteTrparu300ByIsdn(bn.getIsdn());
                logger.error("Someting Error CapitalDeleteTrparu300 " + e + " so system delete Info " + bn);
            }

        }
        listRecord.clear();
        Thread.sleep(sleepTime);
        return listResult;
    }

    @Override
    public void printListRecord(List<Record> listRecord) throws Exception {
        StringBuilder br = new StringBuilder();
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        br.setLength(0);
        br.append("\r\n").
                append("|\tisdn\t|");
        for (Record record : listRecord) {
            Trparu300 bn = (Trparu300) record;
            br.append("\r\n").
                    append("|\t").
                    append(bn.getIsdn());
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
