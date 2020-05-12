/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.obj;

import com.viettel.cluster.agent.integration.Record;
import java.util.Date;

/**
 *
 * @author Huynq13
 */
public class PhoneNewPromotion implements Record {

    private String msisdn;
    private String imsi;
    private String imei;
    private String tac;
    private Date datetime;
    private Long hlr;
    private Long ard;
    private Long nam;
    private String errCode;
    private String description = "";
    private int remainMonths;
    private Date curentCycle;
    private Date nexcycle;
    private int status;
    private int inputType;//1 handset, 2 dcom
    private String threadName;
    private String nodeName;
    private String clusterName;
    private String idBatch;
    private Long id;
    private String resultCode;

    @Override
    public String getID() {
        return "" + this.id;
    }

    @Override
    public String getBatchId() {
        return this.idBatch;
    }

    @Override
    public void setBatchId(String batchId) {
        this.idBatch = batchId;
    }

    @Override
    public String getClusterName() {
        return this.clusterName;
    }

    @Override
    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    @Override
    public String getNodeName() {
        return this.nodeName;
    }

    @Override
    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    @Override
    public String getTheadName() {
        return this.threadName;
    }

    @Override
    public void setThreadName(String threadName) {
        this.threadName = threadName;
    }

    @Override
    public String getResultCode() {
        return this.resultCode;
    }

    @Override
    public void setResultCode(String resultCode) {
        this.resultCode = resultCode;
    }

    public String getMsisdn() {
        return msisdn;
    }

    public void setMsisdn(String msisdn) {
        this.msisdn = msisdn;
    }

    public String getImsi() {
        return imsi;
    }

    public void setImsi(String imsi) {
        this.imsi = imsi;
    }

    public String getImei() {
        return imei;
    }

    public void setImei(String imei) {
        this.imei = imei;
    }

    public String getTac() {
        return tac;
    }

    public void setTac(String tac) {
        this.tac = tac;
    }

    public Date getDatetime() {
        return datetime;
    }

    public void setDatetime(Date datetime) {
        this.datetime = datetime;
    }

    public Long getHlr() {
        return hlr;
    }

    public void setHlr(Long hlr) {
        this.hlr = hlr;
    }

    public Long getArd() {
        return ard;
    }

    public void setArd(Long ard) {
        this.ard = ard;
    }

    public Long getNam() {
        return nam;
    }

    public void setNam(Long nam) {
        this.nam = nam;
    }

    public String getIdBatch() {
        return idBatch;
    }

    public void setIdBatch(String idBatch) {
        this.idBatch = idBatch;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getErrCode() {
        return errCode;
    }

    public void setErrCode(String errCode) {
        this.errCode = errCode;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public int getRemainMonths() {
        return remainMonths;
    }

    public void setRemainMonths(int remainMonths) {
        this.remainMonths = remainMonths;
    }

    public Date getCurentCycle() {
        return curentCycle;
    }

    public void setCurentCycle(Date curentCycle) {
        this.curentCycle = curentCycle;
    }

    public Date getNexcycle() {
        return nexcycle;
    }

    public void setNexcycle(Date nexcycle) {
        this.nexcycle = nexcycle;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public int getInputType() {
        return inputType;
    }

    public void setInputType(int inputType) {
        this.inputType = inputType;
    }
}
