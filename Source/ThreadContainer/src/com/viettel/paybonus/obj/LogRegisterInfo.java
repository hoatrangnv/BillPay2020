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
public class LogRegisterInfo implements Record {

    public static String STAFF_CODE = "STAFF_CODE";
    public static String ISDN = "ISDN";
    public static String CREATE_TIME = "CREATE_TIME";
    public static String PROCESS_TIME = "PROCESS_TIME";
    public static String LOG_REGISTER_INFO_ID = "LOG_REGISTER_INFO_ID";
    public static String RESULT_CODE = "RESULT_CODE";
    public static String DESCRIPTION = "DESCRIPTION";
    private String staffCode;
    private String isdn;
    private Date createTime;
    private Date processTime;
    private Long logRegisterInfoId;
    private String threadName;
    private String nodeName;
    private String clusterName;
    private String idBatch;
    private String message;
    private Long id;
    private String resultCode;
    private String description;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getIdBatch() {
        return idBatch;
    }

    public void setIdBatch(String idBatch) {
        this.idBatch = idBatch;
    }

    public String getIsdn() {
        return isdn;
    }

    public void setIsdn(String isdn) {
        this.isdn = isdn;
    }

    public String getStaffCode() {
        return staffCode;
    }

    public void setStaffCode(String staffCode) {
        this.staffCode = staffCode;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public LogRegisterInfo() {
    }

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

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getProcessTime() {
        return processTime;
    }

    public void setProcessTime(Date processTime) {
        this.processTime = processTime;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Long getLogRegisterInfoId() {
        return logRegisterInfoId;
    }

    public void setLogRegisterInfoId(Long logRegisterInfoId) {
        this.logRegisterInfoId = logRegisterInfoId;
    }
}
