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
public class ComplainInfo implements Record {

    public static String RESCUE_MODEL_ISDN = "RESCUE_MODEL_ISDN";
    public static String RESCUE_CC_USER = "RESCUE_CC_USER";
    public static String RESCUE_TIME = "RESCUE_TIME";
    public static String RESCUE_CLOSE_TIME = "RESCUE_CLOSE_TIME";
    public static String RESULT_CODE = "RESULT_CODE";
    public static String DESCRIPTION = "DESCRIPTION";
    private String rescueModelIsdn;
    private String rescueCcUser;
    private Date rescueTime;
    private Date rescueCloseTime;
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

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public ComplainInfo() {
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

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getRescueModelIsdn() {
        return rescueModelIsdn;
    }

    public void setRescueModelIsdn(String rescueModelIsdn) {
        this.rescueModelIsdn = rescueModelIsdn;
    }

    public String getRescueCcUser() {
        return rescueCcUser;
    }

    public void setRescueCcUser(String rescueCcUser) {
        this.rescueCcUser = rescueCcUser;
    }

    public Date getRescueTime() {
        return rescueTime;
    }

    public void setRescueTime(Date rescueTime) {
        this.rescueTime = rescueTime;
    }

    public Date getRescueCloseTime() {
        return rescueCloseTime;
    }

    public void setRescueCloseTime(Date rescueCloseTime) {
        this.rescueCloseTime = rescueCloseTime;
    }
}
