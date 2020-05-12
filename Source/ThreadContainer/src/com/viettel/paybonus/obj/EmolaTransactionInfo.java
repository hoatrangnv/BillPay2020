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
public class EmolaTransactionInfo implements Record {

    private String cusName;
    private String mobile;
    private Date createTime;
    private Date activedTime;
    private Date processTime;
    private Date transactionTime;
    private long cusId;
    private long requestId;
    private double amount;
    private int serviceId;
    private String transCode;
    private String btsReg;
    private String threadName;
    private String nodeName;
    private String clusterName;
    private String idBatch;
    private String message;
    private Long id;
    private String resultCode;
    private String description;
    private String agentWallet;
    private String agentName;
    private String agentChannelCode;
    private String agentCode;

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

    public EmolaTransactionInfo() {
    }

    public String getAgentWallet() {
        return agentWallet;
    }

    public void setAgentWallet(String agentWallet) {
        this.agentWallet = agentWallet;
    }

    public String getAgentName() {
        return agentName;
    }

    public void setAgentName(String agentName) {
        this.agentName = agentName;
    }

    public String getAgentChannelCode() {
        return agentChannelCode;
    }

    public void setAgentChannelCode(String agentChannelCode) {
        this.agentChannelCode = agentChannelCode;
    }

    public Date getActivedTime() {
        return activedTime;
    }

    public void setActivedTime(Date activedTime) {
        this.activedTime = activedTime;
    }

    public Date getTransactionTime() {
        return transactionTime;
    }

    public void setTransactionTime(Date transactionTime) {
        this.transactionTime = transactionTime;
    }

    public String getAgentCode() {
        return agentCode;
    }

    public void setAgentCode(String agentCode) {
        this.agentCode = agentCode;
    }

    public long getRequestId() {
        return requestId;
    }

    public void setRequestId(long requestId) {
        this.requestId = requestId;
    }

    public String getTransCode() {
        return transCode;
    }

    public void setTransCode(String transCode) {
        this.transCode = transCode;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
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

    public String getCusName() {
        return cusName;
    }

    public void setCusName(String cusName) {
        this.cusName = cusName;
    }

    public String getMobile() {
        return mobile;
    }

    public void setMobile(String mobile) {
        this.mobile = mobile;
    }

    public long getCusId() {
        return cusId;
    }

    public void setCusId(long cusId) {
        this.cusId = cusId;
    }

    public int getServiceId() {
        return serviceId;
    }

    public void setServiceId(int serviceId) {
        this.serviceId = serviceId;
    }

    public String getBtsReg() {
        return btsReg;
    }

    public void setBtsReg(String btsReg) {
        this.btsReg = btsReg;
    }

}
