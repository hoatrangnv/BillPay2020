/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.obj;

import com.viettel.cluster.agent.integration.Record;
import java.sql.Date;

/**
 *
 * @author Huynq13
 */
public class KitBatchSub implements Record {

	private Long id;
	private String isdn;
	private int status;
	private Date createTime;
	private Long actionAuditId;
	private Long duration;
	private String resultCode;
	private String description;
	private Date dateProcess;
	private String threadName;
	private String nodeName;
	private String clusterName;
	private String idBatch;
	private Long countProcess;
	private String receiveDate;

	private String serial;
	private String productCode;
	private Long moneyProduct;
	private int charity;

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getIsdn() {
		return isdn;
	}

	public void setIsdn(String isdn) {
		this.isdn = isdn;
	}

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	public Date getCreateTime() {
		return createTime;
	}

	public void setCreateTime(Date createTime) {
		this.createTime = createTime;
	}

	public Long getActionAuditId() {
		return actionAuditId;
	}

	public void setActionAuditId(Long actionAuditId) {
		this.actionAuditId = actionAuditId;
	}

	public Long getDuration() {
		return duration;
	}

	public void setDuration(Long duration) {
		this.duration = duration;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public Date getDateProcess() {
		return dateProcess;
	}

	public void setDateProcess(Date dateProcess) {
		this.dateProcess = dateProcess;
	}

	public String getIdBatch() {
		return idBatch;
	}

	public void setIdBatch(String idBatch) {
		this.idBatch = idBatch;
	}

	public Long getCountProcess() {
		return countProcess;
	}

	public void setCountProcess(Long countProcess) {
		this.countProcess = countProcess;
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

	public String getSerial() {
		return serial;
	}

	public void setSerial(String serial) {
		this.serial = serial;
	}

	public String getProductCode() {
		return productCode;
	}

	public void setProductCode(String productCode) {
		this.productCode = productCode;
	}

	public Long getMoneyProduct() {
		return moneyProduct;
	}

	public void setMoneyProduct(Long moneyProduct) {
		this.moneyProduct = moneyProduct;
	}

	public int getCharity() {
		return charity;
	}

	public void setCharity(int charity) {
		this.charity = charity;
	}

	public String getReceiveDate() {
		return receiveDate;
	}

	public void setReceiveDate(String receiveDate) {
		this.receiveDate = receiveDate;
	}

}
