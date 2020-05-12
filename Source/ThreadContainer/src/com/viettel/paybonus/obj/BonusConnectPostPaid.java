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
public class BonusConnectPostPaid implements Record {

	private Long subId;
	private String isdn;
	private String serial;
	private int status;
	private String staffCode;
	private Long actionAuditId;
	private Long duration;
	private String resultCode;
	private String description;
	private Long bonusValues;
	private String shopCode;
	private String productCode;
	private String imsi;

	private Date dateProcess;
	private String threadName;
	private String nodeName;
	private String clusterName;
	private String idBatch;
	private Long countProcess;

	public Long getActionAuditId() {
		return actionAuditId;
	}

	public void setActionAuditId(Long actionAuditId) {
		this.actionAuditId = actionAuditId;
	}

	public Long getSubId() {
		return subId;
	}

	public void setSubId(Long subId) {
		this.subId = subId;
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

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getShopCode() {
		return shopCode;
	}

	public void setShopCode(String shopCode) {
		this.shopCode = shopCode;
	}

	public String getProductCode() {
		return productCode;
	}

	public void setProductCode(String productCode) {
		this.productCode = productCode;
	}

	public BonusConnectPostPaid() {
	}

	@Override
	public String getID() {
		return "" + this.subId;
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

	public Long getCountProcess() {
		return countProcess;
	}

	public void setCountProcess(Long countProcess) {
		this.countProcess = countProcess;
	}

	public Long getDuration() {
		return duration;
	}

	public void setDuration(Long duration) {
		this.duration = duration;
	}

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	public Date getDateProcess() {
		return dateProcess;
	}

	public void setDateProcess(Date dateProcess) {
		this.dateProcess = dateProcess;
	}

	public String getStaffCode() {
		return staffCode;
	}

	public void setStaffCode(String staffCode) {
		this.staffCode = staffCode;
	}

	public Long getBonusValues() {
		return bonusValues;
	}

	public void setBonusValues(Long bonusValues) {
		this.bonusValues = bonusValues;
	}

	public String getSerial() {
		return serial;
	}

	public void setSerial(String serial) {
		this.serial = serial;
	}

	public String getImsi() {
		return imsi;
	}

	public void setImsi(String imsi) {
		this.imsi = imsi;
	}

}
