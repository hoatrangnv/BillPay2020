/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.report;

import com.viettel.cluster.agent.integration.Record;
import java.util.Date;
import java.util.Map;

/**
 *
 * @author dev_bacnx
 */
public class ReportInput implements Record {

	private Long actionAuditId;
	private Long duration;
	private String resultCode;
	private Date dateProcess;
	private String threadName;
	private String nodeName;
	private String clusterName;
	private String idBatch;
	private Long countProcess;

	private Long id;
	private String branch;
	private int status;
	private int sendEmailStatus;
	private String reportType;

	public int getSendEmailStatus() {
		return sendEmailStatus;
	}

	public void setSendEmailStatus(int sendEmailStatus) {
		this.sendEmailStatus = sendEmailStatus;
	}

	public Long getActionAuditId() {
		return actionAuditId;
	}

	@Override
	public String getID() {
		return "" + this.id;
	}

	public void setActionAuditId(Long actionAuditId) {
		this.actionAuditId = actionAuditId;
	}

	public String getIdBatch() {
		return idBatch;
	}

	public void setIdBatch(String idBatch) {
		this.idBatch = idBatch;
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

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getBranch() {
		return branch;
	}

	public void setBranch(String branch) {
		this.branch = branch;
	}

	public String getReportType() {
		return reportType;
	}

	public void setReportType(String reportType) {
		this.reportType = reportType;
	}

}
