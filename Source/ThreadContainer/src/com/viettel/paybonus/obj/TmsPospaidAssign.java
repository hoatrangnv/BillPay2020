/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.obj;

import com.viettel.cluster.agent.integration.Record;

/**
 *
 * @author Huynq13
 */
public class TmsPospaidAssign implements Record {

	private Long actionAuditId;
	private String resultCode;
	private String threadName;
	private String nodeName;
	private String clusterName;
	private String idBatch;
	private Long id;
	private Long countProcess;
	private Long duration;
	private String description;
	private Double valueAssign;
	private String targetCode;
	private String sqlConmand;
	private String object;
	private String targetMonth;

	public String getObject() {
		return object;
	}

	public void setObject(String object) {
		this.object = object;
	}

	public String getTargetMonth() {
		return targetMonth;
	}

	public void setTargetMonth(String targetMonth) {
		this.targetMonth = targetMonth;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public Double getValueAssign() {
		return valueAssign;
	}

	public void setValueAssign(Double valueAssign) {
		this.valueAssign = valueAssign;
	}

	public String getTargetCode() {
		return targetCode;
	}

	public void setTargetCode(String targetCode) {
		this.targetCode = targetCode;
	}

	public String getSqlConmand() {
		return sqlConmand;
	}

	public void setSqlConmand(String sqlConmand) {
		this.sqlConmand = sqlConmand;
	}

	public Long getActionAuditId() {
		return actionAuditId;
	}

	public void setActionAuditId(Long actionAuditId) {
		this.actionAuditId = actionAuditId;
	}

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

	public TmsPospaidAssign() {
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

}
