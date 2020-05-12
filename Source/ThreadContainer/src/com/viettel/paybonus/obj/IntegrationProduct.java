/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.obj;

/**
 *
 * @author USER
 */
public class IntegrationProduct {

	private String Id;
	private String productOrderKey;
	private String effectiveDate;
	private String expiredDate;
	private String status;
	private String curCycleStartTime;
	private String curCycleEndTime;
	private String billStatus;

	public final String ID = "ID";
	public final String PRODUCTORDERKEY = "PRODUCTORDERKEY";
	public final String EFFECTIVEDATE = "EFFECTIVEDATE";
	public final String EXPIREDDATE = "EXPIREDDATE";
	public final String STATUS = "STATUS";
	public final String CURCYCLESTARTTIME = "CURCYCLESTARTTIME";
	public final String CURCYCLEENDTIME = "CURCYCLEENDTIME";
	public final String BILLSTATUS = "BILLSTATUS";

	public String getId() {
		return Id;
	}

	public void setId(String Id) {
		this.Id = Id;
	}

	public String getProductOrderKey() {
		return productOrderKey;
	}

	public void setProductOrderKey(String productOrderKey) {
		this.productOrderKey = productOrderKey;
	}

	public String getEffectiveDate() {
		return effectiveDate;
	}

	public void setEffectiveDate(String effectiveDate) {
		this.effectiveDate = effectiveDate;
	}

	public String getExpiredDate() {
		return expiredDate;
	}

	public void setExpiredDate(String expiredDate) {
		this.expiredDate = expiredDate;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getCurCycleStartTime() {
		return curCycleStartTime;
	}

	public void setCurCycleStartTime(String curCycleStartTime) {
		this.curCycleStartTime = curCycleStartTime;
	}

	public String getCurCycleEndTime() {
		return curCycleEndTime;
	}

	public void setCurCycleEndTime(String curCycleEndTime) {
		this.curCycleEndTime = curCycleEndTime;
	}

	public String getBillStatus() {
		return billStatus;
	}

	public void setBillStatus(String billStatus) {
		this.billStatus = billStatus;
	}

	@Override
	public String toString() {
		return "IntegrationProduct{" + "Id=" + Id + ", productOrderKey=" + productOrderKey + ", effectiveDate=" + effectiveDate + ", expiredDate=" + expiredDate + ", status=" + status + ", curCycleStartTime=" + curCycleStartTime + ", curCycleEndTime=" + curCycleEndTime + ", billStatus=" + billStatus + '}';
	}
}
