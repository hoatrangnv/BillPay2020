/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.vas.wsfw.object;

/**
 *
 * @author dev_bacnx
 */
public class PMSagaRequest {

	private Long agreementId;
	private Long customerId;
	private Double paymentAmount;
	private String paymentTypeCode;
	private PMSagaCreateUser createUser;
	private Integer billCycleFrom;
	private String receiptDate;
	private String isdnCharge;
	private String isdnForRemain;
	private String correlatorId;
	private Integer openType;
	private String ip;
	private Integer isOpen;
	private String isdnEpay;
	private Integer payMethod;
	private String otpCode;

	public Long getAgreementId() {
		return agreementId;
	}

	public void setAgreementId(Long agreementId) {
		this.agreementId = agreementId;
	}

	public Long getCustomerId() {
		return customerId;
	}

	public void setCustomerId(Long customerId) {
		this.customerId = customerId;
	}

	public Double getPaymentAmount() {
		return paymentAmount;
	}

	public void setPaymentAmount(Double paymentAmount) {
		this.paymentAmount = paymentAmount;
	}

	public String getPaymentTypeCode() {
		return paymentTypeCode;
	}

	public void setPaymentTypeCode(String paymentTypeCode) {
		this.paymentTypeCode = paymentTypeCode;
	}

	public PMSagaCreateUser getCreateUser() {
		return createUser;
	}

	public void setCreateUser(PMSagaCreateUser createUser) {
		this.createUser = createUser;
	}

	public Integer getBillCycleFrom() {
		return billCycleFrom;
	}

	public void setBillCycleFrom(Integer billCycleFrom) {
		this.billCycleFrom = billCycleFrom;
	}

	public String getReceiptDate() {
		return receiptDate;
	}

	public void setReceiptDate(String receiptDate) {
		this.receiptDate = receiptDate;
	}

	public String getIsdnCharge() {
		return isdnCharge;
	}

	public void setIsdnCharge(String isdnCharge) {
		this.isdnCharge = isdnCharge;
	}

	public String getIsdnForRemain() {
		return isdnForRemain;
	}

	public void setIsdnForRemain(String isdnForRemain) {
		this.isdnForRemain = isdnForRemain;
	}

	public String getCorrelatorId() {
		return correlatorId;
	}

	public void setCorrelatorId(String correlatorId) {
		this.correlatorId = correlatorId;
	}

	public Integer getOpenType() {
		return openType;
	}

	public void setOpenType(Integer openType) {
		this.openType = openType;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public Integer getIsOpen() {
		return isOpen;
	}

	public void setIsOpen(Integer isOpen) {
		this.isOpen = isOpen;
	}

	public String getIsdnEpay() {
		return isdnEpay;
	}

	public void setIsdnEpay(String isdnEpay) {
		this.isdnEpay = isdnEpay;
	}

	public Integer getPayMethod() {
		return payMethod;
	}

	public void setPayMethod(Integer payMethod) {
		this.payMethod = payMethod;
	}

	public String getOtpCode() {
		return otpCode;
	}

	public void setOtpCode(String otpCode) {
		this.otpCode = otpCode;
	}

}
