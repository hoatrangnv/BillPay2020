/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.obj;

/**
 *
 * @author Huynq13
 */
public class PhonePromotionConfig {

	private long id;
	private String stockModelCode;
	private String tac;
	private String balanceType;
	private long balanceAmount;
	private int balanceValidDays;
	private String dataType;
	private long dataAmount;
	private int dataValidDays;
	private String smsType;
	private long smsAmount;
	private int smsValidDays;
	private String smsToCus;
	private long status;
	private long handSetType;
	private int remainMonths;

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String getStockModelCode() {
		return stockModelCode;
	}

	public void setStockModelCode(String stockModelCode) {
		this.stockModelCode = stockModelCode;
	}

	public String getTac() {
		return tac;
	}

	public void setTac(String tac) {
		this.tac = tac;
	}

	public String getBalanceType() {
		return balanceType;
	}

	public void setBalanceType(String balanceType) {
		this.balanceType = balanceType;
	}

	public long getBalanceAmount() {
		return balanceAmount;
	}

	public void setBalanceAmount(long balanceAmount) {
		this.balanceAmount = balanceAmount;
	}

	public int getBalanceValidDays() {
		return balanceValidDays;
	}

	public void setBalanceValidDays(int balanceValidDays) {
		this.balanceValidDays = balanceValidDays;
	}

	public String getDataType() {
		return dataType;
	}

	public void setDataType(String dataType) {
		this.dataType = dataType;
	}

	public long getDataAmount() {
		return dataAmount;
	}

	public void setDataAmount(long dataAmount) {
		this.dataAmount = dataAmount;
	}

	public int getDataValidDays() {
		return dataValidDays;
	}

	public void setDataValidDays(int dataValidDays) {
		this.dataValidDays = dataValidDays;
	}

	public String getSmsType() {
		return smsType;
	}

	public void setSmsType(String smsType) {
		this.smsType = smsType;
	}

	public long getSmsAmount() {
		return smsAmount;
	}

	public void setSmsAmount(long smsAmount) {
		this.smsAmount = smsAmount;
	}

	public int getSmsValidDays() {
		return smsValidDays;
	}

	public void setSmsValidDays(int smsValidDays) {
		this.smsValidDays = smsValidDays;
	}

	public String getSmsToCus() {
		return smsToCus;
	}

	public void setSmsToCus(String smsToCus) {
		this.smsToCus = smsToCus;
	}

	public long getStatus() {
		return status;
	}

	public void setStatus(long status) {
		this.status = status;
	}

	public long getHandSetType() {
		return handSetType;
	}

	public void setHandSetType(long handSetType) {
		this.handSetType = handSetType;
	}

	public int getRemainMonths() {
		return remainMonths;
	}

	public void setRemainMonths(int remainMonths) {
		this.remainMonths = remainMonths;
	}

}
