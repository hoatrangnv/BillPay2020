package com.viettel.paybonus.obj;

/**
 * SubRelProduct entity.
 *
 * @author MyEclipse Persistence Tools
 */
public class SpecialProductMB implements java.io.Serializable {

	// Fields
	private Long id;
	private String prductCode;
	private long prepaidMonths;
	private long payAdvence;
	private long limit;
	private String saleServiceCode;
	private long agentMoney;
	private String specialUser;
	private long productId;
	private long status;
	private double monthlyFee;
	private double bonusAgent;
	private double bonusOther;

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getPrductCode() {
		return prductCode;
	}

	public void setPrductCode(String prductCode) {
		this.prductCode = prductCode;
	}

	public long getPrepaidMonths() {
		return prepaidMonths;
	}

	public void setPrepaidMonths(long prepaidMonths) {
		this.prepaidMonths = prepaidMonths;
	}

	public long getPayAdvence() {
		return payAdvence;
	}

	public void setPayAdvence(long payAdvence) {
		this.payAdvence = payAdvence;
	}

	public long getLimit() {
		return limit;
	}

	public void setLimit(long limit) {
		this.limit = limit;
	}

	public String getSaleServiceCode() {
		return saleServiceCode;
	}

	public void setSaleServiceCode(String saleServiceCode) {
		this.saleServiceCode = saleServiceCode;
	}

	public long getAgentMoney() {
		return agentMoney;
	}

	public void setAgentMoney(long agentMoney) {
		this.agentMoney = agentMoney;
	}

	public String getSpecialUser() {
		return specialUser;
	}

	public void setSpecialUser(String specialUser) {
		this.specialUser = specialUser;
	}

	public long getProductId() {
		return productId;
	}

	public void setProductId(long productId) {
		this.productId = productId;
	}

	public long getStatus() {
		return status;
	}

	public void setStatus(long status) {
		this.status = status;
	}

	public double getMonthlyFee() {
		return monthlyFee;
	}

	public void setMonthlyFee(double monthlyFee) {
		this.monthlyFee = monthlyFee;
	}

	public double getBonusAgent() {
		return bonusAgent;
	}

	public void setBonusAgent(double bonusAgent) {
		this.bonusAgent = bonusAgent;
	}

	public double getBonusOther() {
		return bonusOther;
	}

	public void setBonusOther(double bonusOther) {
		this.bonusOther = bonusOther;
	}

	@Override
	public String toString() {
		return "SpecialProductMB{" + "id=" + id + ", prductCode=" + prductCode + ", prepaidMonths=" + prepaidMonths + ", payAdvence=" + payAdvence + ", limit=" + limit + ", saleServiceCode=" + saleServiceCode + ", agentMoney=" + agentMoney + ", specialUser=" + specialUser + ", productId=" + productId + ", status=" + status + ", monthlyFee=" + monthlyFee + ", bonusAgent=" + bonusAgent + ", bonusOther=" + bonusOther + '}';
	}

}
