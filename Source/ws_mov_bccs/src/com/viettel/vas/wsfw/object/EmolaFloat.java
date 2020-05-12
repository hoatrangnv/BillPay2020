/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.vas.wsfw.object;

import java.math.BigDecimal;

/**
 *
 * @author tungnt64
 */
public class EmolaFloat {

	private String transferCode;
	private BigDecimal amount;
	private String createdDate;
	private String createdUser;
	private int purpose;
	private String shopCodeLv1;
	private String shopCodeLv2;
	private String shopCodeLv3;
	private String description;

	public String getTransferCode() {
		return transferCode;
	}

	public void setTransferCode(String transferCode) {
		this.transferCode = transferCode;
	}

	public BigDecimal getAmount() {
		return amount;
	}

	public void setAmount(BigDecimal amount) {
		this.amount = amount;
	}

	public String getCreatedDate() {
		return createdDate;
	}

	public void setCreatedDate(String createdDate) {
		this.createdDate = createdDate;
	}

	public String getCreatedUser() {
		return createdUser;
	}

	public void setCreatedUser(String createdUser) {
		this.createdUser = createdUser;
	}

	public int getPurpose() {
		return purpose;
	}

	public void setPurpose(int purpose) {
		this.purpose = purpose;
	}

	public String getShopCodeLv1() {
		return shopCodeLv1;
	}

	public void setShopCodeLv1(String shopCodeLv1) {
		this.shopCodeLv1 = shopCodeLv1;
	}

	public String getShopCodeLv2() {
		return shopCodeLv2;
	}

	public void setShopCodeLv2(String shopCodeLv2) {
		this.shopCodeLv2 = shopCodeLv2;
	}

	public String getShopCodeLv3() {
		return shopCodeLv3;
	}

	public void setShopCodeLv3(String shopCodeLv3) {
		this.shopCodeLv3 = shopCodeLv3;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

}
