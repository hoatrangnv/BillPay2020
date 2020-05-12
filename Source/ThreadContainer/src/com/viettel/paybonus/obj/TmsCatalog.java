/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.obj;

public class TmsCatalog {

	private Long targetId;
	private String targetCode;
	private String targetGroup;
	private String targetType;
	private String unit;
	private String parentTargetCode;

	public Long getTargetId() {
		return targetId;
	}

	public void setTargetId(Long targetId) {
		this.targetId = targetId;
	}

	public String getTargetCode() {
		return targetCode;
	}

	public void setTargetCode(String targetCode) {
		this.targetCode = targetCode;
	}

	public String getTargetGroup() {
		return targetGroup;
	}

	public void setTargetGroup(String targetGroup) {
		this.targetGroup = targetGroup;
	}

	public String getTargetType() {
		return targetType;
	}

	public void setTargetType(String targetType) {
		this.targetType = targetType;
	}

	public String getUnit() {
		return unit;
	}

	public void setUnit(String unit) {
		this.unit = unit;
	}

	public String getParentTargetCode() {
		return parentTargetCode;
	}

	public void setParentTargetCode(String parentTargetCode) {
		this.parentTargetCode = parentTargetCode;
	}

	@Override
	public String toString() {
		return "TmsCatalog{" + "targetId=" + targetId + ", targetCode=" + targetCode + ", targetGroup=" + targetGroup + ", targetType=" + targetType + ", unit=" + unit + ", parentTargetCode=" + parentTargetCode + '}';
	}

}
