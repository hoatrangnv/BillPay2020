/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.vas.wsfw.object;

/**
 *
 * @author tungnt64
 */
public class ResponseSyncEmolaFloat {

	private String errorCode;
	private String description;
	private ListEmolaFloat listEmolaFloat;

	public String getErrorCode() {
		return errorCode;
	}

	public void setErrorCode(String errorCode) {
		this.errorCode = errorCode;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public ListEmolaFloat getListEmolaFloat() {
		return listEmolaFloat;
	}

	public void setListEmolaFloat(ListEmolaFloat listEmolaFloat) {
		this.listEmolaFloat = listEmolaFloat;
	}

}
