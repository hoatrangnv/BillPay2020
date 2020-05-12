/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.obj;

/**
 *
 * @author mov_itbl_dinhdc
 */
public class ResponseWallet {
    
    private String ResponseCode;
    private String ResponseMessage;
    private String RequestId;
    private String CreatedDate;

    public String getRequestId() {
        return RequestId;
    }

    public void setRequestId(String RequestId) {
        this.RequestId = RequestId;
    }

    public String getCreatedDate() {
        return CreatedDate;
    }

    public void setCreatedDate(String CreatedDate) {
        this.CreatedDate = CreatedDate;
    }

    public ResponseWallet() {
    }

    public String getResponseCode() {
        return ResponseCode;
    }

    public void setResponseCode(String ResponseCode) {
        this.ResponseCode = ResponseCode;
    }

    public String getResponseMessage() {
        return ResponseMessage;
    }

    public void setResponseMessage(String ResponseMessage) {
        this.ResponseMessage = ResponseMessage;
    }
}
