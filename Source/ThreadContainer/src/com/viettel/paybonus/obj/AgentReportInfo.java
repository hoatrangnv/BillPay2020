/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.obj;

/**
 *
 * @author dev_linh
 */
public class AgentReportInfo {

    private String province;
    private int agentNotConfirm;
    private int exportNotConfirm;
    private int rejectInDay;
    private int rejectInMonth;
    private int notActiveLessThan3h;
    private int notActiveLessThan5h;
    private int notActiveLessThan8h;
    private int notActiveOver8h;

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public int getAgentNotConfirm() {
        return agentNotConfirm;
    }

    public void setAgentNotConfirm(int agentNotConfirm) {
        this.agentNotConfirm = agentNotConfirm;
    }

    public int getExportNotConfirm() {
        return exportNotConfirm;
    }

    public void setExportNotConfirm(int exportNotConfirm) {
        this.exportNotConfirm = exportNotConfirm;
    }

    public int getRejectInDay() {
        return rejectInDay;
    }

    public void setRejectInDay(int rejectInDay) {
        this.rejectInDay = rejectInDay;
    }

    public int getRejectInMonth() {
        return rejectInMonth;
    }

    public void setRejectInMonth(int rejectInMonth) {
        this.rejectInMonth = rejectInMonth;
    }

    public int getNotActiveLessThan3h() {
        return notActiveLessThan3h;
    }

    public void setNotActiveLessThan3h(int notActiveLessThan3h) {
        this.notActiveLessThan3h = notActiveLessThan3h;
    }

    public int getNotActiveLessThan5h() {
        return notActiveLessThan5h;
    }

    public void setNotActiveLessThan5h(int notActiveLessThan5h) {
        this.notActiveLessThan5h = notActiveLessThan5h;
    }

    public int getNotActiveLessThan8h() {
        return notActiveLessThan8h;
    }

    public void setNotActiveLessThan8h(int notActiveLessThan8h) {
        this.notActiveLessThan8h = notActiveLessThan8h;
    }

    public int getNotActiveOver8h() {
        return notActiveOver8h;
    }

    public void setNotActiveOver8h(int notActiveOver8h) {
        this.notActiveOver8h = notActiveOver8h;
    }
}
