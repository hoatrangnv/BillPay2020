/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.obj;

/**
 *
 * @author itbl_linh
 */
public class Comission {

    private long bonusCenter;
    private long bonusCtv;
    private long bonusCtvCenter;
    private long bonusChannel;
    private long bonusChannelCtv;
    private long bonusChannelCenter;
    private String bonusAgentOwner;
    private String bonusAgentStaff;
    private long moneyBrDirector;

    public long getMoneyBrDirector() {
        return moneyBrDirector;
    }

    public void setMoneyBrDirector(long moneyBrDirector) {
        this.moneyBrDirector = moneyBrDirector;
    }

    public long getBonusCenter() {
        return bonusCenter;
    }

    public void setBonusCenter(long bonusCenter) {
        this.bonusCenter = bonusCenter;
    }

    public long getBonusCtv() {
        return bonusCtv;
    }

    public void setBonusCtv(long bonusCtv) {
        this.bonusCtv = bonusCtv;
    }

    public long getBonusChannel() {
        return bonusChannel;
    }

    public void setBonusChannel(long bonusChannel) {
        this.bonusChannel = bonusChannel;
    }

    public long getBonusCtvCenter() {
        return bonusCtvCenter;
    }

    public void setBonusCtvCenter(long bonusCtvCenter) {
        this.bonusCtvCenter = bonusCtvCenter;
    }

    public long getBonusChannelCtv() {
        return bonusChannelCtv;
    }

    public void setBonusChannelCtv(long bonusChannelCtv) {
        this.bonusChannelCtv = bonusChannelCtv;
    }

    public long getBonusChannelCenter() {
        return bonusChannelCenter;
    }

    public void setBonusChannelCenter(long bonusChannelCenter) {
        this.bonusChannelCenter = bonusChannelCenter;
    }

    public String getBonusAgentOwner() {
        return bonusAgentOwner;
    }

    public void setBonusAgentOwner(String bonusAgentOwner) {
        this.bonusAgentOwner = bonusAgentOwner;
    }

    public String getBonusAgentStaff() {
        return bonusAgentStaff;
    }

    public void setBonusAgentStaff(String bonusAgentStaff) {
        this.bonusAgentStaff = bonusAgentStaff;
    }
}
