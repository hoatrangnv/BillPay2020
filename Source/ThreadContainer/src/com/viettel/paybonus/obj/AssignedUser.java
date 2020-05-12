/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.obj;

/**
 *
 * @author dev_linh
 */
public class AssignedUser {

    private String assingedUser;
    private Long count;

    public AssignedUser(String assingedUser, Long count) {
        this.assingedUser = assingedUser;
        this.count = count;
    }

    public String getAssingedUser() {
        return assingedUser;
    }

    public void setAssingedUser(String assingedUser) {
        this.assingedUser = assingedUser;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }
}
