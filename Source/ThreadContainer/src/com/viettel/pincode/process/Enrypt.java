/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.pincode.process;

import com.viettel.security.PassTranformer;

/**
 *
 * @author dev_linh
 */
public class Enrypt {

    public static void main(String[] args) throws Exception {
//        String email = "hungnd@movitel.co.mz";
//        String[] arrEmail = email.split("\\@");
//        String password = "pincode@123";
//        String[] arrPass = password.split("\\@");
//        String emailEncrypt = PassTranformer.encrypt(arrEmail[0] + "#2019@" + arrEmail[1]);
//        String passwordEncrypt = PassTranformer.encrypt(arrPass[0] + "#2019@" + arrPass[1]);
//        System.out.println("After encrypt: " + emailEncrypt + ", pass: " + passwordEncrypt);
//        String emailDecrypt = PassTranformer.decrypt(emailEncrypt);
//        String[] arrEmailDecrypt = emailDecrypt.split("\\#");
//        String finalEmail = arrEmailDecrypt[0] + "@" + arrEmailDecrypt[1].split("\\@")[1];
//        String passDecrypt = PassTranformer.decrypt(passwordEncrypt);
//        String[] arrPassDecrypt = passDecrypt.split("\\#");
//        String finalPass = arrPassDecrypt[0] + "@" + arrPassDecrypt[1].split("\\@")[1];
//
//        System.out.println("Final email: " + finalEmail + ", final password: " + finalPass);
//        Using this.....
        String email = "tranthivui274@gmail.com";
        String[] arrEmail = email.split("\\@");
        String password1 = "6A75Bwvg%46H";
        String password2 = "YQ5%Ch4^h7&k";
        String password3 = "v4f43s#Q3F3v";

        String emailEncrypt = PassTranformer.encrypt(arrEmail[0] + "#2019@" + arrEmail[1]);
        String password1Encrypt = PassTranformer.encrypt(password1 + "_2019@#$%");
        String password2Encrypt = PassTranformer.encrypt(password2 + "_2019@#$%");
        String password3Encrypt = PassTranformer.encrypt(password3 + "_2019@#$%");
        System.out.println("After encrypt: " + emailEncrypt + ", pass1: " + password1Encrypt + ",\n pass2: "
                + password2Encrypt + ",\n pass3: " + password3Encrypt);
        String emailDecrypt = PassTranformer.decrypt(emailEncrypt);
//        String emailDecrypt = PassTranformer.decrypt("5c598ae82ba9dc450143916cf1b06715adbbd02a58421f71ecd84af12e708b0a");
        String[] arrEmailDecrypt = emailDecrypt.split("\\#");
        String finalEmail = arrEmailDecrypt[0] + "@" + arrEmailDecrypt[1].split("\\@")[1];
        String pass1Decrypt = PassTranformer.decrypt(password1Encrypt);
        String[] arr1PassDecrypt = pass1Decrypt.split("\\_");
        String finalPass1 = arr1PassDecrypt[0];

        String pass2Decrypt = PassTranformer.decrypt(password2Encrypt);
        String[] arr2PassDecrypt = pass2Decrypt.split("\\_");
        String finalPass2 = arr2PassDecrypt[0];

        String pass3Decrypt = PassTranformer.decrypt(password3Encrypt);
        String[] arr3PassDecrypt = pass3Decrypt.split("\\_");
        String finalPass3 = arr3PassDecrypt[0];

        System.out.println("Final email: " + finalEmail + ", \npassword1: " + finalPass1 + ", \npassword2: " + finalPass2 + ", \npassword3: " + finalPass3);
    }
}
