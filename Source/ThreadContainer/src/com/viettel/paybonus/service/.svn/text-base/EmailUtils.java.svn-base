/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.service;

import com.sun.mail.util.MailSSLSocketFactory;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import javax.activation.DataHandler;
import javax.activation.FileDataSource;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

/**
 *
 * @author dev_linh
 */
public class EmailUtils {

    public static String sendInformEmail(final HashMap<String, String> param) {
        if (!param.containsKey("EMAIL_HOST") || !param.containsKey("EMAIL_PORT") || !param.containsKey("EMAIL_ADDRESS") || !param.containsKey("EMAIL_PASSWORD")) {
            return "No have parameter config of Mail";
        }
        if (!param.containsKey("SEND_EMAIL") || param.get("SEND_EMAIL").trim().equals("")) {
            return "No have contain SEND_EMAIL";
        }
        try {
            Properties props = System.getProperties();
            props.put("mail.smtps.host", param.get("EMAIL_HOST"));
            props.put("mail.smtps.port", param.get("EMAIL_PORT"));
            props.put("mail.smtps.auth", "true");
            props.put("mail.transport.protocol", "smtps");
            props.put("mail.smtps.timeout", "90000");
            props.put("mail.smtps.connectiontimeout", "50000");
            MailSSLSocketFactory socketFactory = new MailSSLSocketFactory();
            socketFactory.setTrustAllHosts(true);
            props.put("mail.smtps.ssl.socketFactory", socketFactory);
            Session mailSession = Session.getDefaultInstance(props);
            Multipart multipart = new MimeMultipart();
            MimeBodyPart messagePart = new MimeBodyPart();
            messagePart.setContent(fillKeys(param.get("EMAIL_CONTENT"), param), "text/html; charset=UTF-8");
            multipart.addBodyPart(messagePart);

            javax.mail.BodyPart filebodyPart = new javax.mail.internet.MimeBodyPart();
            filebodyPart.setDataHandler(new DataHandler(new FileDataSource(param.get("FILE_PATH"))));
            filebodyPart.setFileName(new java.io.File(param.get("FILE_PATH")).getName());
            multipart.addBodyPart(filebodyPart);

            MimeMessage message = new MimeMessage(mailSession);
            message.setFrom(new InternetAddress(param.get("EMAIL_ADDRESS"), param.get("EMAIL_ADDRESS"), "UTF-8"));
            message.setRecipient(Message.RecipientType.TO, new InternetAddress(param.get("SEND_EMAIL")));
            message.setSubject(fillKeys(param.get("EMAIL_SUBJECT"), param), "utf-8");
            message.setSentDate(new Date());
            message.setContent(multipart);
            Transport transport = mailSession.getTransport("smtps");
            int port = 465;
            try {
                port = Integer.parseInt(param.get("EMAIL_PORT"));
            } catch (Exception ex) {
                ex.printStackTrace();
                return "Can not parse EMAIL_PORT";
            }
            transport.connect(param.get("EMAIL_HOST"), port, param.get("EMAIL_ADDRESS"), param.get("EMAIL_PASSWORD"));
            transport.sendMessage(message, message.getRecipients(Message.RecipientType.TO));
            transport.close();
            return "0";
        } catch (Exception e) {
            e.printStackTrace();
            return "Error when sendInformEmail: " + e.toString();
        }
    }

    public static String fillKeys(String strIn, HashMap<String, String> param) {
        for (String k : param.keySet()) {
            strIn = repAll(strIn, k, (String) param.get(k));
        }
        return strIn;
    }

    public static String repAll(String str, String oldStr, String newStr) {
        if ((oldStr == null) || (oldStr.equals("")) || (oldStr.length() > str.length())) {
            return str;
        }
        if ((str == null) || (str.length() <= 0)) {
            return str;
        }
        int idx = str.indexOf(oldStr, 0);
        while (idx != -1) {
            if (idx + oldStr.length() < str.length()) {
                str = str.substring(0, idx) + newStr + str.substring(idx + oldStr.length());
            } else {
                str = str.substring(0, idx) + newStr;
            }
            idx = str.indexOf(oldStr, idx + newStr.length());
        }
        return str;
    }

    public static String sendInformEmailWithMultipleAttached(final HashMap<String, String> param, List<String> listAttachedFilePath) {
        if (!param.containsKey("EMAIL_HOST") || !param.containsKey("EMAIL_PORT") || !param.containsKey("EMAIL_ADDRESS") || !param.containsKey("EMAIL_PASSWORD")) {
            return "No have parameter config of Mail";
        }
        if (!param.containsKey("SEND_EMAIL") || param.get("SEND_EMAIL").trim().equals("")) {
            return "No have contain SEND_EMAIL";
        }
        try {
            Properties props = System.getProperties();
            props.put("mail.smtps.host", param.get("EMAIL_HOST"));
            props.put("mail.smtps.port", param.get("EMAIL_PORT"));
            props.put("mail.smtps.auth", "true");
            props.put("mail.transport.protocol", "smtps");
            props.put("mail.smtps.timeout", "90000");
            props.put("mail.smtps.connectiontimeout", "50000");
            MailSSLSocketFactory socketFactory = new MailSSLSocketFactory();
            socketFactory.setTrustAllHosts(true);
            props.put("mail.smtps.ssl.socketFactory", socketFactory);
            Session mailSession = Session.getDefaultInstance(props);
            Multipart multipart = new MimeMultipart();
            MimeBodyPart messagePart = new MimeBodyPart();
            messagePart.setContent(fillKeys(param.get("EMAIL_CONTENT"), param), "text/html; charset=UTF-8");
            multipart.addBodyPart(messagePart);

            //add attached file
            if (listAttachedFilePath != null && !listAttachedFilePath.isEmpty()) {
                for (String file : listAttachedFilePath) {
                    addAttachment(multipart, file);
                }
            }

            MimeMessage message = new MimeMessage(mailSession);
            message.setFrom(new InternetAddress(param.get("EMAIL_ADDRESS"), param.get("EMAIL_ADDRESS"), "UTF-8"));
            message.setRecipient(Message.RecipientType.TO, new InternetAddress(param.get("SEND_EMAIL")));
            message.setSubject(fillKeys(param.get("EMAIL_SUBJECT"), param), "utf-8");
            message.setSentDate(new Date());
            message.setContent(multipart);
            Transport transport = mailSession.getTransport("smtps");
            int port = 465;
            try {
                port = Integer.parseInt(param.get("EMAIL_PORT"));
            } catch (Exception ex) {
                ex.printStackTrace();
                return "Can not parse EMAIL_PORT";
            }
            transport.connect(param.get("EMAIL_HOST"), port, param.get("EMAIL_ADDRESS"), param.get("EMAIL_PASSWORD"));
            transport.sendMessage(message, message.getRecipients(Message.RecipientType.TO));
            transport.close();
            return "0";
        } catch (Exception e) {
            e.printStackTrace();
            return "Error when sendInformEmail: " + e.toString();
        }
    }

    private static void addAttachment(Multipart multipart, String filePath) throws MessagingException {
        javax.mail.BodyPart filebodyPart = new javax.mail.internet.MimeBodyPart();
        filebodyPart.setDataHandler(new DataHandler(new FileDataSource(filePath)));
        filebodyPart.setFileName(new java.io.File(filePath).getName());
        multipart.addBodyPart(filebodyPart);
    }
}
