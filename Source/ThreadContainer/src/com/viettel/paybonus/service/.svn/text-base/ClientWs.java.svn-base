///*
// * Copyright (C) 2010 Viettel Telecom. All rights reserved.
// * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
// */
//package com.viettel.paybonus.service;
//
//import com.viettel.threadfw.manager.AppManager;
//import java.awt.image.BufferedImage;
//import java.io.ByteArrayInputStream;
//import java.io.ByteArrayOutputStream;
//import java.io.File;
//import javax.imageio.ImageIO;
//import org.apache.commons.httpclient.HttpConnectionManager;
//import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
//import org.apache.commons.httpclient.methods.PostMethod;
//import org.apache.commons.httpclient.methods.RequestEntity;
//import org.apache.commons.httpclient.methods.StringRequestEntity;
//import org.apache.commons.httpclient.params.HttpConnectionManagerParams;
//import java.io.InputStream;
//import sun.misc.BASE64Decoder;
//import sun.misc.BASE64Encoder;
//
///**
// *
// * @author minhnh@viettel.com.vn
// * @since Jun 4, 2013
// * @version 1.0
// */
//public class ClientWs {
//
////    DbChannelProcessor db;
//    public ClientWs() {
//    }
//
//    public String callHttp(String filePath) {
//        String soapResponse = "";
////        byte[] sign = getArrayBytes(filePath);
//        long start = System.currentTimeMillis();
//        PostMethod post = new PostMethod("http://10.229.47.30:8096/ConvertImage?wsdl");
//        try {
//            byte[] imageInByte;
//            BufferedImage originalImage = ImageIO.read(new File(filePath));
//
//            // convert BufferedImage to byte array
//            ByteArrayOutputStream baos = new ByteArrayOutputStream();
//            ImageIO.write(originalImage, "jpg", baos);
//            baos.flush();
//            imageInByte = baos.toByteArray();
//            baos.close();
//            String fileOld = (new BASE64Encoder()).encode(imageInByte);
//            // convert byte array back to BufferedImage
//            BASE64Decoder decoder = new BASE64Decoder();
//            byte[] imageByte2 = decoder.decodeBuffer(fileOld);
//            InputStream in = new ByteArrayInputStream(imageByte2);
//            BufferedImage bImageFromConvert = ImageIO.read(in);
//
//            ImageIO.write(bImageFromConvert, "jpg", new File(
//                    "D:\\STUDY\\Project\\Movitel\\MOV_WS\\new2.jpg"));
//            String soapRequest = "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:ser=\"http://services.wsfw.vas.viettel.com/\">\n"
//                    + "   <soapenv:Header/>\n"
//                    + "   <soapenv:Body>\n"
//                    + "      <ser:pushImage>\n"
//                    + "         <dataImage>"
//                    + (new BASE64Encoder()).encode(imageInByte)
//                    + "</dataImage>\n"
//                    + "      </ser:pushImage>\n"
//                    + "   </soapenv:Body>\n"
//                    + "</soapenv:Envelope>";
//            RequestEntity entity = new StringRequestEntity(soapRequest, "text/xml", "UTF-8");
//            post.setRequestEntity(entity);
//            post.setRequestHeader("SOAPAction", "false");
//            org.apache.commons.httpclient.HttpClient httpTransport = null;
//            MultiThreadedHttpConnectionManager conMgr = null;
//            if (conMgr == null) {
//                conMgr = new MultiThreadedHttpConnectionManager();
//                conMgr.setMaxConnectionsPerHost(20000);
//                conMgr.setMaxTotalConnections(20000);
//            }
//            if (httpTransport == null) {
//                httpTransport = new org.apache.commons.httpclient.HttpClient(conMgr);
//                HttpConnectionManager conMgr1 = httpTransport.getHttpConnectionManager();
//                HttpConnectionManagerParams conPars = conMgr1.getParams();
//                conPars.setMaxTotalConnections(2000);
//                conPars.setConnectionTimeout(30000); //timeout ket noi : ms
//                conPars.setSoTimeout(60000); //timeout doc ket qua : ms
//            }
//            httpTransport.executeMethod(post);
//            soapResponse = post.getResponseBodyAsString();
//            System.out.println("Finish callHttp soapRequest " + soapRequest);
//            return soapResponse;
//        } catch (Exception ex) {
//            System.out.println("Exception callHttp ex " + ex.toString());
//            System.out.println(AppManager.logException(start, ex));
//            return soapResponse;
//        } finally {
//            post.releaseConnection();
//        }
//    }
//
//    public static void main(String[] args) {
//        ClientWs test = new ClientWs();
//        String result = test.callHttp("D:\\STUDY\\Project\\Movitel\\MOV_WS\\test.jpg");
//        System.out.println("Result " + result);
//        
//                // connects to the web service
//        FileTransfererService service = new FileTransfererService();
//        FileTransferer port = service.getFileTransfererPort(new MTOMFeature(10240));
//         
//        String fileName = "binary.png";
//        String filePath = "e:/Test/Client/Upload/" + fileName;
//        File file = new File(filePath);
//         
//        // uploads a file
//        try {
//            FileInputStream fis = new FileInputStream(file);
//            BufferedInputStream inputStream = new BufferedInputStream(fis);
//            byte[] imageBytes = new byte[(int) file.length()];
//            inputStream.read(imageBytes);
//             
//            port.upload(file.getName(), imageBytes);
// 
//            inputStream.close();
//            System.out.println("File uploaded: " + filePath);
//        } catch (IOException ex) {
//            System.err.println(ex);
//        }      
//         
//        // downloads another file
//        fileName = "camera.png";
//        filePath = "E:/Test/Client/Download/" + fileName;
//        byte[] fileBytes = port.download(fileName);
//         
//        try {
//            FileOutputStream fos = new FileOutputStream(filePath);
//            BufferedOutputStream outputStream = new BufferedOutputStream(fos);
//            outputStream.write(fileBytes);
//            outputStream.close();
//             
//            System.out.println("File downloaded: " + filePath);
//        } catch (IOException ex) {
//            System.err.println(ex);
//        }
//    }
//}
