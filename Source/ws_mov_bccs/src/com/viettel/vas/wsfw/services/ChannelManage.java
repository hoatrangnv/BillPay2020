/*
 * Copyright (C) 2010 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.vas.wsfw.services;

import com.viettel.vas.wsfw.common.WebserviceAbstract;
import javax.jws.WebMethod;
import javax.jws.WebParam;
import javax.jws.WebService;
import com.viettel.vas.wsfw.database.DbChannelProcessor;
import com.viettel.vas.wsfw.object.FTPDownloadFile;
import com.viettel.vas.wsfw.object.ResponseChannel;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 *
 * @author minhnh@viettel.com.vn
 * @since Jun 4, 2013
 * @version 1.0
 */
@WebService
public class ChannelManage extends WebserviceAbstract {

    DbChannelProcessor db;

    public ChannelManage() {
        super("getAvatarOfStaffId");
        try {
            db = new DbChannelProcessor("dbBockd", logger);
        } catch (Exception ex) {
            logger.error("Fail init webservice ChannelManage");
            logger.error(ex);
        }
    }

    @WebMethod(operationName = "getAvatarOfStaffId")
    public ResponseChannel ShowAccInfo(@WebParam(name = "staffCode") String staffCode) throws Exception {
        ResponseChannel response = new ResponseChannel();
        logger.info("Start process get Image ");
//        Step 1: validate input
        if (staffCode == null || "".equals(staffCode.trim())) {
            logger.warn("Invalid input staffCode " + staffCode);
            response.setErrorCode(01);
            response.setDescription("Staff code not null");
            return response;
        }
//        Step 2: check DB
        String check = db.validateInfo(staffCode.toUpperCase());
        if (check == null || "".equals(check.trim())) {
            logger.warn("Invalid input staffCode " + staffCode);
            response.setErrorCode(02);
            response.setDescription("Not Img");
            return response;
        }

//        Step 3: getfile from FTP
        FTPDownloadFile fTPDownloadFile = new FTPDownloadFile();
        fTPDownloadFile.getFileToFtpServer(check);
//        Step 4: zip file
        byte[] zip = createZIP(check);
        if (zip != null) {
            response.setErrorCode(0);
            response.setDescription("Sucsess");
            response.setData(zip);
        } else {
            response.setErrorCode(03);
            response.setDescription("Not Image");
            response.setData(zip);
        }
        return response;
    }

    public static byte[] createZIP(String stringsFileName) {
        stringsFileName = stringsFileName.replaceAll(".jpg", ".zip");
        byte[] zipFile = null;
        String[] filesrc = stringsFileName.split("\\/", 3);
        try {
//            StringBuilder sb = new StringBuilder();
//            sb.append(filesrc[2]);
//
//            File file = new File(filesrc[2]);
//            ZipOutputStream out = new ZipOutputStream(new FileOutputStream(file));
//            ZipEntry ze = new ZipEntry(filesrc[2]);
//            out.putNextEntry(ze);
//
//            byte[] data = sb.toString().getBytes();
//            out.write(data, 0, data.length);
//            out.closeEntry();
//
//            out.close();
//            zipFile = data;

            byte[] buffer = new byte[1024];
            FileOutputStream fos = new FileOutputStream(filesrc[2]);
            ZipOutputStream zos = new ZipOutputStream(fos);
            File srcFile = new File(filesrc[2]);
            FileInputStream fis = new FileInputStream(srcFile);
            // begin writing a new ZIP entry, positions the stream to the start of the entry data

            zos.putNextEntry(new ZipEntry(srcFile.getName()));
            int length;
            while ((length = fis.read(buffer)) > 0) {
                zos.write(buffer, 0, length);
            }
            zos.closeEntry();
            fis.close();
            zipFile = buffer;
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return zipFile;
    }
}
