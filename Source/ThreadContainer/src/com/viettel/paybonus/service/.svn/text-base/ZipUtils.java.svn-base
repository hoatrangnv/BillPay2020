/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.service;

import java.io.File;
import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import net.lingala.zip4j.model.ZipParameters;
import net.lingala.zip4j.util.Zip4jConstants;

/**
 *
 * @author dev_linh
 */
public class ZipUtils {

    /**
     * Method for creating password protected zip file
     *
     * @param sourcePath
     * @throws ZipException
     */
    public static void compressWithPassword(String sourcePath, String password, String fileName) throws ZipException {

        String destPath = sourcePath + "order_pincode_" + fileName + ".zip";
        System.out.println("Destination " + destPath);
        ZipFile zipFile = new ZipFile(destPath);
        // Setting parameters
        ZipParameters zipParameters = new ZipParameters();
        zipParameters.setCompressionMethod(Zip4jConstants.COMP_DEFLATE);
        zipParameters.setCompressionLevel(Zip4jConstants.DEFLATE_LEVEL_ULTRA);
        zipParameters.setEncryptFiles(true);
        zipParameters.setEncryptionMethod(Zip4jConstants.ENC_METHOD_AES);
        zipParameters.setAesKeyStrength(Zip4jConstants.AES_STRENGTH_256);
        // Setting password
        zipParameters.setPassword(password);

        zipFile.addFolder(sourcePath, zipParameters);

        File file = new File(sourcePath);
        File[] lstFile = file.listFiles();
        for (File tmpFile : lstFile) {
            if (!tmpFile.getName().startsWith("order_pincode_")) {
                tmpFile.delete();
            }
        }
    }

    /**
     * Method for un zipping password protected file
     *
     * @param sourcePath
     * @throws ZipException
     */
    public static void unCompressPasswordProtectedFiles(String sourcePath, String password) throws ZipException {
        String destPath = sourcePath.substring(0, sourcePath.lastIndexOf("."));
        System.out.println("Destination " + destPath);
        ZipFile zipFile = new ZipFile(sourcePath);
        // If it is encrypted then provide password
        if (zipFile.isEncrypted()) {
            zipFile.setPassword(password);
        }
        zipFile.extractAll(destPath);
    }
}
