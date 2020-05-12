package com.viettel.vas.wsfw.object;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ResourceBundle;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;

public class FTPDownloadFile {

    /**
     * author itbl_jony
     *
     */
    public void getFileToFtpServer(String urlFile) throws Exception {
        String server = "10.229.42.55";
        int port = 21;
        String user = "scan_doc";
        String pass = "8dw29Jk$3d";

        FTPClient ftpClient = new FTPClient();
        try {

            ftpClient.connect(server, port);
            ftpClient.login(user, pass);
            ftpClient.enterLocalPassiveMode();
            ftpClient.setFileType(FTP.BINARY_FILE_TYPE);

            // APPROACH #1: using retrieveFile(String, OutputStream)
            // create working folder
            ResourceBundle resourceBundle = ResourceBundle.getBundle("dbconfig");
            String path = resourceBundle.getString("pathLocalExportFile");

            String[] filesrc = urlFile.split("\\/", 3);
            new File(path + filesrc[1]).mkdir();

            String newPath = path + filesrc[1] + "/" + filesrc[2];
            // end create
            File downloadFile = new File(newPath);
            OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(downloadFile));
            boolean success = ftpClient.retrieveFile(urlFile, outputStream);
            outputStream.close();

            if (success) {
                System.out.println("File #1 has been downloaded successfully.");
            }

        } catch (IOException ex) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
        } finally {
            try {
                if (ftpClient.isConnected()) {
                    ftpClient.logout();
                    ftpClient.disconnect();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }
}
