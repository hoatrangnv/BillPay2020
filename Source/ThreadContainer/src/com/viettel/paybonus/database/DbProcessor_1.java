/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.database;

import com.viettel.vas.util.ConnectionPoolManager;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.ResourceBundle;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author MinhNH
 */
public class DbProcessor_1 {

    public Logger logger = Logger.getLogger(DbProcessor.class);
    private String loggerLabel = "DbProcessorAbstract: ";

    public Connection getConnection(String dbName) throws Exception {
        ConnectionPoolManager.loadConfig("D:\\STUDY\\Project\\Movitel\\PinCodeManager\\etc\\database.xml");
//        ConnectionPoolManager.loadConfig("../etc/database.xml");
        return ConnectionPoolManager.getConnection(dbName);
    }

    public DbProcessor_1() {
        //Them ghi Log4j
        PropertyConfigurator.configure("D:\\STUDY\\Project\\Movitel\\PinCodeManager\\etc\\log.conf");
//        PropertyConfigurator.configure(log4JPath);
    }

    public void logTime(String strLog, long timeSt) {
        long timeEx = System.currentTimeMillis() - timeSt;
        StringBuilder br = new StringBuilder();
        if (timeEx >= 10000) {
            br.setLength(0);
            br.append(loggerLabel).
                    append("Slow db: ").
                    append(strLog).
                    append(": ").
                    append(timeEx).
                    append(" ms");

            logger.warn(br);
        } else {
            br.setLength(0);
            br.append(loggerLabel).
                    append(strLog).
                    append(": ").
                    append(timeEx).
                    append(" ms");

            logger.info(br);
        }
    }

    public void closeConnection(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
                conn = null;
            } catch (SQLException ex) {
                logger.warn(loggerLabel + "Close Statement: " + ex);
                conn = null;
            }
        }
    }

    public void closeStatement(PreparedStatement stmt) {
        if (stmt != null) {
            try {
                stmt.close();
                stmt = null;
            } catch (SQLException ex) {
                logger.warn(loggerLabel + "ERROR close Statement: " + ex.getMessage());
                stmt = null;
            }
        }
    }

    public void closeResultSet(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
                rs = null;
            } catch (SQLException ex) {
                logger.warn(loggerLabel + "ERROR close ResultSet: " + ex.getMessage());
                rs = null;
            }
        }
    }

    public static String logException(long times, Throwable ex) {
        long timeEx = System.currentTimeMillis() - times;
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        ex.printStackTrace(pw);
        return "Error: " + sw.toString() + " Time: " + timeEx;
    }

    public void getPinCodeBySerial(String fromSerial, String toSerial, String tableName) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        String sqlMo = " select * from " + tableName + " where to_number(serial) between to_number(?) and to_number(?)";
        PreparedStatement psMo = null;        
        try {
            connection = getConnection("dbvipsub");
            psMo = connection.prepareStatement(sqlMo);
            psMo.setString(1, fromSerial);
            psMo.setString(2, toSerial);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                String serial = rs1.getString("serial");
                String pincode = rs1.getString("pincode");
            }
            logTime("Time to getPinCodeBySerial fromSerial " + fromSerial + " toSerial "
                    + toSerial + " tableName " + tableName, timeSt);
        } catch (Throwable ex) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error("ERROR getPinCodeBySerial fromSerial " + fromSerial + " toSerial "
                    + toSerial + " tableName " + tableName + " detail " + sw.toString());
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);            
        }
    }    
}
