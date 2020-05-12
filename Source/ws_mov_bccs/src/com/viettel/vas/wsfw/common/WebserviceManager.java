/*
 * Copyright (C) 2010 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.vas.wsfw.common;

//import com.sun.net.httpserver.HttpContext;
//import com.sun.net.httpserver.HttpsConfigurator;
//import com.sun.net.httpserver.HttpsServer;
import com.viettel.utility.PropertiesUtils;
import com.viettel.vas.util.ConnectionPoolManager;
import java.io.File;
//import java.io.FileInputStream;
import java.io.FileReader;
//import java.net.InetSocketAddress;
//import java.security.KeyStore;
//import java.security.SecureRandom;
import java.util.*;
//import javax.net.ssl.KeyManagerFactory;
//import javax.net.ssl.SSLContext;
//import javax.net.ssl.TrustManagerFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.ws.Endpoint;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import utils.Config;

/**
 *
 * @author minhnh@viettel.com.vn
 * @since Jun 3, 2013
 * @version 1.0
 */
public class WebserviceManager {

    private static WebserviceManager instance;
    private Logger logger = Logger.getLogger(WebserviceManager.class);
    private static List<WebserviceObject> listWS;
    public static HashMap<String, Integer> listWSname;
    private static List<Endpoint> listEndpoint;
    private String connectorsFile;
    public static String processClass;
    public static String appId;
    public static int maxRow;
    public static int queryDbTimeout;
    public static int breakQuery;
    public static int dbTimeOut;
    public static boolean exchangeEnable;
    public static boolean enableQueryDbTimeout;
    public static String pathDatabase;
    public static String pathExch;
    public static long[] timesDbLevel;
    public static long[] timesOcsLevel;
    public static long minTimeDb;
    public static long minTimeOcs;
    public static HashMap loggerDbMap;
    public static HashMap loggerOcsMap;
    public static String schemaPre;
    public static String schemaPos;
    public static String shortCode;
    public static String messageRecharge;
    public static String accountList;

    public static WebserviceManager getInstance() throws Exception {
        if (instance == null) {
            instance = new WebserviceManager();
            instance.initWebService();
        }
        return instance;
    }

    public void initWebService() {
        try {
            // get config
            String config = Config.configDir + File.separator + "app.conf";
            FileReader fileReader = null;
            fileReader = new FileReader(config);
            Properties pro = new Properties();
            pro.load(fileReader);
            // Ma ung dung
            try {
                appId = pro.getProperty("APP_ID").toUpperCase();
            } catch (Exception ex) {
                logger.warn("APP_ID not found in app.conf\n");
                appId = "NoName";
            }
            // So luong record xu ly MO
            try {
                maxRow = Integer.parseInt(pro.getProperty("MAX_ROW"));
            } catch (Exception ex) {
                logger.warn("MAX_ROW not found in app.conf => Default value: 100\n");
                maxRow = 100;
            }
            /**
             * EXCHANGE
             */
            try {
                exchangeEnable = Boolean.parseBoolean(pro.getProperty("EXCHANGE_ENABLE"));
            } catch (Exception ex) {
                logger.warn("EXCHANGE_ENABLE not found in app.conf => Default value: false\n");
                exchangeEnable = false;
            }
            if (exchangeEnable) {
                try {
                    pathExch = pro.getProperty("PATH_EXCH");
                } catch (Exception ex) {
                    logger.warn("PATH_EXCH not found in app.conf\n");
                }
            }

            /**
             * DATABASE
             */
            try {
                pathDatabase = pro.getProperty("PATH_DB");
                schemaPre = pro.getProperty("SCHEMA_PRE");
                schemaPos = pro.getProperty("SCHEMA_POS");
                shortCode = pro.getProperty("SHORT_CODE");
                messageRecharge = pro.getProperty("MESSAGE_RECHARGE");
                accountList = pro.getProperty("ACCOUNT_LIST");
            } catch (Exception ex) {
                logger.warn("PATH_DB not found in app.conf\n");
            }
            ConnectionPoolManager.loadConfig(pathDatabase);
            try {
                enableQueryDbTimeout = Boolean.parseBoolean(pro.getProperty("ENABLE_QUERY_DB_TIMEOUT", "false"));
            } catch (Exception ex) {
                logger.warn("ENABLE_QUERY_DB_TIMEOUT not found in app.conf => Default value: ENABLE_QUERY_DB_TIMEOUT = FALSE\n");
                enableQueryDbTimeout = false;
            }
            if (enableQueryDbTimeout) {
                try {
                    queryDbTimeout = Integer.parseInt(pro.getProperty("QUERY_DB_TIMEOUT"));
                } catch (Exception ex) {
                    logger.warn("QUERY_DB_TIMEOUT not found in app.conf\n");
                }
                try {
                    breakQuery = Integer.parseInt(pro.getProperty("BREAK_QUERY")) * 1000;
                } catch (Exception ex) {
                    logger.warn("BREAK_QUERY not found in app.conf\n");
                }
            }
            try {
                dbTimeOut = Integer.parseInt(pro.getProperty("DB_TIME_OUT"));
            } catch (Exception ex) {
                logger.warn("DB_TIME_OUT not found in app.conf => Default value: 300\n");
                dbTimeOut = 300;
            }
            fileReader.close();
            // Load log warning
            loadLogLevelWarnning();
            connectorsFile = Config.configDir + File.separator + "webservices.xml";
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();
            Document dc = db.parse(connectorsFile);
            Element root = dc.getDocumentElement();

            NodeList list = root.getElementsByTagName("webservice");
            if (list.getLength() < 1) {
                throw new Exception("No webservice to publish");
            }

            listWS = new ArrayList<WebserviceObject>();
            listWSname = new HashMap<String, Integer>();

            for (int i = 0; i < list.getLength(); ++i) {
                Element element = (Element) list.item(i);

                String name = element.getAttribute("name");

                if (listWSname.containsKey(name)) {
                    throw new Exception("same webservice name: " + name);
                }
                WebserviceObject webserviceObject = new WebserviceObject();

                logger.info("===> get config for webservice: " + name);
                webserviceObject.setName(name);
                webserviceObject.setIp(element.getAttribute("ip"));
                webserviceObject.setPort(element.getAttribute("port"));
                webserviceObject.setPath(element.getAttribute("path"));
                webserviceObject.setImplementClass(element.getAttribute("implementClass"));
                webserviceObject.makeUrl();
                listWSname.put(name, 1);
                listWS.add(webserviceObject);
            }
        } catch (Exception e) {
            logger.error("Error init webservice ", e);
        }
    }

    public void start() {
        logger.info("+++ SYSTEM STARTING ...  +++");
        ClassLoader cl = new ClassLoader() {
        };
        listEndpoint = new ArrayList<Endpoint>();
        for (WebserviceObject webserviceObject : listWS) {
            try {

                Class c = cl.loadClass(webserviceObject.getImplementClass());
                logger.info("===> Load class: " + c.getName());
                WebserviceAbstract webserviceAbstract = (WebserviceAbstract) c.newInstance();
//              Setup https
//                Endpoint service = Endpoint.create(webserviceAbstract);
//                SSLContext ssl = SSLContext.getInstance("TLS");
//                KeyManagerFactory keyFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
//                KeyStore store = KeyStore.getInstance("JKS");
//                store.load(new FileInputStream("D:\\STUDY\\Project\\Movitel\\TopUp\\Mueway\\wsTopup\\etc\\paymentgw.keystore"),
//                        "Mvgw@032017".toCharArray());
//                keyFactory.init(store, "Mvgw@032017".toCharArray());
//                TrustManagerFactory trustFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
//                trustFactory.init(store);
//                ssl.init(keyFactory.getKeyManagers(),
//                        trustFactory.getTrustManagers(), new SecureRandom());
//                HttpsConfigurator configurator = new HttpsConfigurator(ssl);
//                HttpsServer httpsServer = HttpsServer.create(new InetSocketAddress("10.229.47.30", 8089), 10000);
//                httpsServer.setHttpsConfigurator(configurator);
//                String uri = "/ShowAccInfo";
//                HttpContext httpContext = httpsServer.createContext(uri);
//                httpsServer.start();
//                service.publish(httpContext);
//                End setup https
                Endpoint service = Endpoint.publish(webserviceObject.getUrl(), webserviceAbstract);
                logger.info("Publish service " + webserviceObject.getName() + " success!");
                logger.info("URL: " + webserviceObject.getUrl() + "?wsdl");
                listEndpoint.add(service);
            } catch (Exception e) {
                logger.error("Publish service " + webserviceObject.getName() + " error!", e);
            }

        }
        logger.info("+++ SYSTEM PROCESS STARTED  +++");
    }

    public void stop() {
        logger.info("+++ SYSTEM STOPPING ...  +++");
        for (Endpoint endpoint : listEndpoint) {
            try {
                endpoint.stop();
            } catch (Exception e) {
                logger.error("Stop endpoint " + endpoint.getClass().toString() + " error!", e);
            }
        }
        logger.info("+++ SYSTEM PROCESS STOPPED  +++");
    }

    /**
     * Doc thong tin file loglevel.conf
     */
    private void loadLogLevelWarnning() throws Exception {
        PropertiesUtils pros = new PropertiesUtils();
        pros.loadProperties("../etc/loglevel.conf", false);
        try {
            String[] dbTimes = pros.getProperty("DB_TIMES").split(",");
            String[] dbKey = pros.getProperty("DB_MESSAGE_KEY").split(",");

            loggerDbMap = new HashMap();
            timesDbLevel = new long[dbTimes.length];
            minTimeDb = Long.parseLong(dbTimes[0].trim());
            for (int i = 0; i < dbTimes.length; i++) {
                timesDbLevel[i] = Long.parseLong(dbTimes[i].trim());
                loggerDbMap.put(i, dbKey[i].trim());
            }
        } catch (Exception ex) {
            logger.error("Loi lay thong tin DB_TIMES, DB_MESSAGE_KEY trong loglevel.conf");
            loggerDbMap = null;
            throw ex;
        }

        if (exchangeEnable) {
            try {
                String[] ocsTimes = pros.getProperty("OCS_TIMES").split(",");
                String[] ocsKey = pros.getProperty("OCS_MESSAGE_KEY").split(",");

                loggerOcsMap = new HashMap();
                timesOcsLevel = new long[ocsTimes.length];
                minTimeOcs = Long.parseLong(ocsTimes[0].trim());
                for (int i = 0; i < ocsTimes.length; i++) {
                    timesOcsLevel[i] = Long.parseLong(ocsTimes[i].trim());
                    loggerOcsMap.put(i, ocsKey[i].trim());
                }
            } catch (Exception ex) {
                logger.error("Loi lay thong tin OCS_TIMES, OCS_MESSAGE_KEY trong loglevel.conf");
                loggerOcsMap = null;
                throw ex;
            }
        }
    }

    /**
     * Log cham database
     *
     * @param times
     * @return
     */
    public static String getTimeLevelDb(long times) {
        if (loggerDbMap != null) {
            int key = Arrays.binarySearch(timesDbLevel, times);
            if (key < 0) {
                key = -key - 2;
            }

            String label = (String) loggerDbMap.get(key);

            return (label == null) ? "-" : label;
        }
        return null;
    }

    /**
     * Log cham ocs, hlr
     *
     * @param times
     * @return
     */
    public static String getTimeLevelOcs(long times) {
        if (loggerOcsMap != null) {
            int key = Arrays.binarySearch(timesOcsLevel, times);
            if (key < 0) {
                key = -key - 2;
            }

            String label = (String) loggerOcsMap.get(key);
            return (label == null) ? "-" : label;
        }
        return null;
    }
}
