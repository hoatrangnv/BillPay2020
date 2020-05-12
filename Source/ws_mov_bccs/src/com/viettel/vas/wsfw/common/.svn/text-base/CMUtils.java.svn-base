/*
 * Copyright 2012 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.vas.wsfw.common;

import java.io.StringReader;
import java.util.ArrayList;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.apache.log4j.Logger;
import org.w3c.dom.CharacterData;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

/**
 *
 * @author kdvt_tungtt8
 * @version x.x
 * @since Dec 17, 2012
 */
public class CMUtils {

    public static String getPropertyXML(String subInfor, String propertyName) {
        Logger logger = Logger.getRootLogger();
        try {
            DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            InputSource is = new InputSource();
            is.setCharacterStream(new StringReader(subInfor));

            Document doc = db.parse(is);
            NodeList nodes = doc.getElementsByTagName(propertyName);
            Element line = (Element) nodes.item(0);
            return getCharacterDataFromElement(line);
        } catch (Exception ex) {
            logger.info(ex);
        }
        return "";
    }

    public static String[] getVasList(String subInfor) {
        Logger logger = Logger.getRootLogger();
        ArrayList listVas = new ArrayList();
        try {
            DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            InputSource is = new InputSource();
            is.setCharacterStream(new StringReader(subInfor));

            Document doc = db.parse(is);
            NodeList nodes = doc.getElementsByTagName("VAS");
            Element element = (Element) nodes.item(0);
            NodeList name = element.getElementsByTagName("ITEM");
            for (int i = 0; i < name.getLength(); ++i) {
                Element line = (Element) name.item(i);
                listVas.add(getCharacterDataFromElement(line).trim());
            }
        } catch (Exception ex) {
            logger.info(ex);
        }
        String[] strVas = (String[]) listVas.toArray(new String[listVas.size()]);
        return strVas;
    }

    public static String[] getHomeNumberList(String subInfor) {
        Logger logger = Logger.getRootLogger();
        ArrayList listVas = new ArrayList();
        try {
            DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            InputSource is = new InputSource();
            is.setCharacterStream(new StringReader(subInfor));

            Document doc = db.parse(is);
            NodeList name = doc.getElementsByTagName("ITEM");
            for (int i = 0; i < name.getLength(); ++i) {
                Element line = (Element) name.item(i);
                listVas.add(getCharacterDataFromElement(line).trim());
            }
        } catch (Exception ex) {
            logger.info(ex);
        }
        String[] strVas = (String[]) listVas.toArray(new String[listVas.size()]);
        return strVas;
    }

    private static String getCharacterDataFromElement(Element e) {
        Node child = e.getFirstChild();
        if (child instanceof CharacterData) {
            CharacterData cd = (CharacterData) child;
            return cd.getData();
        }
        return "";
    }
}
