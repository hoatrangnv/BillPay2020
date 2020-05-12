/**
 * @(#)XmlConfig.java May 10, 2010 Copyright 2010 Viettel Telecom. All rights
 * reserved VIETTEL PROPRIETARY/CONFIDENTIAL.Use is subject to license terms.
 */
package com.viettel.data.ws.utils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;


import org.jdom.*;
import org.jdom.input.*;
import org.jdom.output.Format;
import org.jdom.output.XMLOutputter;

/**
 * Description:
 *
 * @author: Nguyen Anh Tue
 * @since: May 10, 2010
 */
public class XmlConfig {

    private Document document;
    private String xmlFileName = "";
    private Element root;

//    public Element eParent;
    public Document getDocument() {
        return document;
    }

    public void setRoot(Element root) {
        this.root = root;
    }

    /**
     * Nguyen Anh Tue
     *
     * @since: May 12, 2010
     * @param:
     * @return:
     * @modified by:
     * @modified Date: return insert SQL
     */
    public XmlConfig() {
    }

    ;

    /**
     * Nguyen Anh Tue
     * @since:  May 12, 2010
     * @param:
     * @return:
     * @modified by:
     * @modified Date:
     * return insert SQL
     * @param mConfFileName
     */
    public XmlConfig(String mConfFileName) {
        xmlFileName = mConfFileName;
    }

    /*
     /**
     * Nguyen Anh Tue
     * @since:  May 12, 2010
     * @param:
     * @return:
     * @modified by:
     * @modified Date:
     * Load file xml config
     */
//    public void load() throws Exception {
//        SAXBuilder builder = new SAXBuilder();
//        try {
//            InputStream is = new FileInputStream(new File(xmlFileName));
//            document = builder.build(is);
//            root = document.getRootElement();
//            is.close();
//        } catch (Exception ex) {
//            ex.printStackTrace()
//        }
//    }

    /*
     /**
     * Nguyen Thanh Binh
     * @since:  Mar 17, 2011
     * @param:
     * @return:
     * @modified by:
     * @modified Date:
     * Load file xml content in a string
     */
    public void load(String xmlContent, Logger logger) {
        SAXBuilder builder = new SAXBuilder();
        try {
            InputStream is = new ByteArrayInputStream(xmlContent.getBytes("UTF-8"));
            document = builder.build(is);
            root = document.getRootElement();
            is.close();
        } catch (Exception ex) {
//            ex.printStackTrace();
            logger.debug("Load xml error:" + ex.getMessage());
        }
    }

    /**
     * /**
     * Nguyen Anh Tue
     *
     * @since: May 12, 2010
     * @param:
     * @return:
     * @modified by:
     * @modified Date: Write to xml file
     * @throws IOException
     */
    public void writeToFile() throws IOException {
        FileWriter writer = null;
        try {
            XMLOutputter outputter = new XMLOutputter();
            writer = new FileWriter(xmlFileName);
            //Set format
            outputter.setFormat(Format.getPrettyFormat());
            outputter.output(document, writer);

        } catch (IOException e) {
//            e.printStackTrace();
        } finally {
            writer.flush();
            writer.close();
        }
    }

    /**
     * /**
     * Nguyen Anh Tue
     *
     * @since: May 12, 2010
     * @param:
     * @return:
     * @modified by:
     * @modified Date: findElement
     * @param sPath
     * @return
     */
    public Element findElement(String sPath) {
        return XmlUtil.findElement(root, sPath);
    }

    /**
     * /**
     * Nguyen Anh Tue
     *
     * @since: May 12, 2010
     * @param: sPath
     * @return:
     * @modified by: Nguyen Thanh Binh
     * @modified Date: Mar 17, 2011 findElement
     * @param elementName
     */
    public String getTextValue(String elementName) {
        Element element = XmlUtil.findElement(root, elementName);
        if (element != null) {
            return element.getTextTrim();
        } else {
            return null;
        }
    }

    /**
     * /**
     * Nguyen Anh Tue
     *
     * @since: May 12, 2010
     * @param:
     * @return:
     * @modified by:
     * @modified Date: setTextValue
     * @param sPath
     * @param sValue
     */
    public void setTextValue(String sPath, String sValue) {
        Element element = null;
//        element = XmlUtil.findElementByPath(root, sPath);
        element = XmlUtil.findElement(root, sPath);
        if (element != null) {
            element.setText(sValue);
        }

    }

    /**
     * /**
     * Nguyen Anh Tue
     *
     * @since: May 12, 2010
     * @param:
     * @return:
     * @modified by:
     * @modified Date: setAttributeValue
     * @param sPath
     * @param attrName
     * @param sValue
     */
    public void setAttributeValue(String sPath, String attrName, String sValue) {
        Attribute attribute = null;
        attribute = XmlUtil.findAtribute(root, sPath, attrName);
        if (attribute != null) {
            attribute.setValue(sValue);
        }
    }

    /**
     * Nguyen Anh Tue
     *
     * @since: May 12, 2010
     * @param:
     * @return:
     * @modified by:
     * @modified Date: getAttributeValue
     * @param sPath
     * @param attrName
     * @return
     */
    public Attribute getAttribute(String sPath, String attrName) {

        return XmlUtil.findAtribute(root, sPath, attrName);

    }

    /**
     * Nguyen Anh Tue
     *
     * @since: May 12, 2010
     * @param:
     * @return:
     * @modified by:
     * @modified Date: getAttributeValue
     * @param sPath
     * @param attrName
     * @return
     */
    public Element getRoot() {

        return root;

    }

    /**
     * /**
     * Nguyen Thanh Binh
     *
     * @since: Mar 17, 2011
     * @param: Find list of Element by Element name
     * @return: List of Element
     */
    public List<Element> findListElement(String elementName) {
        List<Element> resultList = new ArrayList<Element>();
        findElement(resultList, root, elementName);
        return resultList;
    }

    public void findElement(List<Element> resultList, Element current, String elementName) {
        if (current.getName().equals(elementName)) {
            resultList.add(current);
            return;
        } else {
            List<Element> children = current.getChildren();
            for (Element child : children) {
                findElement(resultList, child, elementName);
            }
        }
    }
}
