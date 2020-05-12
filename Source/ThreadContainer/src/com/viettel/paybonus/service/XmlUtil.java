/**
 * @(#)XmlUtil.java Jul 15, 2010 Copyright 2010 Viettel Telecom. All rights
 * reserved VIETTEL PROPRIETARY/CONFIDENTIAL
 */
package com.viettel.paybonus.service;

import java.util.ArrayList;
import java.util.List;
import org.jdom.Attribute;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;
import org.jdom.output.DOMOutputter;

import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * Description:
 *
 * @author: Nguyen Anh Tue
 * @since: Jul 15, 2010
 */
public class XmlUtil {

    /**
     * Nguyen Anh Tue
     *
     * @since: May 12, 2010
     * @param:
     * @return:
     * @modified by:
     * @modified Date:
     * @param arrHeader
     * @param mElement
     * @param vtInput
     * @throws Exception
     */
    public static void changeElementTable(String[] arrHeader, Element mElement, ArrayList vtInput) throws Exception {

        changeElementTable(arrHeader, mElement, "row", vtInput);

    }

    /**
     * Nguyen Anh Tue
     *
     * @since: May 12, 2010
     * @param:
     * @return:
     * @modified by:
     * @modified Date:
     * @param arrHeader
     * @param mElement
     * @param elemntName
     * @param vtInput
     * @throws Exception
     */
    public static void changeElementTable(String[] arrHeader, Element mElement, String elemntName, ArrayList vtInput) throws Exception {

        ArrayList vtRow = null;
        Element row = null;
        mElement.removeChildren(elemntName);

        if (vtInput != null) {
            if (vtInput.size() > 0 && arrHeader.length > 0) {
                for (int i = 0; i < vtInput.size(); i++) {
                    vtRow = (ArrayList) vtInput.get(i);
                    if (vtRow != null) {
                        if (arrHeader.length + 1 <= vtRow.size()) {
                            row = new Element(elemntName);
                            for (int j = 1; j < vtRow.size(); j++) {
                                row.setAttribute(arrHeader[i], (String) vtRow.get(j).toString());
                            }
                            row.setText((String) vtRow.get(0).toString());
                        }
                    }
                    mElement.addContent(row);
                }
            }
        }

    }

    /**
     * Nguyen Anh Tue
     *
     * @since: May 12, 2010
     * @param:
     * @return:
     * @modified by:
     * @modified Date:
     * @param mPathFileName
     * @return
     * @throws IOException
     */
//    public static ArrayList getArrayList(Element mElement) throws Exception {
//        String strHeader = mElement.getChild("header").getTextTrim();
//        String[] arrHeader = strHeader.split(",");
//        return getArrayList(arrHeader, mElement, "row");
//    }
    /**
     * Nguyen Anh Tue
     *
     * @since: May 12, 2010
     * @param:
     * @return:
     * @modified by:
     * @modified Date:
     * @param mPathFileName
     * @return
     * @throws IOException
     */
//    public static ArrayList getArrayList(Element mElement, String elementName) throws Exception {
//        String strHeader = mElement.getChild("header").getTextTrim();
//        String[] arrHeader = strHeader.split(",");
//        return getArrayList(arrHeader, mElement, elementName);
//    }
    /**
     * Nguyen Anh Tue
     *
     * @since: May 12, 2010
     * @param:
     * @return:
     * @modified by:
     * @modified Date:
     * @param mPathFileName
     * @return
     * @throws IOException
     */
//    public static ArrayList getArrayList(String[] arrHeader, Element mElement, String elementName) throws Exception {
//        ArrayList vtTable = new ArrayList();
//        try {
//            List rows = mElement.getChildren(elementName);
//
//            for (int i = 0; i < rows.size(); i++) {
//                Element row = (Element) rows.get(i);
//                ArrayList vtElement = new ArrayList();
//                for (int j = 0; j < arrHeader.length; j++) {
//                    vtElement.add((row.getAttribute(arrHeader[j].trim())).getValue());
//                }
//                vtTable.add(vtElement);
//            }
//        } catch (Exception ex) {
//            ex.printStackTrace()
//        }
//        return vtTable;
//    }
//    public static ArrayList getThreadList(String[] arrAttr, Element mElement) throws Exception {
//        ArrayList arrThread = new ArrayList();
//        try {
//            List rows = mElement.getChildren("thread");
//
//            for (int i = 0; i < rows.size(); i++) {
//                Element thread = (Element) rows.get(i);
//                HashMap mapAttr = new HashMap();
//                for (int k = 0; k < arrAttr.length; k++) {
//                    mapAttr.put(arrAttr[k], findElementByPath(thread, arrAttr[k]).getValue());
//                }
//                arrThread.add(mapAttr);
//            }
//        } catch (Exception ex) {
//            ex.printStackTrace()
//        }
//        return arrThread;
//    }
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
    public static Element findElementByPath(Element root, String sPath) {
        Element eChild = null;
        String[] arrElement = sPath.split("/");
        if (arrElement.length == 0) {
            arrElement = new String[1];
            arrElement[0] = sPath;
        }
        if (arrElement.length > 0) {
            for (int i = 0; i < arrElement.length; i++) {
                eChild = root.getChild(arrElement[i]);
                root = eChild;
            }
        }
        return root;
    }

    /**
     * /**
     * Nguyen Anh Tue
     *
     * @since: May 12, 2010
     * @param:
     * @return:
     * @modified by:
     * @modified Date:
     * @param sElementPath
     * @param atributeName
     * @return
     */
    public static Attribute findAtribute(Element root, String sElementPath, String atributeName) {
        root = findElementByPath(root, sElementPath);
        Attribute a = null;
        if (root != null) {
            a = root.getAttribute(atributeName);
        }
        return a;
    }

    /**
     * /**
     * Nguyen Thanh Binh
     *
     * @since: Mar 17, 2011
     * @param: Xml Content in a String
     * @return: True - wellformed, False - unwellformed
     */
    public static boolean xmlCheck(String xmlContent) {
        SAXBuilder builder = new SAXBuilder();
        try {
            builder.build(new ByteArrayInputStream(xmlContent.getBytes("UTF-8")));
        } catch (JDOMException jex) {
            return false;
        } catch (IOException ioex) {
            return false;
        }
        return true;
    }

    /**
     * Nguyen Thanh Binh
     *
     * @since: Mar 17, 2011
     * @param: Xml Content in a String
     * @return: True - wellformed, False - unwellformed
     */
    public static Element findElement(Element current, String elementName) {
        if (current.getName().equals(elementName)) {
            return current;
        } else {
            List<Element> children = current.getChildren();
            if (children.isEmpty()) {
                return null;
            } else {
                Element elementTemp = null;
                for (Element child : children) {
                    elementTemp = findElement(child, elementName);
                    if (elementTemp != null) {
                        break;
                    }
                }
                return elementTemp;
            }
        }
    }

    public static Element findElementByAttribute(Element current, String elementName, String attrName, String attrValue) {
        if (current.getName().equals(elementName)) {
            if (current.getAttribute(attrName) != null
                    && current.getAttribute(attrName).getValue() != null
                    && current.getAttribute(attrName).getValue().equals(attrValue)) {
                return current;
            }
        }
        List<Element> children = current.getChildren();
        if (children.isEmpty()) {
            return null;
        } else {
            Element elementTemp = null;
            for (Element child : children) {
                elementTemp = findElementByAttribute(child, elementName, attrName, attrValue);
                if (elementTemp != null) {
                    break;
                }
            }
            return elementTemp;
        }

    }

    public static org.w3c.dom.Document convertToDOM(org.jdom.Document jdomDoc)
            throws JDOMException {
        DOMOutputter outputter = new DOMOutputter();
        return outputter.output(jdomDoc);
    }
    
//    public static <T> ArrayList<T> parseXMLToListObject(String xml,
//            String objName, Class<T> className) {
//        if (xml != null && !xml.isEmpty()) {
//            String strFind = objName;
//            String strEndTag = "</" + strFind + ">";
//            String strStartTag = "<" + strFind + ">";
//            T obj = null;
//            int startIndex = xml.indexOf(strStartTag);
//            int endIndex = xml.lastIndexOf(strEndTag);
//            if (startIndex > -1 && endIndex > -1) {
//                xml = xml.substring(startIndex, endIndex);
//                if (!xml.isEmpty()) {
//                    String[] arrFind = xml.split(strEndTag);
//                    ArrayList<T> lstRes = new ArrayList<T>();
//                    for (int i = 0; i < arrFind.length; i++) {
//                        arrFind[i] += strEndTag;
//                        arrFind[i] = arrFind[i].trim();
//                        obj = (T) parseXMLToObject(arrFind[i], className);
//                        if (obj != null) {
//                            lstRes.add(obj);
//                        } else {
//                            return null;
//                        }
//                    }
//                    return lstRes;
//                }
//            }
//        }
//        return null;
//    }
//
//    private static Object parseXMLToObject(String xml, Class c) {
//        try {
//            InputStream is = new ByteArrayInputStream(xml.getBytes());
//            Serializer serializer = new Persister();
//            Object obj = serializer.read(c, is);
//            return c.cast(obj);
//        } catch (Exception ex) {
//            ex.printStackTrace();
//        }
//        return null;
//    }
    
}
