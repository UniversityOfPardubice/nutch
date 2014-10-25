/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cz.upce.search;

import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.util.DomUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 *  *
 *   * @author lusl0338
 *    */
public enum UpceRight {

    PUBLIC, ZAMESTNANCI, STUDENTI, RIGHTS_XML, NONE;
    
    private static final Logger LOG = LoggerFactory.getLogger(UpceRight.class);

    public void setRights(NutchDocument doc, URL url) {
        switch (this) {
            case ZAMESTNANCI:
                doc.add("rights", "zamestnanci");
                break;
            case STUDENTI:
                doc.add("rights", "zamestnanci");
                doc.add("rights", "studenti");
                break;
            case PUBLIC:
                doc.add("rights", "zamestnanci");
                doc.add("rights", "studenti");
                doc.add("rights", "public");
                break;
            case RIGHTS_XML:
                setRightsByXml(doc, url);
                break;
            case NONE:
                break;
        }
    }

    private void setRightsByXml(NutchDocument doc, URL url) {
        LOG.info("UpceRight - setRightsByXml; doc=[" + doc + "], url=[" + url + "]");
        String rightsUrl;
        rightsUrl = url.toExternalForm().replaceFirst("\\.[^.]+$", "_rights.xml");
        LOG.info("UpceRight - setRightsByXml; rightsUrl=[" + rightsUrl + "]");
        UpceRight right = getRightFromUrl(rightsUrl);
        if (right == null) {
            rightsUrl = url.toExternalForm().replaceFirst("$", "_rights.xml");
            right = getRightFromUrl(rightsUrl);
        }
        if (right == null) {
            right = ZAMESTNANCI;
        }
        right.setRights(doc, url);
    }

    private UpceRight getRightFromUrl(String stringUrl) {
        LOG.info("UpceRight - getRightFromUrl; stringUrl=[" + stringUrl + "]");
        InputStream is = null;
        try {
            URL url = new URL(stringUrl);
            URLConnection connection = url.openConnection();
            connection.connect();
            is = connection.getInputStream();
        } catch (Exception ex) {
            return null;
        }
        if (is == null) {
            return null;
        }
        Element roles = DomUtil.getDom(is);
        if (roles == null) {
            return null;
        }
        LOG.info("UpceRight - getRightFromUrl; roles=[" + roles + "]");
        if (roles.hasAttribute("anonymous")
                && roles.getAttribute("anonymous").toLowerCase().equals("true")) {
            LOG.info("UpceRight - getRightFromUrl; right=[PUBLIC]");
            return PUBLIC;
        }
        NodeList roleNames = roles.getElementsByTagName("roleName");
        UpceRight right = PUBLIC;
        for (int i = 0; i < roleNames.getLength(); i++) {
            String roleName = roleNames.item(i).getTextContent();
            if ("je_student".equals(roleName) && (right == PUBLIC)) {
                right = STUDENTI;
            }
            if ("je_zamestnanec".equals(roleName) && (right == PUBLIC || right == STUDENTI)) {
                right = ZAMESTNANCI;
            }
        }
        LOG.info("UpceRight - getRightFromUrl; right=[" + right + "]");
        return right;
    }
}
