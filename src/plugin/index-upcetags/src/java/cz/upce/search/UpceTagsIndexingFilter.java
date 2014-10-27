/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.upce.search;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.parse.Parse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Indexing filter that indexes all inbound anchor text for a document. 
 */
public class UpceTagsIndexingFilter
        implements IndexingFilter {

    public static final Logger LOG = LoggerFactory.getLogger(UpceTagsIndexingFilter.class);
    private Configuration conf;
    private Map<String, UpceRight> rightsMap;

    public UpceTagsIndexingFilter() {
        LOG.info("UpceTagsIndexingFilter construct");
        rightsMap = new HashMap<String, UpceRight>();
        rightsMap.put("zamestnanci.upce.cz", UpceRight.ZAMESTNANCI);
        rightsMap.put("studenti.upce.cz", UpceRight.STUDENTI);
        rightsMap.put("dokumenty.upce.cz", UpceRight.RIGHTS_XML);
        rightsMap.put("data.upce.cz", UpceRight.RIGHTS_XML);
        rightsMap.put("cas.upce.cz", UpceRight.RIGHTS_XML);
        rightsMap.put("idp.upce.cz", UpceRight.RIGHTS_XML);
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public Configuration getConf() {
        return this.conf;
    }

    public NutchDocument filter(NutchDocument doc, Parse parse, Text textUrl, CrawlDatum datum,
            Inlinks inlinks) throws IndexingException {
        LOG.info("UpceTagsIndexingFilter filter: textUrl=[" + textUrl + "], datum=[" + datum + "], inlinks=[" + inlinks + "]");
        URL url = null;
        try {
            url = new URL(textUrl.toString());
            indexAccessRights(doc, url);
        } catch (MalformedURLException ex) {
            throw new IndexingException(ex);
        }

        return doc;
    }

    protected void indexAccessRights(NutchDocument doc, URL url) {
        String host = url.getHost().toLowerCase();
        for (Map.Entry<String, UpceRight> rights : rightsMap.entrySet()) {
            if (host.equals(rights.getKey())) {
                LOG.info("UpceTagsIndexingFilter indexAccessRights - " + rights.getValue());
                rights.getValue().setRights(doc, url);
                return;
            }
        }
        LOG.info("UpceTagsIndexingFilter indexAccessRights - PUBLIC");
        UpceRight.PUBLIC.setRights(doc, url);
    }
}
