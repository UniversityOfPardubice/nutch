package cz.upce.search;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
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

public class UpceTagsIndexingFilter
        implements IndexingFilter {

    public static final Logger LOG = LoggerFactory.getLogger(UpceTagsIndexingFilter.class);
    private Configuration conf;
    private final Map<String, UpceRight> rightsMap;

    public UpceTagsIndexingFilter() {
        LOG.info("UpceTagsIndexingFilter construct");
        rightsMap = new HashMap<String, UpceRight>();
        rightsMap.put("zamestnanci.upce.cz", UpceRight.ZAMESTNANCI);
        rightsMap.put("studenti.upce.cz", UpceRight.STUDENTI);
        rightsMap.put("dokumenty.upce.cz", UpceRight.RIGHTS_XML);
        rightsMap.put("data.upce.cz", UpceRight.RIGHTS_XML);
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    @Override
    public NutchDocument filter(NutchDocument doc, Parse parse, Text textUrl, CrawlDatum datum, Inlinks inlinks) throws IndexingException {
        LOG.info("UpceTagsIndexingFilter filter: textUrl=[{}]", textUrl);
        URL url;
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
        if (rightsMap.containsKey(host)) {
            UpceRight rights = rightsMap.get(host);
            LOG.info("UpceTagsIndexingFilter indexAccessRights {} - {}", url, rights);
            rights.setRights(doc, url);
            return;
        }
        LOG.info("UpceTagsIndexingFilter indexAccessRights {} - PUBLIC", url);
        UpceRight.PUBLIC.setRights(doc, url);
    }
}
