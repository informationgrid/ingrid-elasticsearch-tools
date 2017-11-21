package de.ingrid.elasticsearch;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class ElasticConfig {

    @Value("${elastic.enabled:true}")
    public boolean isEnabled;
    
    @Value("${elastic.isRemote:true}")
    public boolean isRemote;
    
    @Value("${elastic.remoteHosts:localhost:9300}")
    public String[] remoteHosts;
    
    @Value("${elastic.indexWithAutoId:true}")
    public boolean indexWithAutoId;
    
    @Value("${elastic.indexSearchDefaultFields:}")
    public String[] indexSearchDefaultFields;

    @Value("${elastic.boostField:boost}")
    public String boostField;

    @Value("${elastic.boostModifier:log1p}")
    public String boostModifier;

    @Value("${elastic.boostFactor:1}")
    public float boostFactor;

    @Value("${elastic.boostMode:}")
    public String boostMode;

    @Value("${elastic.indexFieldTitle:}")
    public String indexFieldTitle;

    @Value("${elastic.additionalSearchDetailFields:}")
    public String[] additionalSearchDetailFields;

    @Value("${elastic.indexFieldSummary:}")
    public String indexFieldSummary;

    
    @Value("${index.boost.enable:false}")
    public boolean indexEnableBoost;

    
    @Value("${communication.clientName:}")
    public String communicationProxyUrl;

    @Value("${index.search.groupByUrl:false}")
    public boolean groupByUrl;

    @Value("${plugdescription.partner:}")
    public String[] partner;

    @Value("${plugdescription.provider:}")
    public String[] provider;

    @Value("${index.fields.include:*}")
    public String indexFieldsIncluded;

    @Value("${index.fields.exclude:}")
    public String indexFieldsExcluded;

    // public String[] docProducerIndices;

    @Value("${elastic.communication.ibus:false}")
    public boolean esCommunicationThroughIBus;

    public IndexInfo[] activeIndices;

}
