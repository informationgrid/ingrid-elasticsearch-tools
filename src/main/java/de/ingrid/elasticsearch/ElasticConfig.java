package de.ingrid.elasticsearch;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class ElasticConfig {

    @Value("${elastic.enabled:true}")
    public boolean isEnabled;
    
    @Value("${elastic.isRemote:true}")
    public boolean isRemote;
    
    @Value("${elastic.remote.hosts:localhost:9300}")
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

    public boolean indexEnableBoost;

    public String communicationProxyUrl;

    public boolean groupByUrl;

    public String[] partner;

    public String[] provider;

    public String indexFieldsIncluded;

    public String indexFieldsExcluded;
}
