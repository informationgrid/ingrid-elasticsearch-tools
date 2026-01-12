/*-
 * **************************************************-
 * InGrid Elasticsearch Tools
 * ==================================================
 * Copyright (C) 2014 - 2026 wemove digital solutions GmbH
 * ==================================================
 * Licensed under the EUPL, Version 1.2 or – as soon they will be
 * approved by the European Commission - subsequent versions of the
 * EUPL (the "Licence");
 * 
 * You may not use this work except in compliance with the Licence.
 * You may obtain a copy of the Licence at:
 * 
 * https://joinup.ec.europa.eu/software/page/eupl
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the Licence is distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Licence for the specific language governing permissions and
 * limitations under the Licence.
 * **************************************************#
 */
package de.ingrid.elasticsearch;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class ElasticConfig {

    @Value("${iplug.uuid:}")
    public String uuid;

    @Value("${elastic.enabled:true}")
    public boolean isEnabled;

    @Value("${elastic.remoteHosts:localhost:9200}")
    public String[] remoteHosts;

    @Value("${elastic.indexWithAutoId:true}")
    public boolean indexWithAutoId;

    @Value("${elastic.indexSearchDefaultFields:title^10.0,title.edge_ngram^4.0,title.ngram^2.0,summary,summary.edge_ngram^0.4,summary.ngram^0.2,content^0.2,content.ngram^0.1}")
    public String[] indexSearchDefaultFields;

    @Value("${elastic.boostField:boost}")
    public String boostField;

    @Value("${elastic.boostModifier:log1p}")
    public String boostModifier;

    @Value("${elastic.boostFactor:1}")
    public float boostFactor;

    @Value("${elastic.boostMode:}")
    public String boostMode;

    @Value("${index.field.title:title}")
    public String indexFieldTitle;

    @Value("${index.search.additional.detail.fields:}")
    public String[] additionalSearchDetailFields;

    @Value("${index.field.summary:summary}")
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

    @Value("${elastic.communication.ibus:true}")
    public boolean esCommunicationThroughIBus;

    @Value("${elastic.cluster.name:ingrid}")
    public String clusterName;

    @Value("${elastic.username:}")
    public String username;

    @Value("${elastic.password:}")
    public String password;

    @Value("${elastic.sslTransport:}")
    public String sslTransport;

    @Value("${elastic.trackTotalHits:true}")
    public boolean trackTotalHits;

    public IndexInfo[] activeIndices;

}
