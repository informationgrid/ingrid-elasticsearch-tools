/*
 * **************************************************-
 * ingrid-iplug-se-iplug
 * ==================================================
 * Copyright (C) 2014 - 2024 wemove digital solutions GmbH
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
package de.ingrid.elasticsearch.search;

import co.elastic.clients.elasticsearch._types.aggregations.*;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import de.ingrid.elasticsearch.search.converter.QueryConverter;
import de.ingrid.elasticsearch.search.facets.FacetClassDefinition;
import de.ingrid.elasticsearch.search.facets.FacetDefinition;
import de.ingrid.elasticsearch.search.facets.FacetUtils;
import de.ingrid.elasticsearch.search.facets.IFacetDefinitionProcessor;
import de.ingrid.utils.IngridDocument;
import de.ingrid.utils.query.IngridQuery;
import de.ingrid.utils.queryparser.ParseException;
import de.ingrid.utils.queryparser.QueryStringParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class FacetConverter {

    private static final Log log = LogFactory.getLog(FacetConverter.class);

    @Autowired(required = false)
    private List<IFacetDefinitionProcessor> facetDefinitionProcessors = new ArrayList<>();

    private final QueryConverter queryConverter;

    @Autowired
    public FacetConverter(QueryConverter qc) {
        this.queryConverter = qc;
    }

    public Map<String, Aggregation> getAggregations(IngridQuery ingridQuery) {
        List<FacetDefinition> defs = FacetUtils.getFacetDefinitions(ingridQuery);

        for (IFacetDefinitionProcessor facetdefProcessor : facetDefinitionProcessors) {
            facetdefProcessor.process(defs);
        }

        Map<String, Aggregation> aggregations = new HashMap<>();

        for (FacetDefinition facetDefinition : defs) {
            String name = facetDefinition.getName();
            String field = facetDefinition.getField();
            List<FacetClassDefinition> classes = facetDefinition.getClasses();
            Aggregation aggr;

            if (classes != null) {
                for (FacetClassDefinition fClass : classes) {
                    try {
                        IngridQuery facetQuery = QueryStringParser.parse(fClass.getFragment());
                        aggr = AggregationBuilders.filter(b -> b.bool(queryConverter.convert(facetQuery).build()));
                        aggregations.put(name, aggr);
                    } catch (ParseException e) {
                        log.error("Error during parsing facets", e);
                    }
                }
            } else {
                aggr = AggregationBuilders.terms(t -> t.field(field).size(1000));
                aggregations.put(name, aggr);
            }
        }

        return aggregations;
    }

    public IngridDocument convertFacetResultsToDoc(SearchResponse<HashMap> response) {
        IngridDocument facets = new IngridDocument();
        Map<String, Aggregate> aggregations = response.aggregations();

        if (aggregations != null) {
            for (Map.Entry<String, Aggregate> entry : aggregations.entrySet()) {
                Aggregate aggregate = entry.getValue();

                if (aggregate._kind() == Aggregate.Kind.Sterms) { //StringTermsAggregate.class)) {
                    StringTermsAggregate termsAggregate = aggregate.sterms();
                    for (StringTermsBucket bucket : termsAggregate.buckets().array()) {
                        facets.put(entry.getKey() + ":" + bucket.key().stringValue(), bucket.docCount());
                    }
                } else if (aggregate._kind() == Aggregate.Kind.Filter) {
                    facets.put(entry.getKey(), aggregate.filter().docCount());
                } else {
                    throw new RuntimeException("Aggregation Class not supported: " + aggregate._kind());
                }
            }
        }

        return facets;
    }
}
