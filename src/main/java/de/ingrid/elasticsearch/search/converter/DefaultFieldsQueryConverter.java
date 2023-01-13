/*
 * **************************************************-
 * ingrid-iplug-se-iplug
 * ==================================================
 * Copyright (C) 2014 - 2023 wemove digital solutions GmbH
 * ==================================================
 * Licensed under the EUPL, Version 1.1 or – as soon they will be
 * approved by the European Commission - subsequent versions of the
 * EUPL (the "Licence");
 * 
 * You may not use this work except in compliance with the Licence.
 * You may obtain a copy of the Licence at:
 * 
 * http://ec.europa.eu/idabc/eupl5
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the Licence is distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Licence for the specific language governing permissions and
 * limitations under the Licence.
 * **************************************************#
 */
package de.ingrid.elasticsearch.search.converter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.index.query.*;
import org.elasticsearch.index.query.MultiMatchQueryBuilder.Type;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

import de.ingrid.elasticsearch.ElasticConfig;
import de.ingrid.elasticsearch.search.IQueryParsers;
import de.ingrid.utils.query.IngridQuery;
import de.ingrid.utils.query.TermQuery;

@Service
@Order(1)
public class DefaultFieldsQueryConverter implements IQueryParsers {

    private Map<String, Float> fieldBoosts;
    
    @Autowired
    public DefaultFieldsQueryConverter(ElasticConfig config) {
        fieldBoosts = getFieldBoostMap(config.indexSearchDefaultFields);
    }

    private Map<String, Float> getFieldBoostMap(String[] indexSearchDefaultFields) {
        Map<String, Float> result = new HashMap<>();
        for (String field:indexSearchDefaultFields) {
            if(field.contains("^")){
                String[] split = field.split("\\^");
                result.put(split[0], Float.parseFloat(split[1]));
            }
            else{
                result.put(field, Float.valueOf(1.0F));
            }
        }
        return result;
    }

    @Override
    public void parse(IngridQuery ingridQuery, BoolQueryBuilder queryBuilder) {
        TermQuery[] terms = ingridQuery.getTerms();

        BoolQueryBuilder bq = null;//QueryBuilders.boolQuery();
        
        if (terms.length > 0) {
            List<String> termsAnd = new ArrayList<>();
            List<String> termsOr = new ArrayList<>();
            for (TermQuery term : terms) {
                String t = term.getTerm();
                QueryBuilder subQuery;

                // if it's a phrase
                if (t.contains( " " )) {
                    subQuery = QueryBuilders.boolQuery();
                    for (Map.Entry<String, Float> field : fieldBoosts.entrySet()) {
                        ((BoolQueryBuilder)subQuery).should( QueryBuilders.matchPhraseQuery( field.getKey(), t ).boost(field.getValue()) );
                    }
                // in case a term was not identified as a wildcard-term, e.g. "Deutsch*"
                } else if (t.contains( "*" )) {
                    subQuery = QueryBuilders.boolQuery();
                    ((BoolQueryBuilder)subQuery).should( QueryBuilders.queryStringQuery( t ) );
                    
                } else if (term.isProhibited()) {
                    subQuery = QueryBuilders.multiMatchQuery( t, fieldBoosts.keySet().toArray(new String[]{})).fields(fieldBoosts);
                    
                } else {
                
                    // only add term to the correct list, so that the whole terms will be matched correctly
                    // even with stopwords filtered!!!
                    if (term.isRequred()) {
                        termsAnd.add( t );
                    } else { 
                        termsOr.add( t );
                    }
                    
                    continue;
                }
                
                if (term.isRequred()) {
                    if (bq == null) bq = QueryBuilders.boolQuery();
                    if (term.isProhibited()) {
                        bq.mustNot( subQuery );
                    } else {                        
                        bq.must( subQuery );
                    }
                    
                } else {
                    // if it's an OR-connection then the currently built query must become a sub-query
                    // so that the AND/OR connection is correctly transformed. In case there was an
                    // AND-connection before, the transformation would become:
                    // OR( (term1 AND term2), term3)
                    if (bq == null) {
                        bq = QueryBuilders.boolQuery();
                        bq.should( subQuery );
                        
                    } else {
                        BoolQueryBuilder parentBq = QueryBuilders.boolQuery();
                        parentBq.should( bq ).should( subQuery );
                        bq = parentBq;
                    }
                    
                }
            }

            if (!termsAnd.isEmpty()) {
                String join = String.join( " ", termsAnd );
                MultiMatchQueryBuilder subQuery = QueryBuilders.multiMatchQuery( join, fieldBoosts.keySet().toArray(new String[]{})).fields(fieldBoosts).operator(Operator.AND).type( Type.CROSS_FIELDS );
                if (bq == null) bq = QueryBuilders.boolQuery();
                bq.should( subQuery );
            }
            if (!termsOr.isEmpty()) {
                String join = String.join( " ", termsOr );
                MultiMatchQueryBuilder subQuery = QueryBuilders.multiMatchQuery( join, fieldBoosts.keySet().toArray(new String[]{})).fields(fieldBoosts).operator( Operator.OR ).type( Type.CROSS_FIELDS );
                if (bq == null) bq = QueryBuilders.boolQuery();
                bq.should( subQuery );
            }

            if(bq != null) {
                if (terms.length > 0 && terms[0].isRequred()) {
                    queryBuilder.must(bq);
                } else {
                    queryBuilder.should(bq);
                }
            }
        }
    }
    
}
