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
package de.ingrid.elasticsearch.search.converter;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

import de.ingrid.elasticsearch.search.IQueryParsers;
import de.ingrid.utils.query.IngridQuery;
import de.ingrid.utils.query.WildCardTermQuery;

@Service
@Order(5)
public class WildcardQueryConverter implements IQueryParsers {
    
    @Override
    public void parse(IngridQuery ingridQuery, BoolQueryBuilder queryBuilder) {
        WildCardTermQuery[] terms = ingridQuery.getWildCardTermQueries();

        BoolQueryBuilder bq = null;
        
        if (terms.length > 0) {
            
            for (WildCardTermQuery term : terms) {
                QueryBuilder subQuery = QueryBuilders.queryStringQuery( term.getTerm() );
                
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
                
            if (terms[0].isRequred()) {
                queryBuilder.must( bq );
            } else {
                queryBuilder.should( bq );
            }
        
        }
    }
}
