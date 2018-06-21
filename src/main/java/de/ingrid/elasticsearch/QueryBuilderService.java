/*-
 * **************************************************-
 * InGrid Elasticsearch Tools
 * ==================================================
 * Copyright (C) 2014 - 2018 wemove digital solutions GmbH
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
package de.ingrid.elasticsearch;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TypeQueryBuilder;
import org.springframework.stereotype.Service;

@Service
public class QueryBuilderService {

    private static Logger log = LogManager.getLogger( QueryBuilderService.class );

    public BoolQueryBuilder createIndexTypeFilter(IndexInfo[] activeIndices) {

        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        BoolQueryBuilder boolShould = QueryBuilders.boolQuery();
        List<QueryBuilder> should = boolShould.should();

        for (IndexInfo activeIndex : activeIndices) {
            should.add( buildIndexTypeMust( activeIndex.getRealIndexName(), activeIndex.getToType() ) );
        }

        boolQuery.filter().add( boolShould );

        return boolQuery;
    }

    public BoolQueryBuilder buildMustQuery(String... fieldAndValue) {

        if (fieldAndValue.length % 2 == 1) {
            log.error( "This function only should have an even number of parameters!" );
            throw new RuntimeException( "ERROR: uneven number of parameters" );
        }

        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        List<QueryBuilder> must = boolQuery.must();
        for (int i = 0; i < fieldAndValue.length; i++) {
            must.add( QueryBuilders.termQuery( fieldAndValue[i], fieldAndValue[i + 1] ) );
            i++;
        }

        return boolQuery;
    }

    public BoolQueryBuilder buildIndexTypeMust(String index, String type) {
        TermQueryBuilder indexQuery = QueryBuilders.termQuery( "_index", index );
        TypeQueryBuilder typeQuery = QueryBuilders.typeQuery( type );

        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        List<QueryBuilder> must = boolQuery.must();
        must.add( indexQuery );
        must.add( typeQuery );

        return boolQuery;
    }

    public BoolQueryBuilder createQueryWithFilter(String query, BoolQueryBuilder indexTypeFilter) {
        QueryStringQueryBuilder queryStringQuery = QueryBuilders.queryStringQuery( query.trim().length() == 0 ? "*" : query );
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        List<QueryBuilder> must = boolQuery.must();
        must.add( queryStringQuery );
        must.add( QueryBuilders.boolQuery().should(indexTypeFilter) );

        return boolQuery;
    }
}
