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

import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders;
import co.elastic.clients.elasticsearch._types.query_dsl.TermQuery;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class QueryBuilderService {

    private static final Logger log = LogManager.getLogger( QueryBuilderService.class );

    public BoolQuery.Builder createIndexTypeFilter(IndexInfo[] activeIndices) {

        BoolQuery.Builder boolQuery = QueryBuilders.bool();
//        BoolQuery.Builder boolShould = QueryBuilders.bool();

        List<Query> boolQueriesShould = new ArrayList<>();
        for (IndexInfo activeIndex : activeIndices) {
            boolQueriesShould.add( buildIndexTypeMust( activeIndex.getRealIndexName() ) );
        }

        Query should = BoolQuery.of(b -> b.should(boolQueriesShould))._toQuery();
        boolQuery.filter( should );

        return boolQuery;
    }

    public BoolQuery buildMustQuery(String... fieldAndValue) {

        if (fieldAndValue.length % 2 == 1) {
            log.error( "This function only should have an even number of parameters!" );
            throw new RuntimeException( "ERROR: uneven number of parameters" );
        }

        /*
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        List<QueryBuilder> must = boolQuery.must();
        for (int i = 0; i < fieldAndValue.length; i++) {
            must.add( QueryBuilders.termQuery( fieldAndValue[i], fieldAndValue[i + 1] ) );
            i++;
        }*/



        List<Query> termQueries = new ArrayList<>();
        for (int i = 0; i < fieldAndValue.length; i++) {
            int finalI = i;
            termQueries.add( TermQuery.of(t -> t.field(fieldAndValue[finalI]).value(fieldAndValue[finalI + 1]) )._toQuery() );
            i++;
        }

        return BoolQuery.of(b -> b.must(termQueries));
    }

    private Query buildIndexTypeMust(String index) {
        Query termIndexQuery = TermQuery.of(t -> t.field("_index").value(index))._toQuery();
        return BoolQuery.of(b -> b.must(termIndexQuery))._toQuery();
    }

    /*private BoolQueryBuilder createQueryWithFilter(String query, BoolQueryBuilder indexTypeFilter) {
        QueryStringQueryBuilder queryStringQuery = QueryBuilders.queryStringQuery( query.trim().length() == 0 ? "*" : query );
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        List<QueryBuilder> must = boolQuery.must();
        must.add( queryStringQuery );
        must.add( QueryBuilders.boolQuery().should(indexTypeFilter) );

        return boolQuery;
    }*/
}
