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
package de.ingrid.elasticsearch.search.converter;

import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders;
import de.ingrid.elasticsearch.search.IQueryParsers;
import de.ingrid.utils.query.IngridQuery;
import de.ingrid.utils.query.TermQuery;

public class GlobalQueryConverter implements IQueryParsers {

    @Override
    public BoolQuery.Builder parse(IngridQuery ingridQuery, BoolQuery.Builder queryBuilder) {
        TermQuery[] terms = ingridQuery.getTerms();

        BoolQuery.Builder bq = new BoolQuery.Builder();

        if (terms.length == 0) {
            bq.must(QueryBuilders.matchAll().build()._toQuery());
            queryBuilder.must(bq.build()._toQuery());
        } else {
            for (TermQuery term : terms) {
                Query termQuery = QueryBuilders.queryString(q -> q.query(term.getTerm()));

                if (term.isRequred()) {
                    bq.must(termQuery);
                } else {
                    bq.should(termQuery);
                }
            }

            if (terms[0].isRequred()) {
                queryBuilder.must(bq.build()._toQuery());
            } else {
                queryBuilder.should(bq.build()._toQuery());
            }
        }
        return queryBuilder;
    }
}
