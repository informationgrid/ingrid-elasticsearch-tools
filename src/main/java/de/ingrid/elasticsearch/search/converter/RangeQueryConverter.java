/*-
 * **************************************************-
 * InGrid Elasticsearch Tools
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

import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.RangeQuery;
import co.elastic.clients.json.JsonData;
import de.ingrid.elasticsearch.search.IQueryParsers;
import de.ingrid.utils.query.IngridQuery;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

@Service
@Order(4)
public class RangeQueryConverter implements IQueryParsers {

    @Override
    public BoolQuery.Builder parse(IngridQuery ingridQuery, BoolQuery.Builder queryBuilder) {
        de.ingrid.utils.query.RangeQuery[] rangeQueries = ingridQuery.getRangeQueries();


        for (de.ingrid.utils.query.RangeQuery rangeQuery : rangeQueries) {

            BoolQuery.Builder bq = new BoolQuery.Builder();
            String from = rangeQuery.getRangeFrom();
            from = from.endsWith("*") ? from.substring(0, from.length() - 1) : from;
            String to = rangeQuery.getRangeTo();
            to = to.endsWith("*") ? to.substring(0, to.length() - 1) : to;

            String finalFrom = from;
            String finalTo = to;
            Query subQuery = RangeQuery.of(r -> r
                    .field(rangeQuery.getRangeName())
                    .gte(JsonData.of(finalFrom))
                    .lte(JsonData.of(finalTo))
            )._toQuery();

            if (rangeQuery.isRequred()) {
//                bq = new BoolQuery.Builder();
                if (rangeQuery.isProhibited()) {
                    bq.mustNot(subQuery);
                } else {
                    bq.must(subQuery);
                }
            } else {
//                if (bq == null) {
//                    bq = new BoolQuery.Builder();
                    bq.should(subQuery);
//                } else {
//                    BoolQuery.Builder parentBq = new BoolQuery.Builder();
//                    parentBq.should(bq.build()._toQuery()).should(subQuery);
//                    bq = parentBq;
//                }
            }

            if (rangeQuery.isRequred()) {
                queryBuilder.must(bq.build()._toQuery());
            } else {
                queryBuilder.should(bq.build()._toQuery());
            }
        }
        return queryBuilder;
    }
}
