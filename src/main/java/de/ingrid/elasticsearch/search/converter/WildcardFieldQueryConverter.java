/*-
 * **************************************************-
 * InGrid Elasticsearch Tools
 * ==================================================
 * Copyright (C) 2014 - 2025 wemove digital solutions GmbH
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
import de.ingrid.utils.query.WildCardFieldQuery;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

@Service
@Order(6)
public class WildcardFieldQueryConverter implements IQueryParsers {

    @Override
    public BoolQuery.Builder parse(IngridQuery ingridQuery, BoolQuery.Builder queryBuilder) {
        WildCardFieldQuery[] wildFields = ingridQuery.getWildCardFieldQueries();

        BoolQuery.Builder bq = null;
        boolean wasAndConnection = false;

        for (WildCardFieldQuery fieldQuery : wildFields) {
            Query subQuery = QueryBuilders.wildcard(w -> w
                    .field(fieldQuery.getFieldName())
                    .value(fieldQuery.getFieldValue())
            );

            if (fieldQuery.isRequred()) {
                if (bq == null) bq = new BoolQuery.Builder();
                if (fieldQuery.isProhibited()) {
                    bq.mustNot(subQuery);
                } else {
                    bq.must(subQuery);
                }
                wasAndConnection = true;
            } else {
                // if it's an OR-connection then the currently built query must become a sub-query
                // so that the AND/OR connection is correctly transformed. In case there was an
                // AND-connection before, the transformation would become:
                // OR( (term1 AND term2), term3)
                if (bq == null) bq = new BoolQuery.Builder();

                if (!wasAndConnection) {
                    bq.should(subQuery);
                } else {
                    BoolQuery.Builder parentBq = new BoolQuery.Builder();
                    parentBq.should(bq.build()._toQuery()).should(subQuery);
                    bq = parentBq;
                    wasAndConnection = false;
                }
            }
        }

        if (bq != null) {
            if (wildFields[0].isRequred()) {
                queryBuilder.must(bq.build()._toQuery());
            } else {
                queryBuilder.should(bq.build()._toQuery());
            }
        }
        return queryBuilder;
    }
}
