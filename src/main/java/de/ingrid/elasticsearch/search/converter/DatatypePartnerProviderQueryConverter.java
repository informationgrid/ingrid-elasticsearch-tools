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

import java.util.ArrayList;
import java.util.List;

import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

import de.ingrid.elasticsearch.search.IQueryParsers;
import de.ingrid.utils.query.FieldQuery;
import de.ingrid.utils.query.IngridQuery;

@Service
@Order(2)
public class DatatypePartnerProviderQueryConverter implements IQueryParsers {

    @SuppressWarnings("unchecked")
    @Override
    public void parse(IngridQuery ingridQuery, BoolQuery.Builder queryBuilder) {
        final List<FieldQuery> dataTypes = (List<FieldQuery>)(List<?>)ingridQuery.getArrayList(IngridQuery.DATA_TYPE);
        final List<FieldQuery> partner = (List<FieldQuery>)(List<?>)ingridQuery.getArrayList(IngridQuery.PARTNER);
        final List<FieldQuery> provider = (List<FieldQuery>)(List<?>)ingridQuery.getArrayList(IngridQuery.PROVIDER);

        // Concatenate all fields
        List<FieldQuery> allFields = new ArrayList<>();
        if (dataTypes != null) allFields.addAll(dataTypes);
        if (partner != null) allFields.addAll(partner);
        if (provider != null) allFields.addAll(provider);

        if (!allFields.isEmpty()) {
            BoolQuery.Builder bq = new BoolQuery.Builder();
            for (final FieldQuery fieldQuery : allFields) {
                final String field = fieldQuery.getFieldName();
                final String value = fieldQuery.getFieldValue().toLowerCase();
                Query subQuery = QueryBuilders.term(t -> t.field(field).value(value));

                bq = ConverterUtils.applyAndOrRules(fieldQuery, bq, subQuery);
            }
            if (allFields.get(0).isRequred()) {
                queryBuilder.must(bq.build()._toQuery());
            } else {
                queryBuilder.should(bq.build()._toQuery());
            }
        }
    }
}
