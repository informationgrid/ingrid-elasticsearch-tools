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

import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import de.ingrid.utils.query.IngridQuery;

public class ConverterUtils {

    /**
     * Apply generic combination to the query depending on the settings (required, prohibited, optional).
     *
     * @param fieldQuery is the incoming query used for analysis
     * @param bq         is the boolean query that has been generated so far inside the current parser(!) and is used for combination with the subQuery
     * @param subQuery   is the newly generated subquery by a converter that has to be combined with bq
     * @return a combined query according to the AND, OR, NOT rules
     */
    public static BoolQuery.Builder applyAndOrRules(IngridQuery fieldQuery, BoolQuery.Builder bq, Query subQuery) {
        if (fieldQuery.isRequred()) {
            if (bq == null) {
                bq = new BoolQuery.Builder();
            }
            if (fieldQuery.isProhibited()) {
                bq.mustNot(subQuery);
            } else {
                bq.must(subQuery);
            }

        } else {
            if (bq == null) {
                bq = new BoolQuery.Builder();
                bq.should(subQuery);
            } else {
                BoolQuery.Builder parentBq = new BoolQuery.Builder();

                // if bq top type == "should" then add it, otherwise wrap it around
                // the type should be always at the same position!
//                String bqString = bq.build().toString();
//                if (bqString.contains("\"should\"")) {
                    bq.should(subQuery);
//                } else {
//                    parentBq.should(bq.build()._toQuery()).should(subQuery);
//                    bq = parentBq;
//                }
            }
        }

        return bq;
    }
}
