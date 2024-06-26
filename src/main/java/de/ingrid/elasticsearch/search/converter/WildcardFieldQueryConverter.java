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
