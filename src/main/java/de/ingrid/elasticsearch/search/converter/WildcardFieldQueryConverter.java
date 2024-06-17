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
    public void parse(IngridQuery ingridQuery, BoolQuery.Builder queryBuilder) {
        WildCardFieldQuery[] wildFields = ingridQuery.getWildCardFieldQueries();

        BoolQuery.Builder bq = new BoolQuery.Builder();
        boolean wasAndConnection = false;

        for (WildCardFieldQuery fieldQuery : wildFields) {
            Query subQuery = QueryBuilders.wildcard(w -> w
                    .field(fieldQuery.getFieldName())
                    .value(fieldQuery.getFieldValue())
            );

            if (fieldQuery.isRequred()) {
                if (fieldQuery.isProhibited()) {
                    bq.mustNot(subQuery);
                } else {
                    bq.must(subQuery);
                }
                wasAndConnection = true;
            } else {
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
    }
}
