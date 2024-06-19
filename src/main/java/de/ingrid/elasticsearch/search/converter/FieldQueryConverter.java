package de.ingrid.elasticsearch.search.converter;

import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders;
import de.ingrid.elasticsearch.search.IQueryParsers;
import de.ingrid.utils.query.FieldQuery;
import de.ingrid.utils.query.IngridQuery;

public class FieldQueryConverter implements IQueryParsers {

    @Override
    public BoolQuery.Builder parse(IngridQuery ingridQuery, BoolQuery.Builder queryBuilder) {
        FieldQuery[] fields = ingridQuery.getFields();

        BoolQuery.Builder bq = new BoolQuery.Builder();

        for (FieldQuery fieldQuery : fields) {

            Query subQuery = QueryBuilders.match(m -> m.field(fieldQuery.getFieldName()).query(fieldQuery.getFieldValue()));

            if (fieldQuery.isRequred()) {
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
                    parentBq.should(bq.build()._toQuery()).should(subQuery);
                    bq = parentBq;
                }
            }

            if (fieldQuery.isRequred()) {
                queryBuilder.must(bq.build()._toQuery());
            } else {
                queryBuilder.should(bq.build()._toQuery());
            }
        }
        return queryBuilder;
    }
}
