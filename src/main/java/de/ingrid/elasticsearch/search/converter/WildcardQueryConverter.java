package de.ingrid.elasticsearch.search.converter;

import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders;
import de.ingrid.elasticsearch.search.IQueryParsers;
import de.ingrid.utils.query.IngridQuery;
import de.ingrid.utils.query.WildCardTermQuery;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

@Service
@Order(5)
public class WildcardQueryConverter implements IQueryParsers {

    @Override
    public BoolQuery.Builder parse(IngridQuery ingridQuery, BoolQuery.Builder queryBuilder) {
        WildCardTermQuery[] terms = ingridQuery.getWildCardTermQueries();

        BoolQuery.Builder bq = null;

        if (terms.length > 0) {
            for (WildCardTermQuery term : terms) {
                Query subQuery = QueryBuilders.queryString(q -> q.query(term.getTerm()));

                if (term.isRequred()) {
                    if (bq == null) bq = new BoolQuery.Builder();
                    if (term.isProhibited()) {
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
